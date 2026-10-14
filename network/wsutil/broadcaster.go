package wsutil

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yi-nology/ekit-concurrency/async"
	"github.com/yi-nology/ekit-logx"
)

// wsLog 是 wsutil 包专用 logger(marshal 失败、慢消费者告警)。
var wsLog = logx.Delegating("wsutil")

// Conn 是 WebSocket 连接的最小接口。
// github.com/hertz-contrib/websocket.Conn 满足此接口。
type Conn interface {
	WriteMessage(messageType int, data []byte) error
	Close() error
	// SetWriteDeadline 设置写超时（可选实现）。
	// 如果 Conn 不实现此方法，Broadcaster 会跳过写超时设置。
	SetWriteDeadline(t time.Time) error
}

const (
	// TextMessage 表示文本消息类型（与 websocket.TextMessage 一致）。
	TextMessage = 1
)

// Event 是一个可序列化的事件，携带类型和时间戳。
type Event struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data,omitempty"`
}

type registration struct {
	room string
	conn Conn
}

type broadcastMessage struct {
	room    string
	message []byte
}

// Broadcaster 是基于房间的 WebSocket 广播器。
// 支持多房间、自动清理空房间、容错广播。
//
// 用法:
//
//	b := wsutil.NewBroadcaster()
//	defer b.Close()
//
//	// 客户端加入房间
//	b.Register("review:123", conn)
//
//	// 广播事件
//	b.BroadcastEvent("review:123", wsutil.Event{Type: "progress", Data: progress})
type Broadcaster struct {
	rooms      map[string]map[Conn]bool
	mu         sync.RWMutex
	register   chan *registration
	unregister chan *registration
	broadcast  chan *broadcastMessage
	stop       chan struct{}
	closed     atomic.Bool
	// waitTimeout 是单轮广播等待所有写完成的上界, NewBroadcaster 设默认值,
	// 测试(同包)可调短
	waitTimeout time.Duration
}

// NewBroadcaster 创建并启动广播器。
func NewBroadcaster() *Broadcaster {
	b := &Broadcaster{
		rooms:       make(map[string]map[Conn]bool),
		register:    make(chan *registration),
		unregister:  make(chan *registration),
		broadcast:   make(chan *broadcastMessage),
		stop:        make(chan struct{}),
		waitTimeout: defaultBroadcastWaitTimeout,
	}
	async.GoSafe(b.run)
	return b
}

func (b *Broadcaster) run() {
	for {
		select {
		case reg := <-b.register:
			b.handleRegister(reg)
		case reg := <-b.unregister:
			b.handleUnregister(reg)
		case msg := <-b.broadcast:
			b.handleBroadcast(msg)
		case <-b.stop:
			return
		}
	}
}

func (b *Broadcaster) handleRegister(reg *registration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.rooms[reg.room]; !ok {
		b.rooms[reg.room] = make(map[Conn]bool)
	}
	b.rooms[reg.room][reg.conn] = true
}

func (b *Broadcaster) handleUnregister(reg *registration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conns, ok := b.rooms[reg.room]; ok {
		if _, ok := conns[reg.conn]; ok {
			delete(conns, reg.conn)
			if len(conns) == 0 {
				delete(b.rooms, reg.room)
			}
			// 同一 conn 可注册多个房间：仅当不再属于任何房间才关闭物理
			// 连接，避免单房间退出误杀其他房间的订阅
			if !b.connInAnyRoomLocked(reg.conn) {
				_ = reg.conn.Close()
			}
		}
	}
}

// connInAnyRoomLocked 检查 conn 是否仍属于任一房间（调用方需持写锁）。
func (b *Broadcaster) connInAnyRoomLocked(conn Conn) bool {
	for _, conns := range b.rooms {
		if conns[conn] {
			return true
		}
	}
	return false
}

// writeTimeout 是单次 WebSocket 写操作的超时时间。
// 防止慢消费者阻塞整个广播器。
const writeTimeout = 5 * time.Second

// defaultBroadcastWaitTimeout 是单轮广播等待所有写完成的默认上界
// (正常写入远快于此值;仅慢消费者场景触顶)。
const defaultBroadcastWaitTimeout = writeTimeout + time.Second

func (b *Broadcaster) handleBroadcast(msg *broadcastMessage) {
	b.mu.RLock()
	conns, ok := b.rooms[msg.room]
	if !ok {
		b.mu.RUnlock()
		return
	}
	connections := make([]Conn, 0, len(conns))
	for conn := range conns {
		connections = append(connections, conn)
	}
	b.mu.RUnlock()

	// 使用 goroutine fan-out 写入，避免一个慢连接阻塞其他连接。
	// 失败的连接直接调用 handleUnregister 清理（mutex 保护，线程安全）。
	// 不能通过 unregister channel 发送：run() goroutine 正在 handleBroadcast 内执行，
	// 无法回到 select 分支消费 channel，会导致死锁。
	var wg sync.WaitGroup
	for _, conn := range connections {
		wg.Add(1)
		// async.GoSafe(带 recover):写 panic 时清理连接,同时满足"禁止裸
		// go func"的项目约定(2026-09-06 复审)
		async.GoSafe(func() {
			defer wg.Done()
			// 设置写超时，防止慢消费者阻塞(Conn 契约要求实现;若实现为
			// no-op,下方有等待兜底超时,不会拖死广播器)
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(TextMessage, msg.message); err != nil {
				b.handleUnregister(&registration{room: msg.room, conn: conn})
			}
		})
	}

	// 等待兜底(2026-09-06 复审修复):原实现 wg.Wait() 无上界 — 任一连接
	// 写慢(最长 writeTimeout)期间 run() 循环被占住,Register/Unregister/
	// Broadcast 三个无缓冲 channel 全部阻塞;若 Conn 的 SetWriteDeadline
	// 是 no-op(接口允许),WriteMessage 永久阻塞 → 广播器整体死锁。
	// 现在最多等 writeTimeout+1s,超时放弃等待继续服务(慢连接的在途写
	// goroutine 由连接层错误自行收敛;SetWriteDeadline no-op 的实现会
	// 滞留一个 goroutine,属 Conn 实现方违约,见接口注释)。
	done := make(chan struct{})
	// async.GoSafe:满足"禁止裸 go func"的项目约定(2026-09-15 治理轮)
	async.GoSafe(func() {
		wg.Wait()
		close(done)
	})
	select {
	case <-done:
	case <-time.After(b.waitTimeout):
		wsLog.Warn("broadcast wait timeout; slow consumers dropped for this round",
			"room", msg.room, "conns", len(connections), "timeout", b.waitTimeout)
	}
}

// Register 将连接加入指定房间。
func (b *Broadcaster) Register(room string, conn Conn) {
	if b.closed.Load() {
		return
	}
	select {
	case b.register <- &registration{room: room, conn: conn}:
	case <-b.stop:
	}
}

// Unregister 将连接从指定房间移除。
// 同一连接可注册多个房间；物理连接仅在最后一个房间移除时关闭。
func (b *Broadcaster) Unregister(room string, conn Conn) {
	if b.closed.Load() {
		return
	}
	select {
	case b.unregister <- &registration{room: room, conn: conn}:
	case <-b.stop:
	}
}

// Broadcast 向指定房间广播原始字节消息。
func (b *Broadcaster) Broadcast(room string, message []byte) {
	if b.closed.Load() {
		return
	}
	select {
	case b.broadcast <- &broadcastMessage{room: room, message: message}:
	case <-b.stop:
	}
}

// BroadcastEvent 向指定房间广播事件（自动序列化为 JSON 并添加时间戳）。
func (b *Broadcaster) BroadcastEvent(room string, event Event) {
	if b.closed.Load() {
		return
	}
	event.Timestamp = time.Now()
	data, err := json.Marshal(event)
	if err != nil {
		// 2026-09-06 复审:原实现静默吞掉 marshal 错误(违反"所有 error
		// 必须处理"),至少要留日志
		wsLog.Warn("BroadcastEvent marshal failed; event dropped",
			"room", room, "event_type", event.Type, "err", err)
		return
	}
	b.Broadcast(room, data)
}

// GetRoomCount 返回指定房间的连接数。
func (b *Broadcaster) GetRoomCount(room string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if conns, ok := b.rooms[room]; ok {
		return len(conns)
	}
	return 0
}

// GetTotalConnections 返回所有房间的总连接数。
func (b *Broadcaster) GetTotalConnections() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, conns := range b.rooms {
		total += len(conns)
	}
	return total
}

// GetRooms 返回所有活跃房间名。
func (b *Broadcaster) GetRooms() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rooms := make([]string, 0, len(b.rooms))
	for room := range b.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// Close 关闭广播器，断开所有连接。
func (b *Broadcaster) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	close(b.stop)
	b.mu.Lock()
	defer b.mu.Unlock()
	for room, conns := range b.rooms {
		for conn := range conns {
			_ = conn.Close()
		}
		delete(b.rooms, room)
	}
}
