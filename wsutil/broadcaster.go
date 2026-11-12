package wsutil

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
)

// wsLog 是 wsutil 包专用 logger(marshal 失败、慢消费者告警)。
var wsLog = logx.Delegating("wsutil")

// Conn 是 WebSocket 连接的最小接口。
// github.com/hertz-contrib/websocket.Conn 满足此接口。
type Conn interface {
	WriteMessage(messageType int, data []byte) error
	Close() error
	// SetWriteDeadline 设置写超时。它是**必选**方法，且必须真正生效——
	// no-op 实现属违约：WriteMessage 可能永久阻塞，Broadcaster 对该连接
	// 只能跳过后续写（见 handleBroadcast 的 #87 说明）。
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
	// writeMu 按 Conn 记录串行写锁条目（#87）：保证同一连接上任意时刻至多
	// 一个在途 WriteMessage——websocket 连接并发写会导致帧交错损坏。
	// 条目生命周期与 Conn 一致（#142）：写协程归还（releaseWriteLock）与
	// Unregister 收尾处，在「无在途写协程窗口 且 conn 不属于任何房间」时
	// 回收条目——否则映射持强引用把已断开的 Conn 连同读写缓冲一起钉死，
	// 轮换型推送服务内存无上界。安全回收协议见 connWriteEntry。
	writeMu map[Conn]*connWriteEntry // 受 b.mu 保护
}

// connWriteEntry 是一个 Conn 的串行写锁条目（#87）。
//
// users 计「已取到本条目、尚未归还」的写协程数（受 Broadcaster.mu 保护）。
// 条目回收协议（#142）：users 归零且 conn 不属于任何房间时条目就地删除。
// 这是**安全**的——users 归零即不存在任何持有者（含「已取条目、尚未
// TryLock」的缝隙窗口），此刻删除后新写经 connWriteLock 建新条目，不会
// 与旧写分裂成两把锁并发写坏帧（#87 的红线）；反之只要还有一个写协程
// 窗口开着，条目就留在表里，重注册后的新写与在途旧写继续用同一把锁。
type connWriteEntry struct {
	mu    sync.Mutex
	users int
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
		writeMu:     make(map[Conn]*connWriteEntry),
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
			// 回收写锁条目（#142）：此刻 conn 已不属任何房间；无在途写
			// （users==0）立即可删，有则留待写协程归还时收尾（同一判据）。
			if e, ok := b.writeMu[reg.conn]; ok {
				b.reclaimWriteLockLocked(reg.conn, e)
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
			// 同 conn 写串行化（#87）：上一轮等待超时后仍在途的写与下一轮的
			// 新写并发进行 = websocket 帧交错损坏。TryLock 失败说明该连接上一
			// 轮的写仍未返回——只有 SetWriteDeadline 为 no-op 的违约实现才可能
			// 撑过整轮 waitTimeout(> writeTimeout)。跳过本轮写并留痕（连接保留
			// 在房间里：写恢复后下一轮照常投递；若物理连接已被对端/业务关闭，
			// 卡死的写会以错误返回并触发下方的注销清理）。
			// 条目使用窗口（#142）：取条目到归还之间条目不被回收——在途
			// 旧写与重注册后的新写必然落在同一把锁上。
			entry := b.connWriteLock(conn)
			defer b.releaseWriteLock(conn, entry)
			if !entry.mu.TryLock() {
				wsLog.Warn("broadcast write skipped: previous write still in flight",
					"room", msg.room)
				return
			}
			defer entry.mu.Unlock()
			// 设置写超时，防止慢消费者阻塞(Conn 契约要求实现且必须生效;
			// 若实现为 no-op,上方 TryLock 串行化兜底,不会并发写坏帧)
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(TextMessage, msg.message); err != nil {
				b.handleUnregister(&registration{room: msg.room, conn: conn})
			}
		})
	}

	// 等待兜底(2026-09-06 复审修复):原实现 wg.Wait() 无上界 — 任一连接
	// 写慢(最长 writeTimeout)期间 run() 循环被占住,Register/Unregister/
	// Broadcast 三个无缓冲 channel 全部阻塞;若 Conn 的 SetWriteDeadline
	// 是 no-op,WriteMessage 永久阻塞 → 广播器整体死锁。
	// 现在最多等 waitTimeout,超时放弃等待继续服务(慢连接的在途写由连接层
	// 错误自行收敛;SetWriteDeadline no-op 的实现会滞留一个 goroutine,属
	// Conn 实现方违约,见接口注释)。#87 串行化保证：超时放弃后，下一轮对
	// 仍在途写的连接不会再启动新写（TryLock 跳过），不会并发写同一 Conn。
	done := make(chan struct{})
	// async.GoSafe:满足"禁止裸 go func"的项目约定(2026-09-15 治理轮)
	async.GoSafe(func() {
		wg.Wait()
		close(done)
	})
	// waitTimeout 计时用可停 timer（#169）：提前完成（done 先到）时 defer
	// Stop 归还 timer，不再遗留一个挂到 waitTimeout 的未停 timer——广播高频
	// 的房间里这是每轮一次的 timer 堆积面。
	timer := time.NewTimer(b.waitTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		wsLog.Warn("broadcast wait timeout; slow consumers dropped for this round",
			"room", msg.room, "conns", len(connections), "timeout", b.waitTimeout)
	}
}

// connWriteLock 返回（或首次创建）conn 的串行写锁条目（#87），并登记一个
// 使用窗口（users++）：调用方用完必须 releaseWriteLock 归还，窗口内条目
// 不会被回收（#142）。
func (b *Broadcaster) connWriteLock(conn Conn) *connWriteEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.writeMu[conn]
	if !ok {
		e = &connWriteEntry{}
		b.writeMu[conn] = e
	}
	e.users++
	return e
}

// releaseWriteLock 归还 connWriteLock 登记的使用窗口，并在安全时回收条目（#142）。
func (b *Broadcaster) releaseWriteLock(conn Conn, e *connWriteEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e.users--
	b.reclaimWriteLockLocked(conn, e)
}

// reclaimWriteLockLocked 在「无使用窗口 且 conn 不属于任何房间」时回收
// 条目（#142）。调用方需持 b.mu 写锁；条目已被新写替换时不动（恒等校验）。
func (b *Broadcaster) reclaimWriteLockLocked(conn Conn, e *connWriteEntry) {
	if e.users > 0 || b.connInAnyRoomLocked(conn) {
		return
	}
	if cur, ok := b.writeMu[conn]; ok && cur == e {
		delete(b.writeMu, conn)
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
//
// ⚠️ **message 会被拷贝一份**（#78，与 Outbox.Send 同口径）。投递进 channel
// 之后、WriteMessage 真正执行之前有一段异步窗口；调用方若复用 buffer
// （buf = buf[:0] 再填、循环里 marshal 进同一块内存——高性能广播路径的
// 标准写法），在途消息会被改写成另一帧的内容：客户端收到串包/坏 JSON，
// 服务端无任何报错。一次拷贝换「广播出去的字节确定是 Broadcast 返回那一刻
// 的字节」。BroadcastEvent 每次 marshal 出新切片，天然安全。
func (b *Broadcaster) Broadcast(room string, message []byte) {
	if b.closed.Load() {
		return
	}
	message = cloneBytes(message)
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
			"room", room, "event_type", event.Type, logx.ErrKey, err)
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
	// 一并丢弃写锁条目（#142）：广播器关闭后不再有新写，残留条目只会
	// 钉住 Conn；在途写的 releaseWriteLock 对已移除条目幂等（恒等校验）。
	clear(b.writeMu)
}
