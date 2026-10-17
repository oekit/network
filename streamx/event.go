// Package streamx 提供跨 SSE / WebSocket / Chunked Transfer 协议的
// 流式响应抽象(Event/Source/Sink/Hub),跟 ekit 已有
// hertz/sse(Stream 单连接 SSE)+ wsutil(Broadcaster 房间广播)
// 协同工作,业务可一次写 Event,多协议扇出。
//
// 设计灵感:hertz 官方教程 https://www.cloudwego.io/zh/docs/hertz/tutorials/basic-feature/sse/
// 跟 hertz-contrib/websocket 各自独立,ekit streamx 在它们之上加
// "跨协议统一事件"层,让 AI 助手/聊天/通知等场景能用同一份事件源
// 扇出到 HTTP SSE + WebSocket(浏览器+App 端)。
//
// 已知未桥接:hertz-contrib/streamx(不存在)、kitex-contrib/streamx
// (也不存在),本包是 ekit 自己的抽象层。
package streamx

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// Sequence 是单调递增的事件序号,业务可借此判断丢包/重排。
// 0 表示未分配(由 Sink/Hub 在 Emit 时填充)。
type Sequence uint64

// Event 是流式响应的最小数据单元,跨 SSE/WS/Chunked Transfer 通用。
//
//   - ID: SSE Last-Event-ID / WS 业务自定义;空表示不写入 id 字段。
//   - Type: SSE event / WS message type;空表示"message"默认。
//   - Data: 业务数据(序列化策略由 Sink 决定:JSON for SSE/WS,raw for chunked)。
//   - Retry: SSE retry 字段(毫秒)。⚠️ 当前 SSEStreamOp 适配层未暴露
//     retry 位,置值无效(2026-09-06 文档修正)。
type Event struct {
	ID    string
	Type  string
	Data  any
	Retry int
}

// ErrSinkClosed 表示 Sink 已关闭,Emit 返回它。
var ErrSinkClosed = errors.New("streamx: sink closed")

// Source 是流式输出源抽象(拉模式)。Next 返回 io.EOF 或
// context.Canceled 表示流结束。
type Source interface {
	// Next 拉下一个事件;io.EOF 表示源耗尽。
	Next(ctx context.Context) (Event, error)
	// Close 释放源资源(Source 决定何时关)。
	Close() error
}

// Sink 是流式输出目标抽象(推模式)。Emit 把事件写到具体协议端
// (SSE / WS / chunked HTTP / 自定义)。
type Sink interface {
	// Emit 推一个事件;返回 error 表示推送失败(io.ErrClosedPipe 表示
	// 客户端断开)。
	Emit(ctx context.Context, ev Event) error
	// Close 关闭 sink,后续 Emit 返回 ErrSinkClosed。
	Close() error
}

// SliceSource 把内存切片包装成 Source,适合测试和简单 case。
type SliceSource struct {
	mu    sync.Mutex
	evs   []Event
	idx   int
	seq   Sequence
	once  sync.Once
	close chan struct{}
}

// NewSliceSource 从事件列表构造 Source,事件会被 emit 0..N-1 次。
func NewSliceSource(evs []Event) *SliceSource {
	return &SliceSource{
		evs:   evs,
		close: make(chan struct{}),
	}
}

// Next 实现 Source。
func (s *SliceSource) Next(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	case <-s.close:
		return Event{}, ErrSinkClosed
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.evs) {
		return Event{}, ErrStreamExhausted
	}
	ev := s.evs[s.idx]
	s.idx++
	s.seq++
	if ev.ID == "" {
		ev.ID = sequenceToID(s.seq)
	}
	return ev, nil
}

// Close 实现 Source。
func (s *SliceSource) Close() error {
	s.once.Do(func() { close(s.close) })
	return nil
}

// ErrStreamExhausted 表示 SliceSource 已 emit 完所有事件。
var ErrStreamExhausted = errors.New("streamx: source exhausted")

// sequenceToID 把 Sequence 转成"seq-N"格式的字符串 ID。
func sequenceToID(s Sequence) string {
	const digits = "0123456789"
	if s == 0 {
		return "seq-0"
	}
	var buf [20]byte
	i := len(buf)
	for s > 0 {
		i--
		buf[i] = digits[s%10]
		s /= 10
	}
	return "seq-" + string(buf[i:])
}

// MarshalEvent 把 Event.Data 序列化成 []byte(默认 JSON)。Sink 实现可
// 用这个 helper 走统一 JSON 编码。
func MarshalEvent(ev Event) ([]byte, error) {
	if raw, ok := ev.Data.([]byte); ok {
		return raw, nil
	}
	if s, ok := ev.Data.(string); ok {
		return []byte(s), nil
	}
	return json.Marshal(ev.Data)
}
