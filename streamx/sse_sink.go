package streamx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// SSEStreamOp 是 hertz/sse.Stream 暴露给 streamx 的最小子集(4 方法)。
// 用 type alias 而非直接 import hertz/sse 是为了避免 streamx ↔ hertz/sse
// 循环依赖;业务调用 NewSSESinkFromFuncs 时直接传 hertz/sse.Stream 的
// 方法引用。
type SSEStreamOp interface {
	Send(eventType string, data any) error
	SendRaw(id, eventType string, data []byte) error
	KeepAlive() error
	Close() error
}

// SSESink 把 Event 推到一个已打开的 hertz SSE 连接(由 hertz/sse.Stream
// 创建)。典型用法:在 handler 末尾 defer sse.NewStream(c),然后把 stream
// 4 方法传给 NewSSESinkFromFuncs 推给 Hub。
//
// 官方依据:https://www.cloudwego.io/zh/docs/hertz/tutorials/basic-feature/sse/
// + ekit hertz/sse 桥接的 Stream.Send/SendRaw。
//
// ⚠️ 背压限制：SSE 写底层无写超时（hertz app 层未暴露 write deadline），
// 读取停滞的客户端（TCP 窗口耗尽）会阻塞 Emit 调用——Hub 串行扇出场景
// 会连带阻塞其他 sink。生产部署建议在网关/负载均衡层配置发送空闲超时，
// 或避免把多个 sink 挂在同一个串行 Hub 上。同型限制也作用在 Close 上：
// Close 等在途 Emit 写完才关 stream（见 mu），慢消费者的在途写会推迟
// Close 返回。
type SSESink struct {
	stream SSEStreamOp
	// mu 让「Emit 的关闭检查 + stream 写入」成为同一个临界区（#144，与
	// wsutil.Outbox 的 #117 同构修法）：Emit 持 RLock，Close 持 Lock。
	//
	// 为什么必须是同一临界区：旧实现先 closed.Load() 再写 stream，两步之间
	// 没有 happens-before——Close 可落在中间：通过检查的 Emit 把事件写进
	// **已关闭**的 stream，返回底层 stream 错误而非 ErrSinkClosed（契约
	// 「Close 关闭 sink，后续 Emit 返回 ErrSinkClosed」被击穿），Hub 把正常
	// 关闭当 sink 故障留痕（hub.go 的 "streamx sink emit failed"），断线
	// 续传窗口的最后一帧落进「写了但报错」的灰色态。RWMutex 的排他性堵住
	// 这个交错：Lock 等所有 RLock 离场 ⇒ Close 的临界区开始时所有在途
	// Emit 已写完，之后的 Emit 一律收 ErrSinkClosed、stream 不再被写。
	//
	// 代价（契约明示，issue #144 两选一中取「Close 等待在途 Emit」）：
	// Emit 是阻塞写，Close 等在途 Emit 完成后才关 stream——慢消费者的
	// 在途写会推迟 Close 返回（背压限制同型，见类型文档）；换来「stream
	// 关闭不与在途写并发」的确定性（hertz stream 的写-关并发行为未定义，
	// 正是要堵的窗口）。
	mu sync.RWMutex
	// closed 用 atomic：文档化用法是 handler defer Close 与 Hub goroutine
	// Emit 并发（见 NewSSESinkFromFuncs 示例），裸 bool 是数据竞争。
	// 置位发生在 Close 的临界区内（与 stream 关闭原子），读取发生在
	// Emit 的 RLock 内（与写入原子）。
	closed atomic.Bool
	// closeOnce 保证 stream.Close 只下发一次、重复 Close 返回 nil——
	// 矩阵硬规则「组件 Close 统一 Close(ctx) error + sync.Once 幂等」的
	// 幂等半句；用 sync.Once + 局部结果而非 sync.OnceFunc/OnceValue：
	// 「仅首个调用返回关闭结果、后续返回 nil」的口径（nats.Client.Close
	// 同款）需要 Do 内赋值局部变量的形态，OnceValue 的结果缓存口径不同。
	closeOnce sync.Once
}

// NewSSESinkFromFuncs 显式传入 4 个方法构造 SSESink(避免 ekit package
// 内部循环依赖)。
//
// 典型用法(把 hertz/sse.Stream 适配成 SSESink):
//
//	stream := hertzsse.NewStream(c)
//	sink := streamx.NewSSESinkFromFuncs(stream.Send, stream.SendRaw,
//	    stream.KeepAlive, stream.Close)
//	defer sink.Close()
func NewSSESinkFromFuncs(
	send func(eventType string, data any) error,
	sendRaw func(id, eventType string, data []byte) error,
	keepAlive func() error,
	close func() error,
) *SSESink {
	return &SSESink{
		stream: sseFuncAdapter{send: send, sendRaw: sendRaw, keep: keepAlive, close: close},
	}
}

// sseFuncAdapter 把 4 个 func 适配成 SSEStreamOp interface。
type sseFuncAdapter struct {
	send    func(string, any) error
	sendRaw func(string, string, []byte) error
	keep    func() error
	close   func() error
}

func (a sseFuncAdapter) Send(t string, d any) error           { return a.send(t, d) }
func (a sseFuncAdapter) SendRaw(id, t string, d []byte) error { return a.sendRaw(id, t, d) }
func (a sseFuncAdapter) KeepAlive() error                     { return a.keep() }
func (a sseFuncAdapter) Close() error                         { return a.close() }

// Emit 实现 Sink。
//
// 「closed 检查 + stream 写入」持 RLock 是同一个临界区（#144，与
// wsutil.Outbox.Send 的 #117 同构口径）：不存在「检查通过了、事件却落在
// Close 之后」的交错——写入只会发生在 stream 关闭之前，或干脆不发生而
// 返回 ErrSinkClosed。
//
// ID 透传(2026-09-06 复审修复):Last-Event-ID 断线续传依赖 event id,
// 原实现对 JSON 载荷走 Send 把 ev.ID 整个丢掉 — 现在带 ID 的事件一律
// 编码后走 SendRaw。[]byte 载荷仍直走 SendRaw(避免 JSON 双编码)。
//
// ⚠️ Event.Retry 当前不透传:SSEStreamOp 的 Send/SendRaw 签名没有 retry
// 位,上游 hertz sse.Writer.WriteEvent 虽支持,ekit 适配层未暴露 — 置该
// 字段无效(字段文档已同步修正,不再宣称"仅 SSE 生效")。
func (s *SSESink) Emit(_ context.Context, ev Event) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return ErrSinkClosed
	}
	if raw, ok := ev.Data.([]byte); ok {
		return s.stream.SendRaw(ev.ID, ev.Type, raw)
	}
	if ev.ID != "" {
		encoded, err := json.Marshal(ev.Data)
		if err != nil {
			return fmt.Errorf("streamx: marshal event %q: %w", ev.Type, err)
		}
		return s.stream.SendRaw(ev.ID, ev.Type, encoded)
	}
	return s.stream.Send(ev.Type, ev.Data)
}

// Close 实现 Sink,幂等：仅首个调用执行 stream.Close 并返回其结果，
// 后续调用直接返回 nil（nats.Client.Close 同口径）。
//
// 「置 closed + 关 stream」持写锁，与 Emit 的临界区互斥（#144）：Close
// 等在途 Emit 写完才关 stream，之后的 Emit 一律返回 ErrSinkClosed、不再
// 触碰 stream——契约「Close 关闭 sink,后续 Emit 返回 ErrSinkClosed」在
// 并发形态下（handler defer Close 与 Hub goroutine Emit 并发，见类型
// 文档）同样成立。代价是慢消费者的在途写会推迟 Close 返回（背压限制
// 同型）。
func (s *SSESink) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed.Store(true)
		closeErr = s.stream.Close()
	})
	return closeErr
}

// ErrSinkEmpty 保留 sentinel(避免 unused 误删,实际用 ErrSinkClosed)。
var ErrSinkEmpty = errors.New("streamx: empty sink data")
