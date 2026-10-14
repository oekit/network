package streamx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// 或避免把多个 sink 挂在同一个串行 Hub 上。
type SSESink struct {
	stream SSEStreamOp
	// closed 用 atomic：文档化用法是 handler defer Close 与 Hub goroutine
	// Emit 并发（见 NewSSESinkFromFuncs 示例），裸 bool 是数据竞争。
	closed atomic.Bool
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
// ID 透传(2026-09-06 复审修复):Last-Event-ID 断线续传依赖 event id,
// 原实现对 JSON 载荷走 Send 把 ev.ID 整个丢掉 — 现在带 ID 的事件一律
// 编码后走 SendRaw。[]byte 载荷仍直走 SendRaw(避免 JSON 双编码)。
//
// ⚠️ Event.Retry 当前不透传:SSEStreamOp 的 Send/SendRaw 签名没有 retry
// 位,上游 hertz sse.Writer.WriteEvent 虽支持,ekit 适配层未暴露 — 置该
// 字段无效(字段文档已同步修正,不再宣称"仅 SSE 生效")。
func (s *SSESink) Emit(_ context.Context, ev Event) error {
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

// Close 实现 Sink,幂等。Swap 保证并发 Close 只有首个真正下发。
func (s *SSESink) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.stream.Close()
}

// ErrSinkEmpty 保留 sentinel(避免 unused 误删,实际用 ErrSinkClosed)。
var ErrSinkEmpty = errors.New("streamx: empty sink data")
