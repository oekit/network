package streamx

import (
	"context"
	"errors"
	"sync"

	"github.com/oekit/logx"
)

// hubLog 是 streamx hub 专用 logger(sink emit 失败留痕)。
var hubLog = logx.Delegating("streamx")

// Hub 协调 N 个 Source → N 个 Sink 的多对多转发。
// Run 阻塞直到 ctx 取消或所有 Source 耗尽/出错。
//
// 设计要点:
//   - 单 goroutine 串行读 Source(避免 backpressure 复杂处理)
//   - 每 emit 到一个 Sink 是独立调用,任一失败仅记录不中断流
//   - Source/Sink 须在 Run 前 Attach:Run 启动时对两者做快照,Run 中途
//     Attach 的对本次 Run 不可见(2026-09-06 文档修正,原注释"可在 Run
//     前后动态 Attach"与实现不符)
type Hub struct {
	mu      sync.RWMutex
	sources []Source
	sinks   []Sink
	closed  bool
}

// NewHub 构造空 hub,业务调 AttachSource/AttachSink 装配。
func NewHub() *Hub {
	return &Hub{}
}

// AttachSource 挂一个 Source 到 hub,Hub 关闭后调用返回 false。
func (h *Hub) AttachSource(s Source) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.sources = append(h.sources, s)
	return true
}

// AttachSink 挂一个 Sink 到 hub,Hub 关闭后调用返回 false。
func (h *Hub) AttachSink(s Sink) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.sinks = append(h.sinks, s)
	return true
}

// Run 阻塞调度:从每个 Source 拉事件,fork 到所有 Sink。任一 Source
// 返回 io.EOF 或其他错误,该 Source 被跳过;ctx 取消时返回 ctx.Err();
// 没有 Source 的话立即返回 nil。
func (h *Hub) Run(ctx context.Context) error {
	h.mu.RLock()
	if len(h.sources) == 0 {
		h.mu.RUnlock()
		return nil
	}
	sources := make([]Source, len(h.sources))
	copy(sources, h.sources)
	sinks := make([]Sink, len(h.sinks))
	copy(sinks, h.sinks)
	h.mu.RUnlock()

	for _, src := range sources {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			ev, err := src.Next(ctx)
			if err != nil {
				if errors.Is(err, ErrStreamExhausted) || errors.Is(err, ErrSinkClosed) {
					break // 当前源耗尽,跳下一个源
				}
				// 其他错误也跳过当前源(避免坏源拖死整个 hub)
				break
			}
			for _, snk := range sinks {
				if err := snk.Emit(ctx, ev); err != nil {
					// 单 sink 失败继续(对应官方 MultiSink 的扇出语义),
					// 但必须留痕 — 原实现连注释说的"记录"都没有
					// (2026-09-06 复审修复)
					hubLog.Warn("streamx sink emit failed",
						"err", err, "event_type", ev.Type)
				}
			}
		}
	}
	return nil
}

// Close 关闭 hub,后续 Attach 返回 false。已注册的 Source/Sink 由
// 业务自行 Close;Hub 不主动关它们(避免重复关闭)。
func (h *Hub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	return nil
}
