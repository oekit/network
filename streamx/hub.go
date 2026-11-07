package streamx

import (
	"context"
	"errors"
	"sync"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
)

// hubLog 是 streamx hub 专用 logger(sink emit 失败留痕)。
var hubLog = logx.Delegating("streamx")

// Hub 协调 N 个 Source → N 个 Sink 的多对多转发。
// Run 阻塞直到 ctx 取消或所有 Source 耗尽/出错。
//
// 设计要点:
//   - 每 Source 一个拉取 goroutine 并发拉取,经无缓冲 channel 汇聚后由 Run
//     串行扇出到 Sink(#82 #85:长驻源的 Next 会阻塞等下一条事件,顺序读会
//     饿死后续源;每源至多一条在途事件 = 天然背压)
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
//
// 不同 Source 的事件没有全局排序保证(并发拉取,先到先扇出);
// 单个 Source 内部保持 Next 返回的顺序。
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

	// 并发拉取（#82 #85）：轮询调度（#65）只救了「Next 返回快」的源——
	// 长驻源（SSE 聊天、事件流）的 Next 会阻塞等下一条事件，顺序轮到它时
	// 整个调度卡住，排在后面的源一条都转发不了，多源实际退化为单源。
	// 现在每个 Source 一个拉取 goroutine 并发 Next，结果经 events 汇聚后
	// 仍由本函数串行扇出到全部 Sink：
	//   - 无缓冲 events = 每源至多一条在途事件（天然背压，快源不淹没慢源）；
	//   - 扇出保持 #65/#67 语义：耗尽/出错的源退出、坏源跳过必留痕、
	//     单 sink 失败仅留痕不中断流。
	type pullResult struct {
		idx int
		ev  Event
		err error
	}
	events := make(chan pullResult)
	var pullWG sync.WaitGroup
	for i, src := range sources {
		pullWG.Add(1)
		async.GoSafe(func() {
			defer pullWG.Done()
			for {
				ev, err := src.Next(ctx)
				select {
				case events <- pullResult{idx: i, ev: ev, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return // 源耗尽/出错（或 ctx 取消）：本源退出
				}
			}
		})
	}
	// 全部拉取 goroutine 退出后关闭 done，结束下方调度循环。
	done := make(chan struct{})
	async.GoSafe(func() {
		pullWG.Wait()
		close(done)
	})

	for {
		select {
		case res := <-events:
			if res.err != nil {
				if !errors.Is(res.err, ErrStreamExhausted) && !errors.Is(res.err, ErrSinkClosed) {
					// #67：坏源跳过必须留痕（耗尽/关闭属正常收尾，不留痕）
					hubLog.Warn("streamx source failed, skipping source",
						"err", res.err, "source_index", res.idx)
				}
				continue
			}
			for _, snk := range sinks {
				if err := snk.Emit(ctx, res.ev); err != nil {
					// 单 sink 失败继续(对应官方 MultiSink 的扇出语义),
					// 但必须留痕 — 原实现连注释说的"记录"都没有
					// (2026-09-06 复审修复)
					hubLog.Warn("streamx sink emit failed",
						"err", err, "event_type", res.ev.Type)
				}
			}
		case <-done:
			// 源全部耗尽/出错。ctx 恰在此窗口取消时沿用旧语义（旧实现
			// 每轮起始先查 ctx.Done）优先返回取消原因。
			return ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
