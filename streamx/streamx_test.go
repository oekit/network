package streamx

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeSink 是测试用 Sink,记录所有 Emit 调用。
type fakeSink struct {
	events []Event
	err    error
	closed bool
}

func (f *fakeSink) Emit(_ context.Context, ev Event) error {
	if f.closed {
		return ErrSinkClosed
	}
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeSink) Close() error { f.closed = true; return nil }

// fakeSource 是测试用 Source,逐个返回给定 events,然后返回 ErrStreamExhausted。
type fakeSource struct {
	events []Event
	idx    int
	closed bool
}

func (f *fakeSource) Next(_ context.Context) (Event, error) {
	if f.closed {
		return Event{}, ErrSinkClosed
	}
	if f.idx >= len(f.events) {
		return Event{}, ErrStreamExhausted
	}
	ev := f.events[f.idx]
	f.idx++
	return ev, nil
}
func (f *fakeSource) Close() error { f.closed = true; return nil }

// TestSliceSource_OrderAndIDs 验证 SliceSource 按顺序返回 + 缺省 ID 自动生成。
func TestSliceSource_OrderAndIDs(t *testing.T) {
	src := NewSliceSource([]Event{
		{Type: "message", Data: "first"},
		{Type: "message", Data: "second"},
		{Type: "ping", Data: nil},
	})
	defer src.Close()

	ctx := context.Background()
	ev1, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("Next 1: %v", err)
	}
	if ev1.Data != "first" || ev1.ID != "seq-1" {
		t.Errorf("ev1 = %+v, want Data=first ID=seq-1", ev1)
	}
	ev2, _ := src.Next(ctx)
	if ev2.Data != "second" || ev2.ID != "seq-2" {
		t.Errorf("ev2 = %+v, want Data=second ID=seq-2", ev2)
	}
	ev3, _ := src.Next(ctx)
	if ev3.Type != "ping" || ev3.ID != "seq-3" {
		t.Errorf("ev3 = %+v, want Type=ping ID=seq-3", ev3)
	}
	_, err = src.Next(ctx)
	if !errors.Is(err, ErrStreamExhausted) {
		t.Errorf("Next after exhaust: err = %v, want ErrStreamExhausted", err)
	}
}

// TestSliceSource_PreservesExplicitID 验证业务显式给的 ID 不会被覆盖。
func TestSliceSource_PreservesExplicitID(t *testing.T) {
	src := NewSliceSource([]Event{
		{ID: "custom-1", Data: "x"},
	})
	defer src.Close()
	ev, _ := src.Next(context.Background())
	if ev.ID != "custom-1" {
		t.Errorf("ev.ID = %q, want custom-1", ev.ID)
	}
}

// TestSliceSource_ContextCancel 验证 ctx 取消时 Next 返回 ctx.Err()。
func TestSliceSource_ContextCancel(t *testing.T) {
	src := NewSliceSource([]Event{{Data: "a"}, {Data: "b"}})
	defer src.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := src.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// blockingSource 模拟长驻流（SSE 聊天/事件流）：Next 阻塞等下一条事件，
// 直到事件被推入或 ctx 取消——正是 #82/#85 里「排在前面就饿死后续源」的
// 那种源。
type blockingSource struct {
	events chan Event
}

func newBlockingSource() *blockingSource {
	return &blockingSource{events: make(chan Event)}
}

func (b *blockingSource) Next(ctx context.Context) (Event, error) {
	select {
	case ev := <-b.events:
		return ev, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
func (b *blockingSource) Close() error { return nil }

// chanSink 把每个收到的事件送入 channel，测试侧用同步收代替轮询断言。
type chanSink struct {
	events chan Event
}

func (s *chanSink) Emit(_ context.Context, ev Event) error {
	s.events <- ev
	return nil
}
func (s *chanSink) Close() error { return nil }

// TestHub_LongLivedSourceDoesNotStarveOthers 回归（#82 #85）：长驻源挂第 0 位、
// Next 阻塞等事件时，后续源的事件必须照常扇出。顺序读（以及只解决「Next
// 返回快」交错的 #65 轮询）在源 0 的 Next 上阻塞 ⇒ 源 1 一条都转发不出——
// 多源退化成单源。修复后每源一个拉取 goroutine，源 1 无需等源 0。
func TestHub_LongLivedSourceDoesNotStarveOthers(t *testing.T) {
	live := newBlockingSource() // 挂第 0 位，一直不出事件
	finite := &fakeSource{events: []Event{{Type: "msg", Data: "a"}, {Type: "msg", Data: "b"}}}
	sink := &chanSink{events: make(chan Event, 2)}
	h := NewHub()
	h.AttachSource(live)
	h.AttachSource(finite)
	h.AttachSink(sink)
	defer h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- h.Run(ctx) }()

	// 核心断言：源 0 阻塞在 Next 上，源 1 的两条事件必须已全部到达 sink。
	for i, want := range []string{"a", "b"} {
		select {
		case ev := <-sink.events:
			if ev.Data != want {
				t.Fatalf("event %d = %v, want %q", i, ev.Data, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("源 0 阻塞饿死了源 1：第 %d 条事件未到达——#82/#85 回归", i+1)
		}
	}

	// 取消 ctx：阻塞的源 0 以 ctx 错误退出，Run 返回 ctx.Err()。
	cancel()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestHub_ConcurrentSourcesFanout 验证并发拉取下多个长驻源的事件
// 都能到达 sink（每源至少一条），不存在「谁排前面谁独占」。
func TestHub_ConcurrentSourcesFanout(t *testing.T) {
	a := newBlockingSource()
	b := newBlockingSource()
	sink := &chanSink{events: make(chan Event, 4)}
	h := NewHub()
	h.AttachSource(a)
	h.AttachSource(b)
	h.AttachSink(sink)
	defer h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- h.Run(ctx) }()

	a.events <- Event{Type: "a", Data: 1}
	b.events <- Event{Type: "b", Data: 2}

	got := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case ev := <-sink.events:
			got[ev.Type] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("只收到 %v，两个长驻源的事件应都能扇出", got)
		}
	}
	if !got["a"] || !got["b"] {
		t.Errorf("expected events from both sources, got %v", got)
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestHub_Fanout 验证 Hub 1 source → 2 sinks 扇出。
func TestHub_Fanout(t *testing.T) {
	src := &fakeSource{events: []Event{
		{Type: "a", Data: 1},
		{Type: "b", Data: 2},
	}}
	sink1 := &fakeSink{}
	sink2 := &fakeSink{}
	h := NewHub()
	h.AttachSource(src)
	h.AttachSink(sink1)
	h.AttachSink(sink2)
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(sink1.events) != 2 {
		t.Errorf("sink1 events = %d, want 2", len(sink1.events))
	}
	if len(sink2.events) != 2 {
		t.Errorf("sink2 events = %d, want 2", len(sink2.events))
	}
	if sink1.events[0].Data != 1 || sink2.events[1].Data != 2 {
		t.Errorf("event content mismatch: %+v / %+v", sink1.events, sink2.events)
	}
}

// TestHub_OneSinkFailureDoesNotBlockOthers 验证单个 sink 失败
// 不阻塞其他 sink 接收事件(对应 MultiSink 扇出语义)。
func TestHub_OneSinkFailureDoesNotBlockOthers(t *testing.T) {
	src := &fakeSource{events: []Event{{Data: "x"}, {Data: "y"}}}
	okSink := &fakeSink{}
	failSink := &fakeSink{err: errors.New("disk full")}
	h := NewHub()
	h.AttachSource(src)
	h.AttachSink(failSink)
	h.AttachSink(okSink)
	defer h.Close()
	if err := h.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(okSink.events) != 2 {
		t.Errorf("ok sink events = %d, want 2 (fail sink should not block)", len(okSink.events))
	}
}

// TestHub_EmptyReturnsNil 验证无 Source 时 Run 立即返回 nil。
func TestHub_EmptyReturnsNil(t *testing.T) {
	h := NewHub()
	h.AttachSink(&fakeSink{})
	defer h.Close()
	if err := h.Run(context.Background()); err != nil {
		t.Errorf("Run on empty hub: err = %v, want nil", err)
	}
}

// TestHub_AttachAfterClose 验证 Close 之后 Attach 返回 false。
func TestHub_AttachAfterClose(t *testing.T) {
	h := NewHub()
	h.Close()
	if h.AttachSource(&fakeSource{}) {
		t.Error("AttachSource after Close should return false")
	}
	if h.AttachSink(&fakeSink{}) {
		t.Error("AttachSink after Close should return false")
	}
}

// TestHub_ContextCancel 验证 ctx 取消时 Run 返回 ctx.Err()。
func TestHub_ContextCancel(t *testing.T) {
	src := &fakeSource{events: []Event{{Data: "a"}}}
	sink := &fakeSink{}
	h := NewHub()
	h.AttachSource(src)
	h.AttachSink(sink)
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Run on cancelled ctx: err = %v, want context.Canceled", err)
	}
}

// TestSSESink_EmitBytesAndData 验证 SSESink 对 []byte vs any 分流。
func TestSSESink_EmitBytesAndData(t *testing.T) {
	var sendCalls, sendRawCalls, closeCalls int
	sink := NewSSESinkFromFuncs(
		func(_ string, _ any) error { sendCalls++; return nil },
		func(_, _ string, _ []byte) error { sendRawCalls++; return nil },
		func() error { return nil },
		func() error { closeCalls++; return nil },
	)
	defer sink.Close()

	if err := sink.Emit(context.Background(), Event{Type: "t", Data: "hello"}); err != nil {
		t.Fatalf("Emit string: %v", err)
	}
	if sendCalls != 1 || sendRawCalls != 0 {
		t.Errorf("calls after string Emit: send=%d sendRaw=%d", sendCalls, sendRawCalls)
	}

	if err := sink.Emit(context.Background(), Event{Type: "t", Data: []byte("raw")}); err != nil {
		t.Fatalf("Emit bytes: %v", err)
	}
	if sendCalls != 1 || sendRawCalls != 1 {
		t.Errorf("calls after bytes Emit: send=%d sendRaw=%d", sendCalls, sendRawCalls)
	}
}

// TestSSESink_EmitAfterClose 验证 Close 之后 Emit 返回 ErrSinkClosed。
func TestSSESink_EmitAfterClose(t *testing.T) {
	sink := NewSSESinkFromFuncs(
		func(_ string, _ any) error { return nil },
		func(_, _ string, _ []byte) error { return nil },
		func() error { return nil },
		func() error { return nil },
	)
	sink.Close()
	err := sink.Emit(context.Background(), Event{Data: "x"})
	if !errors.Is(err, ErrSinkClosed) {
		t.Errorf("err = %v, want ErrSinkClosed", err)
	}
}

// TestMarshalEvent_AllDataTypes 验证 MarshalEvent 对不同 Data 类型。
func TestMarshalEvent_AllDataTypes(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"bytes", []byte("raw"), "raw"},
		{"string", "hello", "hello"},
		{"map", map[string]any{"k": 1}, `{"k":1}`},
		{"slice", []int{1, 2, 3}, "[1,2,3]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := MarshalEvent(Event{Data: c.in})
			if err != nil {
				t.Fatalf("MarshalEvent: %v", err)
			}
			if string(got) != c.want {
				t.Errorf("got = %q, want %q", got, c.want)
			}
		})
	}
}

// TestSequenceToID 验证 seq-N 格式。
func TestSequenceToID(t *testing.T) {
	cases := []struct {
		seq  Sequence
		want string
	}{
		{0, "seq-0"},
		{1, "seq-1"},
		{42, "seq-42"},
		{1234567890, "seq-1234567890"},
	}
	for _, c := range cases {
		if got := sequenceToID(c.seq); got != c.want {
			t.Errorf("seq %d → %q, want %q", c.seq, got, c.want)
		}
	}
}
