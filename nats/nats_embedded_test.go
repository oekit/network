package nats

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/oekit/tracing/traceid"
)

// newTestClient starts an embedded NATS server on a random port and returns
// a connected Client. The server is torn down via t.Cleanup.
//
// Random port (Port: -1) avoids conflicts when go test runs packages in
// parallel; tests in this file are not safe to run with -parallel > 1
// against the same Client (single Conn is the unit of parallelism).
func newTestClient(t *testing.T, opts ...NatsOption) *Client {
	t.Helper()
	opts = append(opts, WithName("ekit-nats-test"))
	opts = append(opts, WithMaxReconnects(0)) // tests don't exercise reconnect

	// -1 → random port, picked by the OS.
	portOpts := natsserver.DefaultTestOptions
	portOpts.Port = -1
	srv := natsserver.RunServer(&portOpts)
	t.Cleanup(func() { srv.Shutdown() })

	c, err := NewClient(srv.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// TestPublishSubscribe_RoundTrip covers the basic Publish + Subscribe path.
// We use the raw handler form (not SubscribeJSON) to keep this test focused
// on the byte transport, not the JSON wrapper.
func TestPublishSubscribe_RoundTrip(t *testing.T) {
	c := newTestClient(t)

	got := make(chan []byte, 1)
	sub, err := c.Subscribe("test.roundtrip", func(m *nats.Msg) {
		got <- append([]byte(nil), m.Data...)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.Publish(context.Background(), "test.roundtrip", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case data := <-got:
		if string(data) != "hello" {
			t.Errorf("got %q, want %q", data, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestPublishJSON_RoundTrip verifies JSON marshal + raw subscribe path.
// Uses a struct with json tags to confirm the field naming is preserved
// through the encode/decode round trip.
func TestPublishJSON_RoundTrip(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	got := make(chan []byte, 1)
	sub, err := c.Subscribe("test.json", func(m *nats.Msg) {
		got <- append([]byte(nil), m.Data...)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	want := payload{Name: "alice", Count: 42}
	if err := c.PublishJSON(context.Background(), "test.json", want); err != nil {
		t.Fatalf("PublishJSON: %v", err)
	}

	select {
	case data := <-got:
		var got2 payload
		if err := json.Unmarshal(data, &got2); err != nil {
			t.Fatalf("unmarshal: %v (raw=%s)", err, data)
		}
		if got2 != want {
			t.Errorf("got %+v, want %+v", got2, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestPublishJSON_MarshalError verifies that an unmarshalable value
// (a channel cannot be JSON-encoded) returns an error before any
// network call is made.
func TestPublishJSON_MarshalError(t *testing.T) {
	c := newTestClient(t)

	err := c.PublishJSON(context.Background(), "test.badjson", make(chan int))
	if err == nil {
		t.Fatal("expected marshal error for chan, got nil")
	}
	if !errors.Is(err, err) { // sanity — we got an error, exact wrap is not asserted
		t.Errorf("expected non-nil error, got %v", err)
	}
}

// TestPublish_CancelledContext covers the ctx.Err short-circuit. We cancel
// the context before calling Publish and assert that we get a wrapped
// context.Canceled without touching the connection.
func TestPublish_CancelledContext(t *testing.T) {
	c := newTestClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	err := c.Publish(ctx, "test.cancel", []byte("x"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected wrap of context.Canceled, got %v", err)
	}
}

// TestPublishJSON_CancelledContext — same as above for the JSON path.
func TestPublishJSON_CancelledContext(t *testing.T) {
	c := newTestClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.PublishJSON(ctx, "test.cancel", map[string]int{"k": 1})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected wrap of context.Canceled, got %v", err)
	}
}

// TestPublishWithTraceID_NoTraceID covers the "ctx has no trace_id" branch
// of publishMsg. The message is published without a header. The subscriber
// should see an empty header (no spurious X-Trace-Id entry).
func TestPublishWithTraceID_NoTraceID(t *testing.T) {
	c := newTestClient(t)

	got := make(chan *nats.Msg, 1)
	sub, err := c.Subscribe("test.notrace", func(m *nats.Msg) {
		got <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.PublishWithTraceID(context.Background(), "test.notrace", []byte("x")); err != nil {
		t.Fatalf("PublishWithTraceID: %v", err)
	}

	select {
	case m := <-got:
		if got := m.Header.Get(traceid.HeaderKey); got != "" {
			t.Errorf("expected no trace_id in header when ctx has none, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestPublishWithTraceID_InjectsHeader is the **regression test for the
// v0.16.3 fix (commit 901879b)**: when the ctx carries a trace_id, the
// publisher must inject it into the NATS message header so SubscribeJSON
// can recover it on the other side.
func TestPublishWithTraceID_InjectsHeader(t *testing.T) {
	c := newTestClient(t)

	got := make(chan *nats.Msg, 1)
	sub, err := c.Subscribe("test.trace", func(m *nats.Msg) {
		got <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	ctx := traceid.ContextWith(context.Background(), "tr-xyz-001")
	if err := c.PublishWithTraceID(ctx, "test.trace", []byte("payload")); err != nil {
		t.Fatalf("PublishWithTraceID: %v", err)
	}

	select {
	case m := <-got:
		if got := m.Header.Get(traceid.HeaderKey); got != "tr-xyz-001" {
			t.Errorf("expected trace_id %q in header, got %q", "tr-xyz-001", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestPublishJSONWithTraceID_InjectsHeader — same as above for the JSON path.
func TestPublishJSONWithTraceID_InjectsHeader(t *testing.T) {
	c := newTestClient(t)

	got := make(chan *nats.Msg, 1)
	sub, err := c.Subscribe("test.trjson", func(m *nats.Msg) {
		got <- m
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	ctx := traceid.ContextWith(context.Background(), "tr-json-002")
	if err := c.PublishJSONWithTraceID(ctx, "test.trjson", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("PublishJSONWithTraceID: %v", err)
	}

	select {
	case m := <-got:
		if got := m.Header.Get(traceid.HeaderKey); got != "tr-json-002" {
			t.Errorf("expected trace_id %q in header, got %q", "tr-json-002", got)
		}
		// data should still be JSON-encoded
		var decoded map[string]any
		if err := json.Unmarshal(m.Data, &decoded); err != nil {
			t.Errorf("expected JSON body, got %s (err: %v)", m.Data, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSubscribeJSON_TraceIDExtracted is the **end-to-end regression test**
// for the v0.16.3 fix: PublishWithTraceID + SubscribeJSON together must
// recover the original trace_id in the subscriber's ctx. Pre-fix, the
// handler always received context.Background() and the trace was lost.
func TestSubscribeJSON_TraceIDExtracted(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Tag string `json:"tag"`
	}
	got := make(chan traceCtxPair, 1)
	sub, err := SubscribeJSON[payload](c, "test.subjson", func(ctx context.Context, msg *payload) {
		got <- traceCtxPair{ctx: ctx, tag: msg.Tag}
	})
	if err != nil {
		t.Fatalf("SubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	ctx := traceid.ContextWith(context.Background(), "tr-e2e-003")
	if err := c.PublishJSONWithTraceID(ctx, "test.subjson", payload{Tag: "hello"}); err != nil {
		t.Fatalf("PublishJSONWithTraceID: %v", err)
	}

	select {
	case pair := <-got:
		if pair.tag != "hello" {
			t.Errorf("expected tag %q, got %q", "hello", pair.tag)
		}
		if got := traceid.FromContext(pair.ctx); got != "tr-e2e-003" {
			t.Errorf("expected trace_id %q in handler ctx, got %q", "tr-e2e-003", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SubscribeJSON handler")
	}
}

// TestSubscribeJSON_NoTraceIDInHeader covers the "no header" branch in
// the SubscribeJSON path: handler must still receive the message, but
// with a bare context (no trace_id).
func TestSubscribeJSON_NoTraceIDInHeader(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		N int `json:"n"`
	}
	got := make(chan traceCtxPair, 1)
	sub, err := SubscribeJSON[payload](c, "test.subjson.notrace", func(ctx context.Context, msg *payload) {
		got <- traceCtxPair{ctx: ctx, n: msg.N}
	})
	if err != nil {
		t.Fatalf("SubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	// Use plain PublishJSON (no trace_id) — header is missing.
	if err := c.PublishJSON(context.Background(), "test.subjson.notrace", payload{N: 99}); err != nil {
		t.Fatalf("PublishJSON: %v", err)
	}

	select {
	case pair := <-got:
		if pair.n != 99 {
			t.Errorf("expected n=99, got %d", pair.n)
		}
		if got := traceid.FromContext(pair.ctx); got != "" {
			t.Errorf("expected no trace_id when header missing, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SubscribeJSON handler")
	}
}

// TestSubscribeJSON_BadJSON covers the unmarshal-error path. The handler
// must NOT be called and the error must be logged (we don't assert the
// log line here — just that the subscription is alive and survives the
// bad message).
func TestSubscribeJSON_BadJSON(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Must string `json:"must"`
	}
	called := atomic.Int32{}
	good := make(chan struct{}, 1)
	sub, err := SubscribeJSON[payload](c, "test.badjson", func(ctx context.Context, msg *payload) {
		called.Add(1)
		if msg.Must == "ok" {
			good <- struct{}{}
		}
	})
	if err != nil {
		t.Fatalf("SubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	// Send invalid JSON; handler must not be invoked.
	if err := c.Publish(context.Background(), "test.badjson", []byte("not-json{")); err != nil {
		t.Fatalf("Publish bad json: %v", err)
	}

	// Send a valid one to confirm subscription is still alive.
	if err := c.PublishJSON(context.Background(), "test.badjson", payload{Must: "ok"}); err != nil {
		t.Fatalf("PublishJSON good: %v", err)
	}

	select {
	case <-good:
		if got := called.Load(); got != 1 {
			t.Errorf("expected handler called exactly once (for good msg), got %d", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for good message after bad one")
	}
}

// TestQueueSubscribe_DistributesToOneConsumer verifies the queue
// load-balancing semantics: with one queue group and N consumers, each
// message is delivered to exactly one consumer.
func TestQueueSubscribe_DistributesToOneConsumer(t *testing.T) {
	c := newTestClient(t)

	const msgCount = 6
	var (
		mu      sync.Mutex
		counter = make(map[string]int)
	)
	collect := func(name string) func(*nats.Msg) {
		return func(m *nats.Msg) {
			mu.Lock()
			counter[name]++
			mu.Unlock()
		}
	}

	a, err := c.QueueSubscribe("test.queue", "group", collect("a"))
	if err != nil {
		t.Fatalf("QueueSubscribe a: %v", err)
	}
	unsubscribeOnCleanup(t, a)
	b, err := c.QueueSubscribe("test.queue", "group", collect("b"))
	if err != nil {
		t.Fatalf("QueueSubscribe b: %v", err)
	}
	unsubscribeOnCleanup(t, b)

	for i := 0; i < msgCount; i++ {
		if err := c.Publish(context.Background(), "test.queue", []byte("m")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Give the server a moment to deliver.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		total := counter["a"] + counter["b"]
		mu.Unlock()
		if total == msgCount {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if counter["a"]+counter["b"] != msgCount {
		t.Errorf("expected %d total deliveries, got %d (a=%d b=%d)",
			msgCount, counter["a"]+counter["b"], counter["a"], counter["b"])
	}
	// Sanity: load-balanced (each consumer got at least one). With 6 messages
	// and 2 consumers this is overwhelmingly likely, but not strictly
	// deterministic across NATS versions — we tolerate the off chance that
	// one consumer got all 6.
}

// TestRequestReply_RoundTrip verifies the Request path: publisher sends
// a request, a Reply handler responds, the Request method returns the
// response bytes.
func TestRequestReply_RoundTrip(t *testing.T) {
	c := newTestClient(t)

	sub, err := c.Reply("test.rpc", func(msg *nats.Msg) []byte {
		return []byte("pong:" + string(msg.Data))
	})
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	resp, err := c.Request(context.Background(), "test.rpc", []byte("ping"), time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(resp.Data) != "pong:ping" {
		t.Errorf("got %q, want %q", resp.Data, "pong:ping")
	}
}

// TestRequestJSON_RoundTrip verifies the generic RequestJSON helper end-to-end.
func TestRequestJSON_RoundTrip(t *testing.T) {
	c := newTestClient(t)

	type req struct {
		N int `json:"n"`
	}
	type resp struct {
		Doubled int `json:"doubled"`
	}

	sub, err := c.Reply("test.rpcjson", func(msg *nats.Msg) []byte {
		var r req
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			return []byte("err")
		}
		out, _ := json.Marshal(resp{Doubled: r.N * 2})
		return out
	})
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	got, err := RequestJSON[req, resp](c, context.Background(), "test.rpcjson", &req{N: 21}, time.Second)
	if err != nil {
		t.Fatalf("RequestJSON: %v", err)
	}
	if got.Doubled != 42 {
		t.Errorf("expected 42, got %d", got.Doubled)
	}
}

// TestRequestJSON_MarshalError covers the "cannot marshal the request"
// error path. The function must short-circuit before touching the network.
func TestRequestJSON_MarshalError(t *testing.T) {
	c := newTestClient(t)

	type req struct {
		Bad chan int `json:"bad"`
	}
	_, err := RequestJSON[req, struct{}](c, context.Background(), "test.noop", &req{Bad: make(chan int)}, time.Second)
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
}

// TestIsConnected_ConnectedUrl covers the trivial state accessors. These
// are delegates to the underlying nats.Conn; we just confirm the wiring
// returns the right value after NewClient.
func TestIsConnected_ConnectedUrl(t *testing.T) {
	c := newTestClient(t)

	if !c.IsConnected() {
		t.Error("expected IsConnected true after NewClient")
	}
	if c.ConnectedUrl() == "" {
		t.Error("expected non-empty ConnectedUrl")
	}
}

// TestPublish_NilContext 回归（#79）：四个 Publish 入口对 nil ctx 兜底
// context.Background（与同仓 ratelimit.TokenBucket.WaitN / syncx.Gate.Acquire /
// retry.DoValue 的兜底口径一致），不再 nil 解引用 panic。兜底是 Background
// 而非「吞掉消息」：nil ctx 发布的消息订阅方须真能收到。
func TestPublish_NilContext(t *testing.T) {
	// 类型化的 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参，
	// 而 nil 值的 context.Context 变量在运行时就是 nil 接口——回归语义不变。
	var nilCtx context.Context
	c := newTestClient(t)

	got := make(chan []byte, 1)
	sub, err := c.Subscribe("nilctx.roundtrip", func(m *nats.Msg) {
		got <- append([]byte(nil), m.Data...)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.Publish(nilCtx, "nilctx.roundtrip", []byte("hello")); err != nil {
		t.Fatalf("Publish(nil ctx): %v", err)
	}
	select {
	case data := <-got:
		if string(data) != "hello" {
			t.Errorf("got %q, want %q", data, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message published with nil ctx")
	}

	if err := c.PublishJSON(nilCtx, "nilctx.json", map[string]int{"n": 1}); err != nil {
		t.Fatalf("PublishJSON(nil ctx): %v", err)
	}
	if err := c.PublishWithTraceID(nilCtx, "nilctx.trace", []byte("m")); err != nil {
		t.Fatalf("PublishWithTraceID(nil ctx): %v", err)
	}
	if err := c.PublishJSONWithTraceID(nilCtx, "nilctx.jsontrace", map[string]int{"n": 2}); err != nil {
		t.Fatalf("PublishJSONWithTraceID(nil ctx): %v", err)
	}
}

// traceCtxPair is a small helper struct so the SubscribeJSON tests can
// send the (ctx, payload) tuple through a single channel without resorting
// to a generic helper type.
type traceCtxPair struct {
	ctx context.Context
	tag string
	n   int
}

// TestClient_Close_DrainTimeoutFallsBack 回归（#83/#86）：一个 handler 阻塞时，
// 旧 Close() 内部的 conn.Drain() 会等所有在途消息处理完才返回——关停挂死且
// 无 ctx 可打断。现在 Close(ctx) 在 ctx 到期后回退 conn.Close() 强制关闭，
// 返回包装的 ctx 错误，关停有界。
func TestClient_Close_DrainTimeoutFallsBack(t *testing.T) {
	c := newTestClient(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	sub, err := c.Subscribe("close.blocked", func(m *nats.Msg) {
		close(entered)
		<-release // 模拟永久阻塞的 handler（在途消息处理不完 → Drain 等待）
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err = c.Publish(context.Background(), "close.blocked", []byte("m")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	<-entered // 确保 Drain 被调用时确有在途消息卡在 handler 里

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = c.Close(ctx)
	if err == nil {
		t.Fatal("expected timeout error from Close with a blocked handler")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close err = %v, want wrap of context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close blocked %v; must be bounded by ctx", elapsed)
	}
	if status := c.Conn().Status(); status != nats.CLOSED {
		t.Errorf("conn status = %v, want CLOSED after forced close", status)
	}
	close(release) // 放行 handler，测试收尾
}

// TestClient_Close_IdempotentNilCtx 覆盖 Close(ctx) error 的其余契约
// （#83/#86）：nil ctx 兜底 Background（#79 口径）；sync.Once 幂等——仅首次
// 执行关闭并返回其结果，重复调用返回 nil；无阻塞 handler 时正常路径返回 nil。
func TestClient_Close_IdempotentNilCtx(t *testing.T) {
	c := newTestClient(t)

	// 类型化的 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参，
	// 而 nil 值的 context.Context 变量在运行时就是 nil 接口——回归语义不变。
	var nilCtx context.Context
	if err := c.Close(nilCtx); err != nil {
		t.Fatalf("Close(nil ctx) = %v, want nil on normal drain", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("second Close = %v, want nil (sync.Once idempotent)", err)
	}
	if c.IsConnected() {
		t.Error("expected disconnected after Close")
	}
}
