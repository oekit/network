package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/tracing/traceid"
)

// TestQueueSubscribeJSON_TraceIDExtracted covers the QueueSubscribeJSON path
// (currently 0% coverage). Mirrors TestSubscribeJSON_TraceIDExtracted but
// uses the queue mode. Queue mode + trace_id must work the same way
// (extraction is independent of subscription mode).
func TestQueueSubscribeJSON_TraceIDExtracted(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Tag string `json:"tag"`
	}
	got := make(chan traceCtxPair, 1)
	sub, err := QueueSubscribeJSON[payload](c, "test.qsubjson", "group-1", func(ctx context.Context, msg *payload) {
		got <- traceCtxPair{ctx: ctx, tag: msg.Tag}
	})
	if err != nil {
		t.Fatalf("QueueSubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	ctx := traceid.ContextWith(context.Background(), "tr-qsub-004")
	if err := c.PublishJSONWithTraceID(ctx, "test.qsubjson", payload{Tag: "q-msg"}); err != nil {
		t.Fatalf("PublishJSONWithTraceID: %v", err)
	}

	select {
	case pair := <-got:
		if pair.tag != "q-msg" {
			t.Errorf("expected tag %q, got %q", "q-msg", pair.tag)
		}
		if got := traceid.FromContext(pair.ctx); got != "tr-qsub-004" {
			t.Errorf("expected trace_id %q in handler ctx, got %q", "tr-qsub-004", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for QueueSubscribeJSON handler")
	}
}

// TestQueueSubscribeJSON_BadJSON mirrors TestSubscribeJSON_BadJSON for the
// queue mode. The handler must NOT be invoked for an unparseable body.
func TestQueueSubscribeJSON_BadJSON(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Must string `json:"must"`
	}
	called := atomic.Int32{}
	good := make(chan struct{}, 1)
	sub, err := QueueSubscribeJSON[payload](c, "test.qsubjson.bad", "group-2", func(ctx context.Context, msg *payload) {
		called.Add(1)
		if msg.Must == "ok" {
			good <- struct{}{}
		}
	})
	if err != nil {
		t.Fatalf("QueueSubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.Publish(context.Background(), "test.qsubjson.bad", []byte("not-json{")); err != nil {
		t.Fatalf("Publish bad: %v", err)
	}
	if err := c.PublishJSON(context.Background(), "test.qsubjson.bad", payload{Must: "ok"}); err != nil {
		t.Fatalf("PublishJSON good: %v", err)
	}

	select {
	case <-good:
		if got := called.Load(); got != 1 {
			t.Errorf("expected handler called once (for good msg), got %d", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for good message after bad one")
	}
}

// guard against future import drift kept clean — no unused symbols.
