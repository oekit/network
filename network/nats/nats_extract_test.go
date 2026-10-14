package nats

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/yi-nology/ekit-tracing/traceid"
)

// TestExtractTraceCtx_NilMsg verifies that a nil *nats.Msg yields a bare context.
// Defends against a regression where panicking on nil deref masked the
// "no header" case in real message flow.
func TestExtractTraceCtx_NilMsg(t *testing.T) {
	ctx := extractTraceCtx(nil)
	if got := traceid.FromContext(ctx); got != "" {
		t.Errorf("expected empty trace_id from nil msg, got %q", got)
	}
}

// TestExtractTraceCtx_NoHeader verifies that a message with a nil Header
// yields a bare context (no trace_id injected).
func TestExtractTraceCtx_NoHeader(t *testing.T) {
	m := &nats.Msg{Subject: "foo", Data: []byte("payload")}
	ctx := extractTraceCtx(m)
	if got := traceid.FromContext(ctx); got != "" {
		t.Errorf("expected empty trace_id when header is nil, got %q", got)
	}
}

// TestExtractTraceCtx_HeaderPresentEmpty verifies the "header set but value
// empty" edge case. NATS header semantics treat a missing key and an empty
// value identically; we must not inject an empty trace_id (it would silently
// break downstream lookup).
func TestExtractTraceCtx_HeaderPresentEmpty(t *testing.T) {
	m := &nats.Msg{
		Subject: "foo",
		Header:  nats.Header{traceid.HeaderKey: []string{""}},
	}
	ctx := extractTraceCtx(m)
	if got := traceid.FromContext(ctx); got != "" {
		t.Errorf("expected empty trace_id when header value is empty, got %q", got)
	}
}

// TestExtractTraceCtx_HeaderPresent verifies the happy path: a header
// carrying a non-empty trace_id is injected into the context.
func TestExtractTraceCtx_HeaderPresent(t *testing.T) {
	m := &nats.Msg{
		Subject: "foo",
		Header:  nats.Header{traceid.HeaderKey: []string{"abc123"}},
	}
	ctx := extractTraceCtx(m)
	if got := traceid.FromContext(ctx); got != "abc123" {
		t.Errorf("expected trace_id %q, got %q", "abc123", got)
	}
}

// TestExtractTraceCtx_NotContextWithBackground is a defensive guard:
// extractTraceCtx must never return nil. Callers unwrap with `.Err()` etc.
func TestExtractTraceCtx_NotNilContext(t *testing.T) {
	ctx := extractTraceCtx(nil)
	if ctx == nil {
		t.Fatal("extractTraceCtx must never return nil context")
	}
	if ctx == (context.Context)(nil) {
		t.Fatal("extractTraceCtx must never return zero-value context")
	}
}
