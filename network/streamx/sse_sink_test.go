package streamx

import (
	"context"
	"strings"
	"testing"
)

// TestSSESink_IDPassThroughForJSONPayloads 钉住 2026-09-06 修复:带 ID 的
// JSON 载荷必须走 SendRaw 透传 ID(Last-Event-ID 断线续传依赖),
// 原实现走 Send 把 ID 丢掉。
func TestSSESink_IDPassThroughForJSONPayloads(t *testing.T) {
	var gotID, gotType string
	var gotData []byte
	sink := NewSSESinkFromFuncs(
		func(eventType string, data any) error {
			t.Errorf("Send should not be called for ID-carrying events, got type=%s", eventType)
			return nil
		},
		func(id, eventType string, data []byte) error {
			gotID, gotType, gotData = id, eventType, data
			return nil
		},
		func() error { return nil },
		func() error { return nil },
	)

	err := sink.Emit(context.Background(), Event{
		ID:   "seq-42",
		Type: "progress",
		Data: map[string]any{"percent": 50},
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if gotID != "seq-42" {
		t.Errorf("id = %q, want seq-42 (ID 透传)", gotID)
	}
	if gotType != "progress" {
		t.Errorf("type = %q, want progress", gotType)
	}
	if !strings.Contains(string(gotData), "percent") {
		t.Errorf("data = %s, want JSON-encoded payload", gotData)
	}
}
