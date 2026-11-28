package mq

import (
	"context"
	"testing"
	"time"
)

type mockPublisher struct {
	published []mockMessage
	err       error
}

type mockMessage struct {
	subject string
	data    []byte
}

func (m *mockPublisher) Publish(_ context.Context, subject string, data []byte) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, mockMessage{subject: subject, data: data})
	return nil
}

func (m *mockPublisher) PublishJSON(_ context.Context, subject string, _ any) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, mockMessage{subject: subject})
	return nil
}

type mockRequester struct {
	err error
}

func (m *mockRequester) Request(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []byte("response"), nil
}

func (m *mockRequester) RequestJSON(_ context.Context, _ string, _ any, _ any, _ time.Duration) error {
	return m.err
}

// --- Interface compliance tests ---

func TestMockPublisher_ImplementsPublisher(t *testing.T) {
	var _ Publisher = &mockPublisher{}
}

func TestMockRequester_ImplementsRequester(t *testing.T) {
	var _ Requester = &mockRequester{}
}

// --- Publisher behavior tests ---

func TestPublisher_Publish(t *testing.T) {
	p := &mockPublisher{}
	err := p.Publish(context.Background(), "test.subject", []byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.published) != 1 {
		t.Fatalf("expected 1 message, got %d", len(p.published))
	}
	if p.published[0].subject != "test.subject" {
		t.Errorf("expected subject 'test.subject', got %q", p.published[0].subject)
	}
}

func TestPublisher_PublishJSON(t *testing.T) {
	p := &mockPublisher{}
	err := p.PublishJSON(context.Background(), "test.json", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.published) != 1 {
		t.Fatalf("expected 1 message, got %d", len(p.published))
	}
}

func TestPublisher_PublishError(t *testing.T) {
	p := &mockPublisher{err: context.DeadlineExceeded}
	err := p.Publish(context.Background(), "test", []byte("data"))
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Requester behavior tests ---

func TestRequester_Request(t *testing.T) {
	r := &mockRequester{}
	resp, err := r.Request(context.Background(), "test.rpc", []byte("req"), 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resp) != "response" {
		t.Errorf("expected 'response', got %q", string(resp))
	}
}

func TestRequester_RequestJSON(t *testing.T) {
	r := &mockRequester{}
	err := r.RequestJSON(context.Background(), "test.rpc", nil, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- #143 回归：客户端 Close 必须带 ctx 上界入口（契约现由 MQClientV2 承载） ---

// ctxCloseClient 是「合规实现者」的形态（#143）：Close 带 ctx，可为关停
// 设上界（Drain/flush 遇慢消费者时按 ctx 截止回退强制关闭）。
//
// 修复前门面把无 ctx 的 Close() error 写死进契约，此形态**编译不过**——
// 正是 issue 的失败场景：合规实现者被迫 fork 或放弃门面，门面反而约束出
// 无界关停（#83/#86 事故形态）。v1 门面已删除，钉子落在 MQClientV2。
type ctxCloseClient struct {
	mockPublisher
	mockRequester
	fakeSubscriberV2
	gotCtx context.Context
}

func (c *ctxCloseClient) Close(ctx context.Context) error {
	c.gotCtx = ctx
	return nil
}

// TestMQClientV2_AcceptsContextBoundClose 钉住 #143：带 ctx 的 Close 能满足
// MQClientV2（修复前编译失败），且 ctx 透传至关停动作（上界入口不能丢）。
func TestMQClientV2_AcceptsContextBoundClose(t *testing.T) {
	impl := &ctxCloseClient{}
	var c MQClientV2 = impl
	want, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Close(want); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := impl.gotCtx; got != want {
		t.Errorf("Close ctx = %v, want %v（ctx 须原样透传，关停上界入口不能丢）", got, want)
	}
}
