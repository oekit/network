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

type mockSubscription struct {
	unsubscribed bool
}

func (m *mockSubscription) Unsubscribe() error {
	m.unsubscribed = true
	return nil
}

type mockSubscriber struct {
	err error
}

func (m *mockSubscriber) Subscribe(_ string, _ MessageHandler) (Subscription, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &mockSubscription{}, nil
}

func (m *mockSubscriber) QueueSubscribe(_, _ string, _ MessageHandler) (Subscription, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &mockSubscription{}, nil
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

type mockMQClient struct {
	mockPublisher
	mockSubscriber
	mockRequester
	closed bool
}

func (m *mockMQClient) Close() error {
	m.closed = true
	return nil
}

// --- Interface compliance tests ---

func TestMockPublisher_ImplementsPublisher(t *testing.T) {
	var _ Publisher = &mockPublisher{}
}

func TestMockSubscriber_ImplementsSubscriber(t *testing.T) {
	var _ Subscriber = &mockSubscriber{}
}

func TestMockRequester_ImplementsRequester(t *testing.T) {
	var _ Requester = &mockRequester{}
}

func TestMockMQClient_ImplementsMQClient(t *testing.T) {
	var _ MQClient = &mockMQClient{}
}

func TestMockSubscription_ImplementsSubscription(t *testing.T) {
	var _ Subscription = &mockSubscription{}
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

// --- Subscriber behavior tests ---

func TestSubscriber_Subscribe(t *testing.T) {
	s := &mockSubscriber{}
	sub, err := s.Subscribe("test.subject", func(_ context.Context, _ []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}
}

func TestSubscriber_QueueSubscribe(t *testing.T) {
	s := &mockSubscriber{}
	sub, err := s.QueueSubscribe("test.subject", "group-1", func(_ context.Context, _ []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}
}

func TestSubscription_Unsubscribe(t *testing.T) {
	sub := &mockSubscription{}
	err := sub.Unsubscribe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sub.unsubscribed {
		t.Error("expected unsubscribed to be true")
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

// --- MQClient behavior tests ---

func TestMQClient_Close(t *testing.T) {
	c := &mockMQClient{}
	err := c.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.closed {
		t.Error("expected closed to be true")
	}
}

func TestMQClient_FullWorkflow(t *testing.T) {
	c := &mockMQClient{}

	// Publish
	err := c.Publish(context.Background(), "order.created", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Subscribe
	sub, err := c.Subscribe("order.created", func(_ context.Context, msg []byte) {})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Request
	resp, err := c.Request(context.Background(), "order.get", []byte(`{"id":1}`), 5*time.Second)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	if string(resp) != "response" {
		t.Errorf("expected 'response', got %q", string(resp))
	}

	// Unsubscribe
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe error: %v", err)
	}

	// Close
	if err := c.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}
