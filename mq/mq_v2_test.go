package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// —— v2 接口级契约测试（#251） ——
//
// 无真实 broker：fake 实现把「消息处置契约」写进 deliver——显式
// Ack/Nak/Term 优先，未显式处置时按 handler 返回值自动 Ack/Nak，自动处置
// 失败随返回值上抛。测试验证接口本身能把 at-least-once 语义表达完整，
// 而不是只断言方法存在。

type fakeDisposition int

const (
	dispositionNone fakeDisposition = iota
	dispositionAck
	dispositionNak
	dispositionTerm
)

// fakeMessage 是 Message 的测试实现，记录处置动作与返回的错误。
type fakeMessage struct {
	subject        string
	data           []byte
	headers        Header
	deadline       time.Time
	ackUnsupported bool

	mu          sync.Mutex
	disposition fakeDisposition
}

func (m *fakeMessage) Subject() string        { return m.subject }
func (m *fakeMessage) Data() []byte           { return m.data }
func (m *fakeMessage) Headers() Header        { return m.headers }
func (m *fakeMessage) AckDeadline() time.Time { return m.deadline }

func (m *fakeMessage) Ack() error  { return m.record(dispositionAck) }
func (m *fakeMessage) Nak() error  { return m.record(dispositionNak) }
func (m *fakeMessage) Term() error { return m.record(dispositionTerm) }

func (m *fakeMessage) record(d fakeDisposition) error {
	if m.ackUnsupported {
		return ErrAckUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.disposition != dispositionNone {
		return errors.New("mq fake: 消息已处置，重复确认被拒绝")
	}
	m.disposition = d
	return nil
}

func (m *fakeMessage) gotDisposition() fakeDisposition {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disposition
}

// fakeSubscriberV2 是 SubscriberV2 的测试实现：登记 handler 并支持手动
// deliver（测试驱动一条消息的完整处置路径）。
type fakeSubscriberV2 struct {
	mu             sync.Mutex
	handler        MessageHandlerV2
	subject        string
	queue          string
	subCtx         context.Context
	unsubscribed   bool
	unsubscribeCtx context.Context
	subscribeErr   error
}

func (s *fakeSubscriberV2) Subscribe(ctx context.Context, subject string, handler MessageHandlerV2) (SubscriptionV2, error) {
	return s.subscribe(ctx, subject, "", handler)
}

func (s *fakeSubscriberV2) QueueSubscribe(ctx context.Context, subject, queue string, handler MessageHandlerV2) (SubscriptionV2, error) {
	return s.subscribe(ctx, subject, queue, handler)
}

func (s *fakeSubscriberV2) subscribe(ctx context.Context, subject, queue string, handler MessageHandlerV2) (SubscriptionV2, error) {
	if s.subscribeErr != nil {
		return nil, s.subscribeErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subject, s.queue, s.handler, s.subCtx = subject, queue, handler, ctx
	return &fakeSubscriptionV2{sub: s}, nil
}

// deliver 按「消息处置契约」驱动一条消息：
//  1. 订阅已取消或订阅 ctx 已取消 ⇒ 不调用 handler；
//  2. handler 显式处置优先，返回值不改变 fate；
//  3. 未显式处置 ⇒ nil 自动 Ack、非 nil 自动 Nak；
//  4. 自动处置错误与 handler 错误一并上抛（不静默吞）。
func (s *fakeSubscriberV2) deliver(ctx context.Context, msg Message) error {
	s.mu.Lock()
	handler := s.handler
	subCtx := s.subCtx
	unsubscribed := s.unsubscribed
	s.mu.Unlock()

	if handler == nil {
		return errors.New("mq fake: 未订阅")
	}
	if unsubscribed {
		return errors.New("mq fake: 订阅已取消")
	}
	if err := subCtx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	handlerErr := handler(ctx, msg)
	if fm, ok := msg.(*fakeMessage); ok && fm.gotDisposition() != dispositionNone {
		return handlerErr // 显式处置优先：返回值只作观测
	}

	var disposeErr error
	if handlerErr != nil {
		disposeErr = msg.Nak()
	} else {
		disposeErr = msg.Ack()
	}
	return errors.Join(handlerErr, disposeErr)
}

func (s *fakeSubscriberV2) markUnsubscribed(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsubscribed = true
	s.unsubscribeCtx = ctx
}

func (s *fakeSubscriberV2) gotUnsubscribeCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unsubscribeCtx
}

type fakeSubscriptionV2 struct {
	sub *fakeSubscriberV2
}

func (s *fakeSubscriptionV2) Unsubscribe(ctx context.Context) error {
	s.sub.markUnsubscribed(ctx)
	return nil // 幂等：重复调用同样返回 nil
}

// fakeMQClientV2 证明发布/请求/订阅/关停四半可组合为一个客户端契约。
type fakeMQClientV2 struct {
	fakeSubscriberV2
	mockPublisher
	mockRequester
}

func (c *fakeMQClientV2) Close(context.Context) error { return nil }

// 编译期钉子：v2 契约的全部角色都可被实现（接口自洽、无隐藏方法）。
var (
	_ Message        = (*fakeMessage)(nil)
	_ SubscriberV2   = (*fakeSubscriberV2)(nil)
	_ SubscriptionV2 = (*fakeSubscriptionV2)(nil)
	_ MQClientV2     = (*fakeMQClientV2)(nil)
)

// newFakeV2 构造订阅方与一条消息；订阅成功即返回。
func newFakeV2(t *testing.T, handler MessageHandlerV2) (*fakeSubscriberV2, *fakeMessage) {
	t.Helper()
	s := &fakeSubscriberV2{}
	if _, err := s.Subscribe(context.Background(), "orders.created", handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return s, &fakeMessage{subject: "orders.created", data: []byte(`{"id":1}`)}
}

// TestSubscriberV2_HandlerNilAutoAck 成功处理（未显式处置）⇒ 自动 Ack。
func TestSubscriberV2_HandlerNilAutoAck(t *testing.T) {
	s, msg := newFakeV2(t, func(context.Context, Message) error { return nil })
	if err := s.deliver(context.Background(), msg); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := msg.gotDisposition(); got != dispositionAck {
		t.Fatalf("handler 返回 nil 应自动 Ack，got disposition=%v", got)
	}
}

// TestSubscriberV2_HandlerErrorAutoNak 失败处理（未显式处置）⇒ 自动 Nak
// （至少一次语义的重投路径）。
func TestSubscriberV2_HandlerErrorAutoNak(t *testing.T) {
	hErr := errors.New("下游不可用")
	s, msg := newFakeV2(t, func(context.Context, Message) error { return hErr })
	err := s.deliver(context.Background(), msg)
	if !errors.Is(err, hErr) {
		t.Fatalf("deliver 应保留 handler 错误，got %v", err)
	}
	if got := msg.gotDisposition(); got != dispositionNak {
		t.Fatalf("handler 返回 error 应自动 Nak，got disposition=%v", got)
	}
}

// TestSubscriberV2_ExplicitDispositionWins 显式处置优先于 handler 返回值：
// 显式 Ack 后即使返回 error 也不再 Nak；显式 Term 后即使返回 error 也不
// 重投（毒丸终止）。
func TestSubscriberV2_ExplicitDispositionWins(t *testing.T) {
	cases := []struct {
		name string
		fn   func(Message) error
		want fakeDisposition
	}{
		{"显式 Ack 后返回 error", func(m Message) error { return m.Ack() }, dispositionAck},
		{"显式 Nak 后返回 nil", func(m Message) error { return m.Nak() }, dispositionNak},
		{"显式 Term 后返回 error", func(m Message) error { return m.Term() }, dispositionTerm},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hErr := errors.New("handler 后续失败")
			s, msg := newFakeV2(t, func(_ context.Context, m Message) error {
				if err := c.fn(m); err != nil {
					t.Fatalf("显式处置: %v", err)
				}
				return hErr
			})
			err := s.deliver(context.Background(), msg)
			if !errors.Is(err, hErr) {
				t.Fatalf("显式处置后 handler 错误仍应可观测，got %v", err)
			}
			if got := msg.gotDisposition(); got != c.want {
				t.Fatalf("显式处置应优先，got disposition=%v, want %v", got, c.want)
			}
		})
	}
}

// TestMessageV2_HeadersAndAckDeadline headers 与 ack deadline 在接口层可
// 完整表达（JetStream 的 header + AckWait 窗口映射点）。
func TestMessageV2_HeadersAndAckDeadline(t *testing.T) {
	deadline := time.Now().Add(30 * time.Second)
	msg := &fakeMessage{
		subject:  "orders.created",
		data:     []byte("payload"),
		headers:  Header{"trace_id": {"abc"}, "retry": {"1", "2"}},
		deadline: deadline,
	}
	if got := msg.Subject(); got != "orders.created" {
		t.Errorf("Subject()=%q", got)
	}
	if got := string(msg.Data()); got != "payload" {
		t.Errorf("Data()=%q", got)
	}
	if got := msg.Headers().Get("trace_id"); got != "abc" {
		t.Errorf("Header.Get(trace_id)=%q, want abc", got)
	}
	if got := msg.Headers().Get("missing"); got != "" {
		t.Errorf("Header.Get(missing)=%q, want 空串", got)
	}
	if got := msg.AckDeadline(); !got.Equal(deadline) {
		t.Errorf("AckDeadline()=%v, want %v", got, deadline)
	}
	var nilHeader Header
	if got := nilHeader.Get("any"); got != "" {
		t.Errorf("nil Header.Get=%q, want 空串", got)
	}
}

// TestMessageV2_AckUnsupportedNotSilent 后端无法确认时必须返回
// ErrAckUnsupported，且自动处置失败随 deliver 返回——不静默伪装成功
// （#251：不可表达 ≠ 已确认）。
func TestMessageV2_AckUnsupportedNotSilent(t *testing.T) {
	s, msg := newFakeV2(t, func(context.Context, Message) error { return nil })
	msg.ackUnsupported = true

	if err := msg.Ack(); !errors.Is(err, ErrAckUnsupported) {
		t.Fatalf("Ack 应返回 ErrAckUnsupported，got %v", err)
	}
	if err := msg.Nak(); !errors.Is(err, ErrAckUnsupported) {
		t.Fatalf("Nak 应返回 ErrAckUnsupported，got %v", err)
	}
	if err := msg.Term(); !errors.Is(err, ErrAckUnsupported) {
		t.Fatalf("Term 应返回 ErrAckUnsupported，got %v", err)
	}
	// 自动 Ack 失败不得静默：deliver 上抛。
	if err := s.deliver(context.Background(), msg); !errors.Is(err, ErrAckUnsupported) {
		t.Fatalf("自动处置失败应经 deliver 上抛，got %v", err)
	}
	if got := msg.gotDisposition(); got != dispositionNone {
		t.Fatalf("不支持的处置不得记为已确认，got disposition=%v", got)
	}
}

// TestSubscriberV2_QueueSubscribe 队列组语义可表达（同组负载均衡）。
func TestSubscriberV2_QueueSubscribe(t *testing.T) {
	s := &fakeSubscriberV2{}
	if _, err := s.QueueSubscribe(context.Background(), "orders.created", "workers", func(context.Context, Message) error {
		return nil
	}); err != nil {
		t.Fatalf("QueueSubscribe: %v", err)
	}
	if s.queue != "workers" {
		t.Fatalf("queue=%q, want workers", s.queue)
	}
}

// TestSubscriberV2_SubscribeErrorPropagates 订阅建立失败经返回值上抛。
func TestSubscriberV2_SubscribeErrorPropagates(t *testing.T) {
	wantErr := errors.New("broker 不可达")
	s := &fakeSubscriberV2{subscribeErr: wantErr}
	if _, err := s.Subscribe(context.Background(), "orders", func(context.Context, Message) error {
		return nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Subscribe err=%v, want %v", err, wantErr)
	}
}

// TestSubscriberV2_ContextCancellationStopsDelivery Subscribe 的 ctx 界
// 订阅生命周期：取消后不再调用 handler（v1 无法表达，正是 #251 的缺口）。
func TestSubscriberV2_ContextCancellationStopsDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	called := false
	s := &fakeSubscriberV2{}
	if _, err := s.Subscribe(ctx, "orders", func(context.Context, Message) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	msg := &fakeMessage{subject: "orders"}
	if err := s.deliver(context.Background(), msg); !errors.Is(err, context.Canceled) {
		t.Fatalf("订阅 ctx 取消后 deliver=%v, want context.Canceled", err)
	}
	if called {
		t.Fatal("订阅取消后不得再调用 handler")
	}
	if got := msg.gotDisposition(); got != dispositionNone {
		t.Fatalf("未投递的消息不得被处置，got %v", got)
	}
}

// TestSubscriptionV2_UnsubscribeBoundedAndIdempotent Unsubscribe(ctx) 透传
// 关停上界 ctx；重复调用幂等（返回 nil），取消后不再投递。
func TestSubscriptionV2_UnsubscribeBoundedAndIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &fakeSubscriberV2{}
	sub, err := s.Subscribe(context.Background(), "orders", func(context.Context, Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if got := s.gotUnsubscribeCtx(); got != ctx {
		t.Errorf("Unsubscribe ctx 未透传：got %v, want %v", got, ctx)
	}
	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatalf("重复 Unsubscribe 应幂等返回 nil，got %v", err)
	}
	if err := s.deliver(context.Background(), &fakeMessage{}); err == nil {
		t.Fatal("取消订阅后不得再投递")
	}
}

// TestSubscriberV2_HandlerReceivesDeliveryCtx handler 收到的是投递 ctx
// （实现可在其上加 ack deadline/关停信号）。
func TestSubscriberV2_HandlerReceivesDeliveryCtx(t *testing.T) {
	type ctxKey struct{}
	s, msg := newFakeV2(t, func(ctx context.Context, _ Message) error {
		if v, _ := ctx.Value(ctxKey{}).(string); v != "delivery" {
			t.Errorf("handler ctx value=%q, want delivery", v)
		}
		return nil
	})
	ctx := context.WithValue(context.Background(), ctxKey{}, "delivery")
	if err := s.deliver(ctx, msg); err != nil {
		t.Fatalf("deliver: %v", err)
	}
}

// TestMQClientV2_ComposesAllHalves 客户端契约四半齐全：发布/请求沿用 v1，
// 订阅走 v2，Close 带 ctx 上界。
func TestMQClientV2_ComposesAllHalves(t *testing.T) {
	var c MQClientV2 = &fakeMQClientV2{}
	if err := c.Publish(context.Background(), "orders", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := c.Request(context.Background(), "rpc", nil, time.Second); err != nil {
		t.Fatalf("Request: %v", err)
	}
	if _, err := c.Subscribe(context.Background(), "orders", func(context.Context, Message) error {
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
