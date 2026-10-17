package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oekit/logx"
	"github.com/oekit/tracing/traceid"
)

var log = logx.Delegating("nats")

// Client 是 NATS 客户端封装。
type Client struct {
	conn      *nats.Conn
	opts      options
	closeOnce sync.Once
}

// NatsOption 配置 Client 的可选参数。
type NatsOption func(*options)

type options struct {
	name          string
	reconnectWait time.Duration
	maxReconnects int
	onDisconnect  func(conn *nats.Conn, err error)
	onReconnect   func(conn *nats.Conn)
	onClose       func(conn *nats.Conn)
}

// WithName 设置客户端名称（用于 NATS 监控）。
func WithName(name string) NatsOption {
	return func(o *options) { o.name = name }
}

// WithReconnectWait 设置重连等待时间，默认 2 秒。
func WithReconnectWait(d time.Duration) NatsOption {
	return func(o *options) { o.reconnectWait = d }
}

// WithMaxReconnects 设置最大重连次数，-1 表示无限重连（默认）。
func WithMaxReconnects(n int) NatsOption {
	return func(o *options) { o.maxReconnects = n }
}

// WithDisconnectHandler 设置断开连接回调。
func WithDisconnectHandler(fn func(conn *nats.Conn, err error)) NatsOption {
	return func(o *options) { o.onDisconnect = fn }
}

// WithReconnectHandler 设置重连成功回调。
func WithReconnectHandler(fn func(conn *nats.Conn)) NatsOption {
	return func(o *options) { o.onReconnect = fn }
}

// WithCloseHandler 设置连接关闭回调。
func WithCloseHandler(fn func(conn *nats.Conn)) NatsOption {
	return func(o *options) { o.onClose = fn }
}

// NewClient 创建 NATS 客户端并连接。
//
//	urls 示例: "nats://localhost:4222" 或 "nats://host1:4222,nats://host2:4222"
func NewClient(urls string, opts ...NatsOption) (*Client, error) {
	o := options{
		reconnectWait: 2 * time.Second,
		maxReconnects: -1,
	}
	for _, opt := range opts {
		opt(&o)
	}

	natsOpts := []nats.Option{
		nats.ReconnectWait(o.reconnectWait),
		nats.MaxReconnects(o.maxReconnects),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			log.Warn("disconnected", "err", err)
			if o.onDisconnect != nil {
				o.onDisconnect(conn, err)
			}
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info("reconnected", "url", conn.ConnectedUrl())
			if o.onReconnect != nil {
				o.onReconnect(conn)
			}
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			log.Info("connection closed")
			if o.onClose != nil {
				o.onClose(conn)
			}
		}),
	}

	if o.name != "" {
		natsOpts = append(natsOpts, nats.Name(o.name))
	}

	conn, err := nats.Connect(urls, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	log.Info("connected", "url", conn.ConnectedUrl())
	return &Client{conn: conn, opts: o}, nil
}

// Conn 返回底层 nats.Conn（用于高级用法）。
func (c *Client) Conn() *nats.Conn {
	return c.conn
}

// IsConnected 返回是否已连接。
func (c *Client) IsConnected() bool {
	return c.conn.IsConnected()
}

// ConnectedUrl 返回当前连接的 URL。
func (c *Client) ConnectedUrl() string {
	return c.conn.ConnectedUrl()
}

// Drain 排空连接（等待所有消息处理完成后关闭）。
func (c *Client) Drain() error {
	return c.conn.Drain()
}

// Close 关闭连接（先排空订阅，再关闭）。
// 如果需要立即关闭（不等待消息处理完成），直接调用 c.conn.Close()。
// 多次调用是安全的（sync.Once 保证只执行一次）。
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		// Drain 会等待所有消息处理完成后自动关闭连接。
		// 如果 Drain 失败（如连接已断开），则直接关闭。
		if err := c.conn.Drain(); err != nil {
			c.conn.Close()
		}
	})
}

// Publish 发布消息到指定主题。
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	return c.conn.Publish(subject, data)
}

// PublishWithTraceID 发布消息并从 ctx 提取 trace_id 注入 NATS message header。
// 配合 SubscribeJSON 自动恢复 trace_id 形成跨消息链路的完整追踪。
// ctx 中无 trace_id 时退化为普通 Publish，行为向后兼容。
func (c *Client) PublishWithTraceID(ctx context.Context, subject string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	return c.publishMsg(ctx, subject, data)
}

// PublishJSON 发布 JSON 编码的消息。
func (c *Client) PublishJSON(ctx context.Context, subject string, v any) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats: marshal: %w", err)
	}
	return c.conn.Publish(subject, data)
}

// PublishJSONWithTraceID 发布 JSON 消息并从 ctx 提取 trace_id 注入 NATS message header。
// 配合 SubscribeJSON 自动恢复 trace_id 形成跨消息链路的完整追踪。
func (c *Client) PublishJSONWithTraceID(ctx context.Context, subject string, v any) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats: marshal: %w", err)
	}
	return c.publishMsg(ctx, subject, data)
}

// publishMsg 构造带 trace_id header 的 NATS 消息并发布。
// ctx 中无 trace_id 时退化为无 header 的 PublishMsg（与 Publish 等价）。
func (c *Client) publishMsg(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{Subject: subject, Data: data}
	if traceID := traceid.FromContext(ctx); traceID != "" {
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set(traceid.HeaderKey, traceID)
	}
	return c.conn.PublishMsg(msg)
}

// Subscribe 订阅主题，收到消息时调用 handler。
// 返回 Subscription，调用 Unsubscribe() 取消订阅。
func (c *Client) Subscribe(subject string, handler func(*nats.Msg)) (*nats.Subscription, error) {
	sub, err := c.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, fmt.Errorf("nats subscribe %s: %w", subject, err)
	}
	log.Info("subscribed", "subject", subject)
	return sub, nil
}

// QueueSubscribe 订阅主题（队列模式），同一队列组内只有一个消费者收到消息。
// 适用于负载均衡场景：多个服务实例订阅同一主题，消息只投递给其中一个。
func (c *Client) QueueSubscribe(subject, queue string, handler func(*nats.Msg)) (*nats.Subscription, error) {
	sub, err := c.conn.QueueSubscribe(subject, queue, handler)
	if err != nil {
		return nil, fmt.Errorf("nats queue subscribe %s/%s: %w", subject, queue, err)
	}
	log.Info("queue subscribed", "subject", subject, "group", queue)
	return sub, nil
}

// SubscribeJSON 订阅主题并自动反序列化 JSON 消息。
//
// 从 NATS message header 中提取 trace_id（如果存在）并注入到 handler 的 ctx，
// 保持分布式追踪在异步消息边界不断裂。Publisher 端需要在 Publish 时通过
// PublishWithTraceID 注入 trace_id 才能形成完整链路。
func SubscribeJSON[T any](c *Client, subject string, handler func(ctx context.Context, msg *T)) (*nats.Subscription, error) {
	return c.Subscribe(subject, func(m *nats.Msg) {
		var payload T
		if err := json.Unmarshal(m.Data, &payload); err != nil {
			log.Error("unmarshal error", "subject", subject, "err", err)
			return
		}
		handler(extractTraceCtx(m), &payload)
	})
}

// QueueSubscribeJSON 订阅主题（队列模式）并自动反序列化 JSON 消息。
//
// 同 SubscribeJSON，从 NATS message header 中提取 trace_id 注入 ctx。
func QueueSubscribeJSON[T any](c *Client, subject, queue string, handler func(ctx context.Context, msg *T)) (*nats.Subscription, error) {
	return c.QueueSubscribe(subject, queue, func(m *nats.Msg) {
		var payload T
		if err := json.Unmarshal(m.Data, &payload); err != nil {
			log.Error("unmarshal error", "subject", subject, "err", err)
			return
		}
		handler(extractTraceCtx(m), &payload)
	})
}

// extractTraceCtx 从 NATS message header 中提取 trace_id 并构造带 trace_id 的 context。
// header 缺失或 trace_id 为空时返回 context.Background()，保持向后兼容。
func extractTraceCtx(m *nats.Msg) context.Context {
	if m == nil || m.Header == nil {
		return context.Background()
	}
	traceID := m.Header.Get(traceid.HeaderKey)
	if traceID == "" {
		return context.Background()
	}
	return traceid.ContextWith(context.Background(), traceID)
}

// Request 发送请求并等待回复。
// 使用 context 控制超时和取消。
func (c *Client) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := c.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("nats request %s: %w", subject, err)
	}
	return resp, nil
}

// RequestJSON 发送 JSON 请求并反序列化响应。
// 使用 context 控制超时和取消。
func RequestJSON[Req, Resp any](c *Client, ctx context.Context, subject string, req *Req, timeout time.Duration) (*Resp, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("nats: marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := c.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("nats request %s: %w", subject, err)
	}

	var result Resp
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("nats: unmarshal response: %w", err)
	}
	return &result, nil
}

// Reply 订阅主题并自动回复（用于实现 RPC 服务端）。
// handler 的返回值会作为回复消息发送。
func (c *Client) Reply(subject string, handler func(msg *nats.Msg) []byte) (*nats.Subscription, error) {
	return c.Subscribe(subject, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		reply := handler(msg)
		if err := msg.Respond(reply); err != nil {
			log.Error("respond error", "subject", subject, "err", err)
		}
	})
}
