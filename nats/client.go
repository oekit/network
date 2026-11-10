package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oekit/logx"
	"github.com/oekit/tracing/traceid"
)

var log = logx.Delegating("nats")

// ensureCtx 兜底 nil ctx 为 context.Background（#79/#101 同仓口径：nil ctx
// 是常见形态，Background（无截止、永不取消）即「调用方没有额外约束」，
// 而不是把 nil 透传给标准库/上游 SDK 换一个 panic）。
func ensureCtx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// Client 是 NATS 客户端封装。
type Client struct {
	conn      *nats.Conn
	opts      options
	closeOnce sync.Once
	// closedc 在连接进入 CLOSED 时关闭（由 ClosedHandler 触发）。nats.go 的
	// Drain() 是异步的——立即返回，排空在内部 goroutine 里进行、完成后才
	// 关闭连接；Close(ctx) 靠它等「真正排空完成」，而不是 Drain() 的返回。
	closedc chan struct{}
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

	// 先构造 Client 骨架：ClosedHandler 要引用 closedc（Close 等待排空
	// 完成的信号），而 handler 必须在 nats.Connect 之前装配好。
	c := &Client{
		opts:    o,
		closedc: make(chan struct{}),
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
			close(c.closedc)
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
	c.conn = conn

	log.Info("connected", "url", conn.ConnectedUrl())
	return c, nil
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

// Close 关闭连接：发起 Drain 并等待「真正排空完成、连接进入 CLOSED」——
// nats.go 的 Drain() 是异步的（立即返回，排空在内部 goroutine 里等所有订阅
// 的在途消息处理完），一个 handler 阻塞时排空会挂到库的 DrainTimeout（默认
// 30s）甚至更久（#83/#86），关停随之无界。现在 ctx 到期仍未排空就放弃等待、
// 回退 conn.Close() 强制关闭（连接关闭会让阻塞中的读/写尽快以错误收敛），
// 关停有界。Drain 被拒绝启动、而连接已被强制关闭（网络侧关闭、或重连窗口里
// 上游 Drain 自己关掉的）视为已达目标态，不算错误（#147）。
//
// 签名遵循矩阵硬规则「组件 Close 统一 Close(ctx) error + sync.Once 幂等」：
// 仅首次调用执行关闭动作并返回其结果，后续调用直接返回 nil。
// nil ctx 按 context.Background() 处理（#79 口径）。
//
// ⚠️ BREAKING（0.x 允许）：原无参 Close() 移除——Go 不支持同名方法重载，
// 无法按 issue #83 的设想保留无参兼容别名；调用点改为
// c.Close(context.Background())。需要不等待排空的立即关闭，用 c.Conn().Close()。
func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var closeErr error
	c.closeOnce.Do(func() {
		if err := c.conn.Drain(); err != nil {
			// Drain 拒绝启动：回退强制关闭，确保连接被释放。以下三种错误都是
			// 「上游已把连接强制关闭、目标态已达成」，按本方法口径不算错误：
			//   - ErrConnectionClosed：连接已被网络侧关闭；
			//   - ErrConnectionReconnecting：重连窗口里 Drain() 自己就调了
			//     nc.Close() 再返回它（nats.go@v1.55.0/nats.go:6507-6510）——
			//     连接已强制关闭、正是关停想要的结果，旧豁免只认
			//     ErrConnectionClosed，把它误记成「drain: connection
			//     reconnecting」虚假错误（#147）；
			//   - ErrConnectionDraining：已在排空中（其它版本/竞态下幂等
			//     Drain 的返回语义不同），目标态已在路上，防御性同列。
			c.conn.Close()
			if !errors.Is(err, nats.ErrConnectionClosed) &&
				!errors.Is(err, nats.ErrConnectionReconnecting) &&
				!errors.Is(err, nats.ErrConnectionDraining) {
				closeErr = fmt.Errorf("nats: drain: %w", err)
			}
			return
		}
		select {
		case <-c.closedc:
			// 排空完成（或连接已被网络侧关闭）：连接已进入 CLOSED
		case <-ctx.Done():
			// 排空超时：不再等在途 handler，强制关闭兜底，关停有界
			c.conn.Close()
			closeErr = fmt.Errorf("nats: close: drain timeout, forced close: %w", ctx.Err())
		}
	})
	return closeErr
}

// Publish 发布消息到指定主题。
// nil ctx 按 context.Background 处理（#79，与同仓 WaitN/Acquire/DoValue
// 的兜底口径一致）。
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	return c.conn.Publish(subject, data)
}

// PublishWithTraceID 发布消息并从 ctx 提取 trace_id 注入 NATS message header。
// 配合 SubscribeJSON 自动恢复 trace_id 形成跨消息链路的完整追踪。
// ctx 中无 trace_id 时退化为普通 Publish，行为向后兼容。
// nil ctx 按 context.Background 处理（#79）。
func (c *Client) PublishWithTraceID(ctx context.Context, subject string, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: publish cancelled: %w", err)
	}
	return c.publishMsg(ctx, subject, data)
}

// PublishJSON 发布 JSON 编码的消息。
// nil ctx 按 context.Background 处理（#79）。
func (c *Client) PublishJSON(ctx context.Context, subject string, v any) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
// nil ctx 按 context.Background 处理（#79）。
func (c *Client) PublishJSONWithTraceID(ctx context.Context, subject string, v any) error {
	if ctx == nil {
		ctx = context.Background()
	}
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

// safeMsgHandler 把用户 handler 的调用包进 panic 收编层（#145）。
//
// 为什么必须有：handler 跑在 nats.go 的内部派发 goroutine（waitForMsgs）里，
// 上游 SDK 的派发循环直接 mcb(m)、没有任何 recover（nats.go@v1.55.0/nats.go
// :3861，全文件的 recover 只用于 closeStatusListener）——用户 handler 一个
// panic（外部不可信载荷反序列化成功但语义残缺、nil 解引用是最常见形态）就
// 穿透到 goroutine 顶，整个进程连同其上所有订阅、所有在途请求一并消灭。
// SubscribeJSON 连坏 JSON 都兜了底，唯独 handler 内 panic 没兜。
//
// 收编口径与 wsutil/outbox「写协程 panic 只许死这条连接，不许死整个进程」
// 同源：panic 收编为 Error 日志 + 该条消息丢弃留痕，订阅与进程存活。
//
// 就地 recover 而非 async.GoSafe（另起 goroutine）：nats.go 对同一订阅的
// 消息是**串行**派发的（waitForMsgs 单 goroutine 逐条 mcb(m)），换 goroutine
// 会把串行语义变成并发——handler 里的非线程安全状态会从「安全」变「竞态」。
// 禁止裸 go func 的约束（AGENTS.md）管的是启动 goroutine，这里不启动。
func safeMsgHandler(subject string, handler func(*nats.Msg)) func(*nats.Msg) {
	return func(m *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("handler panic recovered, message dropped",
					"subject", subject, "panic", fmt.Sprintf("%v", r))
			}
		}()
		handler(m)
	}
}

// Subscribe 订阅主题，收到消息时调用 handler。
// 返回 Subscription，调用 Unsubscribe() 取消订阅。
//
// handler 的 panic 被就地收编（#145）：记 Error 日志、丢弃该条消息，订阅与
// 进程照常存活——外部不可信载荷引发的 panic 不得杀死整个进程。
func (c *Client) Subscribe(subject string, handler func(*nats.Msg)) (*nats.Subscription, error) {
	sub, err := c.conn.Subscribe(subject, safeMsgHandler(subject, handler))
	if err != nil {
		return nil, fmt.Errorf("nats subscribe %s: %w", subject, err)
	}
	log.Info("subscribed", "subject", subject)
	return sub, nil
}

// QueueSubscribe 订阅主题（队列模式），同一队列组内只有一个消费者收到消息。
// 适用于负载均衡场景：多个服务实例订阅同一主题，消息只投递给其中一个。
//
// handler 的 panic 同样就地收编（#145，见 safeMsgHandler）。
func (c *Client) QueueSubscribe(subject, queue string, handler func(*nats.Msg)) (*nats.Subscription, error) {
	sub, err := c.conn.QueueSubscribe(subject, queue, safeMsgHandler(subject, handler))
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
// nil ctx 按 context.Background 处理（#101，与同文件 Publish 系列的兜底
// 口径一致——同一份客户端对所有 API 容忍 nil ctx，不挑入口崩进程）。
func (c *Client) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	// nil ctx 直接 WithTimeout 会 panic "cannot create context from nil parent"
	ctx, cancel := context.WithTimeout(ensureCtx(ctx), timeout)
	defer cancel()
	resp, err := c.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("nats request %s: %w", subject, err)
	}
	return resp, nil
}

// RequestJSON 发送 JSON 请求并反序列化响应。
// 使用 context 控制超时和取消。
// nil ctx 按 context.Background 处理（#101，与同文件 Publish 系列口径一致）。
//
// ⚠️ BREAKING（#88，0.x 阶段允许）：ctx 由第二位移至首位，与 Request 及
// Go「ctx 首参」惯例对齐——旧形态 RequestJSON[Req, Resp](c, ctx, ...) 与
// 同文件 Request(ctx, ...) 形态不一致，IDE 补全/人工誊写极易把 subject
// 传进 ctx 位。亦与 mq.Requester.RequestJSON 的接口形态一致。
func RequestJSON[Req, Resp any](ctx context.Context, c *Client, subject string, req *Req, timeout time.Duration) (*Resp, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("nats: marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ensureCtx(ctx), timeout)
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
