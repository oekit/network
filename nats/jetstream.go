package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// JetStream 是 NATS JetStream v2 API 客户端封装。
// 使用新的 jetstream 包，提供持久化消息、至少一次投递保证、流和消费者管理。
type JetStream struct {
	js jetstream.JetStream
}

// NewJetStream 创建 JetStream v2 客户端。
func (c *Client) NewJetStream() (*JetStream, error) {
	js, err := jetstream.New(c.conn)
	if err != nil {
		return nil, fmt.Errorf("nats jetstream: %w", err)
	}
	return &JetStream{js: js}, nil
}

// CreateStream 创建流（如果不存在）。
// 仅在流确实不存在（ErrStreamNotFound）时创建；
// 瞬时错误原样返回，避免误判掩盖真实故障。
// nil ctx 按 context.Background 处理（#101，与 Client 系列兜底口径一致）。
func (js *JetStream) CreateStream(ctx context.Context, name string, subjects ...string) error {
	ctx = ensureCtx(ctx)
	_, err := js.js.Stream(ctx, name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("nats: get stream %s: %w", name, err)
	}

	_, err = js.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		return fmt.Errorf("nats: create stream %s: %w", name, err)
	}
	log.Info("stream created", "name", name, "subjects", subjects)
	return nil
}

// Stream 获取流。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return js.js.Stream(ensureCtx(ctx), name)
}

// DeleteStream 删除流。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) DeleteStream(ctx context.Context, name string) error {
	return js.js.DeleteStream(ensureCtx(ctx), name)
}

// Publish 发布消息到 JetStream 流。
// nil ctx 按 context.Background 处理（#79，与 Client.Publish 口径一致）。
func (js *JetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ack, err := js.js.Publish(ctx, subject, data, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: jetstream publish %s: %w", subject, err)
	}
	return ack, nil
}

// PublishJSON 发布 JSON 消息到 JetStream 流。
// nil ctx 按 context.Background 处理（#79）。
func (js *JetStream) PublishJSON(ctx context.Context, subject string, v any, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("nats: marshal: %w", err)
	}
	return js.Publish(ctx, subject, data, opts...)
}

// CreateOrUpdateConsumer 创建或更新消费者。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	ctx = ensureCtx(ctx)
	s, err := js.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("nats: get stream %s: %w", stream, err)
	}
	return s.CreateOrUpdateConsumer(ctx, cfg)
}

// getOrCreateConsumer 获取已有消费者，不存在时才创建。
// 避免 CreateOrUpdateConsumer 覆盖已有消费者的配置（DeliverPolicy、AckPolicy 等）。
func (js *JetStream) getOrCreateConsumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	s, err := js.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("nats: get stream %s: %w", stream, err)
	}
	// 先尝试获取已有消费者
	cons, err := s.Consumer(ctx, consumer)
	if err == nil {
		return cons, nil
	}
	// 仅在消费者确实不存在时创建；瞬时错误（网络抖动等）若误判为
	// "不存在" 会以默认配置重建消费者，导致已有 DeliverPolicy 丢失、
	// 整条流重新投递
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	// 不存在则创建。必须建 **durable** 消费者（#99）：只给 Name 时服务端按
	// 临时（ephemeral）消费者处理，强制 InactiveThreshold=5s（nats-server 的
	// JsDeleteWaitTimeDefault，每次拉取重置计时）——空闲约 5 秒即被服务端
	// 删除；下次调用 ErrConsumerNotFound ⇒ 按默认配置重建 ⇒ ack 水位丢失、
	// 整条流重新投递。Durable 与 Name 必须相等（上游 nats.go 的约束），
	// durable 消费者不受空闲回收。
	return s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:    consumer,
		Durable: consumer,
	})
}

// Consume 消费消息（推模式，自动管理 ACK）。
// 返回 ConsumeContext，调用 Stop() 停止消费。
//
// ⚠️ ctx 只界「查/建消费者」阶段：上游 Consume 不收 ctx，消费一旦启动
// **只由返回的 ConsumeContext 的 Stop()/Drain() 停止**（#103 文档化口径，
// SDK 限制）。nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) Consume(ctx context.Context, stream, consumer string, handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	cons, err := js.getOrCreateConsumer(ensureCtx(ctx), stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Consume(handler, opts...)
}

// Fetch 获取指定数量的消息（拉模式）。
//
// ctx 界住**整个调用**（#103）：既界消费者查找，也经 jetstream.FetchContext
// 注入取消息阶段——ctx 取消/到期时取消息立即终止，不再挂在 SDK 的
// DefaultExpires（30s）上。delivery 阶段的取消原因经 batch.Error() 返回。
//
// 例外：调用方显式给 FetchMaxWait 时，上游禁止与 FetchContext 同用
// （ErrInvalidOption），此时以显式超时为准回退无 ctx 形态（SDK 限制，
// 取消息阶段最多等 FetchMaxWait）——见 fetchWithContext。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) Fetch(ctx context.Context, stream, consumer string, batchSize int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	ctx = ensureCtx(ctx)
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return fetchWithContext(ctx, cons, batchSize, opts...)
}

// Next 获取下一条消息（拉模式，阻塞直到超时）。
//
// ctx 界住**整个调用**（#103）：同 Fetch，经 jetstream.FetchContext 注入
// 取消息阶段，取消不再等 30s 过期。例外口径同 Fetch（FetchMaxWait 优先）。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) Next(ctx context.Context, stream, consumer string, opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	ctx = ensureCtx(ctx)
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return nextWithContext(ctx, cons, opts...)
}

// Messages 创建消息迭代器（拉模式，持续消费）。
//
// ⚠️ ctx 只界「查/建消费者」阶段：上游 Messages 不收 ctx，迭代器一旦创建
// 只由返回的 MessagesContext 的 Stop()/Drain() 停止（#103 文档化口径，
// SDK 限制）。nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) Messages(ctx context.Context, stream, consumer string) (jetstream.MessagesContext, error) {
	cons, err := js.getOrCreateConsumer(ensureCtx(ctx), stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Messages()
}

// ConsumerInfo 获取消费者信息。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) ConsumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error) {
	cons, err := js.getOrCreateConsumer(ensureCtx(ctx), stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Info(ensureCtx(ctx))
}

// StreamInfo 获取流信息。
// nil ctx 按 context.Background 处理（#101）。
func (js *JetStream) StreamInfo(ctx context.Context, stream string) (*jetstream.StreamInfo, error) {
	ctx = ensureCtx(ctx)
	s, err := js.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("nats: get stream %s: %w", stream, err)
	}
	return s.Info(ctx)
}

// fetchWithContext 把 ctx 注入取消息阶段（#103）：上游是
// jetstream.FetchContext(ctx)，取消息立即响应取消/到期。
//
// 上游禁止 FetchContext 与 FetchMaxWait 同用（均映射 ErrInvalidOption）——
// 调用方显式给了 FetchMaxWait 时以显式超时为准回退无 ctx 形态（SDK 限制）；
// 回退前复查 ctx：已取消/到期的 ctx 不回退，立即返回取消原因，不挂 30s。
func fetchWithContext(ctx context.Context, cons jetstream.Consumer, batchSize int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("nats: fetch cancelled: %w", err)
	}
	injected := append([]jetstream.FetchOpt{jetstream.FetchContext(ctx)}, opts...)
	batch, err := cons.Fetch(batchSize, injected...)
	if errors.Is(err, jetstream.ErrInvalidOption) {
		// FetchMaxWait 冲突（或 deadline 恰在注入判定后过期）：ctx 已取消/
		// 到期则直接返回取消原因；否则回退调用方给的显式超时形态。
		if cerr := ctx.Err(); cerr != nil {
			return nil, fmt.Errorf("nats: fetch cancelled: %w", cerr)
		}
		return cons.Fetch(batchSize, opts...)
	}
	return batch, err
}

// nextWithContext 同 fetchWithContext（#103）：单条形态，上游 Next 走同一
// fetchWithOpts 路径，FetchContext/Next 冲突口径一致。
func nextWithContext(ctx context.Context, cons jetstream.Consumer, opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("nats: next cancelled: %w", err)
	}
	injected := append([]jetstream.FetchOpt{jetstream.FetchContext(ctx)}, opts...)
	msg, err := cons.Next(injected...)
	if errors.Is(err, jetstream.ErrInvalidOption) {
		if cerr := ctx.Err(); cerr != nil {
			return nil, fmt.Errorf("nats: next cancelled: %w", cerr)
		}
		return cons.Next(opts...)
	}
	return msg, err
}

// PublishAsync 异步发布消息（批量发布，提高吞吐量）。
func (js *JetStream) PublishAsync(subject string, data []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return js.js.PublishAsync(subject, data, opts...)
}

// PublishAsyncComplete 等待所有异步发布完成。
func (js *JetStream) PublishAsyncComplete() <-chan struct{} {
	return js.js.PublishAsyncComplete()
}
