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
func (js *JetStream) CreateStream(ctx context.Context, name string, subjects ...string) error {
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
func (js *JetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return js.js.Stream(ctx, name)
}

// DeleteStream 删除流。
func (js *JetStream) DeleteStream(ctx context.Context, name string) error {
	return js.js.DeleteStream(ctx, name)
}

// Publish 发布消息到 JetStream 流。
func (js *JetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := js.js.Publish(ctx, subject, data, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: jetstream publish %s: %w", subject, err)
	}
	return ack, nil
}

// PublishJSON 发布 JSON 消息到 JetStream 流。
func (js *JetStream) PublishJSON(ctx context.Context, subject string, v any, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("nats: marshal: %w", err)
	}
	return js.Publish(ctx, subject, data, opts...)
}

// CreateOrUpdateConsumer 创建或更新消费者。
func (js *JetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
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
	// 不存在则创建（使用默认配置）
	return s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: consumer,
	})
}

// Consume 消费消息（推模式，自动管理 ACK）。
// 返回 ConsumeContext，调用 Stop() 停止消费。
func (js *JetStream) Consume(ctx context.Context, stream, consumer string, handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Consume(handler, opts...)
}

// Fetch 获取指定数量的消息（拉模式）。
func (js *JetStream) Fetch(ctx context.Context, stream, consumer string, batchSize int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Fetch(batchSize, opts...)
}

// Next 获取下一条消息（拉模式，阻塞直到超时）。
func (js *JetStream) Next(ctx context.Context, stream, consumer string, opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Next(opts...)
}

// Messages 创建消息迭代器（拉模式，持续消费）。
func (js *JetStream) Messages(ctx context.Context, stream, consumer string) (jetstream.MessagesContext, error) {
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Messages()
}

// ConsumerInfo 获取消费者信息。
func (js *JetStream) ConsumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error) {
	cons, err := js.getOrCreateConsumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("nats: get consumer %s: %w", consumer, err)
	}
	return cons.Info(ctx)
}

// StreamInfo 获取流信息。
func (js *JetStream) StreamInfo(ctx context.Context, stream string) (*jetstream.StreamInfo, error) {
	s, err := js.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("nats: get stream %s: %w", stream, err)
	}
	return s.Info(ctx)
}

// PublishAsync 异步发布消息（批量发布，提高吞吐量）。
func (js *JetStream) PublishAsync(subject string, data []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return js.js.PublishAsync(subject, data, opts...)
}

// PublishAsyncComplete 等待所有异步发布完成。
func (js *JetStream) PublishAsyncComplete() <-chan struct{} {
	return js.js.PublishAsyncComplete()
}
