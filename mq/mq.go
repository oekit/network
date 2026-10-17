// Package mq 提供消息队列统一门面。
//
// 定义消息队列的通用接口，支持多种后端实现（NATS、Kafka、RabbitMQ 等）。
//
// 使用示例：
//
//	// 任意后端实现 Publisher/Subscriber/Requester 接口后即可面向接口编程：
//	var p mq.Publisher = myPublisher
//	_ = p.Publish(ctx, "order.created", data)
package mq

import (
	"context"
	"time"
)

// Publisher 消息发布接口。
type Publisher interface {
	// Publish 发布消息。
	Publish(ctx context.Context, subject string, data []byte) error

	// PublishJSON 发布 JSON 消息。
	PublishJSON(ctx context.Context, subject string, v any) error
}

// Subscriber 消息订阅接口。
type Subscriber interface {
	// Subscribe 订阅消息。
	Subscribe(subject string, handler MessageHandler) (Subscription, error)

	// QueueSubscribe 队列订阅（负载均衡，同一队列组内只有一个消费者收到消息）。
	QueueSubscribe(subject, queue string, handler MessageHandler) (Subscription, error)
}

// Requester 请求-应答接口。
type Requester interface {
	// Request 发送请求并等待应答。
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)

	// RequestJSON 发送 JSON 请求并等待 JSON 应答。
	RequestJSON(ctx context.Context, subject string, req any, resp any, timeout time.Duration) error
}

// MQClient 消息队列客户端接口，组合了 Publisher、Subscriber 和 Requester。
type MQClient interface {
	Publisher
	Subscriber
	Requester

	// Close 关闭客户端连接。
	Close() error
}

// MessageHandler 消息处理函数。
type MessageHandler func(ctx context.Context, msg []byte)

// Subscription 订阅句柄。
type Subscription interface {
	// Unsubscribe 取消订阅。
	Unsubscribe() error
}
