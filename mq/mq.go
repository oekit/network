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
	//
	// 截止契约（#143，两个截止源并存必须定义交互，同 sshx #106 的教训）：
	// ctx 截止与 timeout 谁也不压谁——**谁先到谁生效**，有效截止取两者
	// 较早者（context.WithTimeout 的合成语义，参考实现 nats.Client.Request
	// 即如此合成）。timeout ≤ 0 **不是**「不限时」：按同一语义视为立即
	// 到期——本接口的每个请求必然有界，长等待请把 timeout 设为上界值
	// （ctx 先到仍先生效）。
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)

	// RequestJSON 发送 JSON 请求并等待 JSON 应答。
	//
	// 截止契约与 [Requester.Request] 相同：ctx 截止与 timeout 先到先生效，
	// timeout ≤ 0 视为立即到期。
	RequestJSON(ctx context.Context, subject string, req any, resp any, timeout time.Duration) error
}

// MQClient 消息队列客户端接口，组合了 Publisher、Subscriber 和 Requester。
type MQClient interface {
	Publisher
	Subscriber
	Requester

	// Close 关闭客户端连接。
	//
	// 签名遵循矩阵硬规则「组件 Close 统一 Close(ctx) error + sync.Once 幂等」
	//（AGENTS.md）：ctx 是关停的**上界入口**——Drain/flush 遇慢消费者不得
	// 无界阻塞（#83/#86 的事故形态），ctx 到期须回退强制关闭（修法参照
	// nats.Client.Close）；实现须幂等，重复 Close 返回 nil。
	//
	// ⚠️ BREAKING（#143，0.x 允许，参照 nats PR #91 先例）：原 `Close() error`
	// 移除——无 ctx 的门面签名把「无界关停」定成了契约，实现者连给关停设
	// 上界的入口都没有；Go 不支持同名方法重载，无法保留兼容别名。调用点
	// 改为 `c.Close(context.Background())`，要上界就传带截止的 ctx。
	Close(ctx context.Context) error
}

// MessageHandler 消息处理函数。
type MessageHandler func(ctx context.Context, msg []byte)

// Subscription 订阅句柄。
type Subscription interface {
	// Unsubscribe 取消订阅。
	Unsubscribe() error
}
