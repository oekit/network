// Package mq 提供消息队列统一门面。
//
// 定义消息队列的通用接口，支持多种后端实现（NATS、Kafka、RabbitMQ 等）。
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
