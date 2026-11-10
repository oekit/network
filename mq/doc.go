// Package mq 提供消息队列统一门面。
//
// 定义消息队列的通用接口（Publisher、Subscriber、Requester、MQClient），
// 支持多种后端实现（NATS、Kafka、RabbitMQ 等）。
//
// 当前实现：
//   - nats: 基于 NATS 的客户端（Close(ctx) error 已符合门面签名（#143）；
//     Request 返回 *nats.Msg 与门面 []byte 形态不同，尚未提供 mq.MQClient
//     适配器，需自行桥接）
package mq
