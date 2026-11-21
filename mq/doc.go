// Package mq 提供消息队列统一门面。
//
// 定义消息队列的通用接口（Publisher、Subscriber、Requester、MQClient），
// 支持多种后端实现（NATS、Kafka、RabbitMQ 等）。
//
// # v2 订阅面（#251）
//
// v1 订阅接口（[Subscriber]/[Subscription]/[MessageHandler]）无法表达
// ctx 生命周期、handler 错误/ack 返回与有界取消，承载不了 JetStream/NATS
// 的至少一次语义，已标注 Deprecated。可承载 ack/重投/headers/ack deadline
// 的并行契约为 [SubscriberV2]/[SubscriptionV2]/[MessageHandlerV2]/[Message]，
// 客户端整体替代为 [MQClientV2]；发布/请求两半（[Publisher]/[Requester]）
// 语义未变，继续使用。
//
// 当前实现：
//   - nats: 基于 NATS 的客户端（Close(ctx) error 已符合门面签名（#143）；
//     Request 返回 *nats.Msg 与门面 []byte 形态不同，尚未提供 mq.MQClient
//     适配器，需自行桥接）
//   - v2 订阅面的 NATS/JetStream 适配器尚未提供（#251 后续波）：接口先行，
//     适配器落地前接入方可按 [SubscriberV2] 自行桥接，门面不再需要为
//     ack/重投绕过。
package mq
