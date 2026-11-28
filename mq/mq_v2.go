package mq

import (
	"context"
	"errors"
	"time"
)

// v2 订阅面（#251）：可承载投递语义的订阅契约（v1 订阅面已删除——
// 见 doc.go；0.x 允许 breaking，发布/请求两半 [Publisher]/[Requester]
// 未动）。
//
// 设计目标：JetStream/NATS 的至少一次投递（显式 Ack、Nak 重投、毒丸
// Term）与订阅生命周期（ctx 上界、有界 Unsubscribe）必须在**接口层面**
// 可表达，接入方不必绕过门面直连 SDK。
//
// # 消息处置契约
//
// 实现（含后端适配器）必须按以下顺序解释 handler 的意图，fake 契约测试
// 见 mq_v2_test.go：
//
//  1. 显式处置优先：handler 内调用 [Message].Ack / Nak / Term 后消息 fate
//     已定，handler 的返回值不再改变它（返回值仍应记录，供重试预算/
//     告警决策）。
//  2. 未显式处置时按 handler 返回值自动处置：返回 nil ⇒ Ack；返回非 nil
//     ⇒ Nak（至少一次投递的失败重投语义）。
//  3. 自动处置失败不得静默吞掉：handler 已返回，调用方拿不到该错误，
//     实现必须自行留痕（日志/指标），否则「确认失败但没人知道」会退化成
//     重复投递风暴（与 #251「不可表达不能伪装成功」同一哲学）。
//
// Ack/Nak/Term 至多生效一次；重复调用返回实现定义的错误——业务不得把它
// 当成处理失败（消息 fate 已定）。

// ErrAckUnsupported 表示该投递面不支持显式确认（at-most-once，如 core
// NATS 普通订阅，或 Kafka 自动提交模式的适配器）。
//
// 实现必须在 [Message].Ack / Nak / Term 返回本哨兵（errors.Is 可判定），
// **不得静默返回 nil**——静默成功会让 handler 误以为消息不会被重投，把
// 「后端无法表达」伪装成「已确认」，正是 #251 要消灭的缺陷形态。
var ErrAckUnsupported = errors.New("mq: 该投递面不支持显式消息确认")

// Header 是消息头（多值形态，与 net/http.Header / nats.Header 同形）。
//
// 键是否大小写敏感由后端定义；本类型不做归一化。只读视图：实现方保证
// handler 执行期间 map 不被并发修改。
type Header map[string][]string

// Get 返回 key 的第一个值；不存在或空值列表返回 ""。
func (h Header) Get(key string) string {
	if values := h[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// Message 是一条 v2 投递消息。
//
// 除载荷外携带 headers、ack deadline 与显式处置入口（#251）：Ack 确认
// 成功、Nak 要求重投、Term 终止投递（毒丸/不可恢复消息）。后端不支持
// 显式确认时三者返回 [ErrAckUnsupported]。
//
// 实现方不得在 handler 返回后复用 Data/Headers 的底层存储（或必须文档化
// 该生命周期）；handler 内应把需要留存的内容拷贝出去。
type Message interface {
	// Subject 返回消息主题（通配订阅下是实际命中的具体主题）。
	Subject() string

	// Data 返回消息载荷。只读：handler 不得修改，需要保留请自行拷贝。
	Data() []byte

	// Headers 返回消息头；nil 表示无头。只读。
	Headers() Header

	// AckDeadline 返回本条消息的确认截止时间（后端的 ack wait 窗口）。
	//
	// 零值表示后端未提供该信息——**不表示无限期**；未提供时以订阅/
	// 消费者配置为准。handler 可用它核对剩余处理预算，避免 Ack 时已
	// 超出重投窗口。
	AckDeadline() time.Time

	// Ack 确认消息处理成功，broker 不再重投。
	//
	// 返回 nil 表示确认已被后端接受；后端不支持显式确认时返回
	// [ErrAckUnsupported]（不得静默成功，见该哨兵文档）。
	Ack() error

	// Nak 否定确认，要求 broker 重投（至少一次语义的失败路径）。
	//
	// 不支持显式确认时返回 [ErrAckUnsupported]。
	Nak() error

	// Term 终止投递：broker 既不重投也不视为成功（毒丸/不可恢复消息）。
	//
	// 不支持显式确认时返回 [ErrAckUnsupported]。
	Term() error
}

// MessageHandlerV2 是 v2 订阅的消息处理函数。
//
// 返回值语义（详见包级「消息处置契约」）：nil = 处理成功（未显式处置时
// 实现自动 Ack）；非 nil = 处理失败（未显式处置时实现 Nak 触发重投）。
// 需要显式控制消息 fate（先落盘再 Ack、毒丸直接 Term、分类延迟重投）时
// 在 handler 内调用 [Message].Ack / Nak / Term——显式处置优先于返回值。
//
// ctx 由实现派生，订阅关停（Unsubscribe 的 ctx 到期）会取消它：handler
// 必须尊重 ctx.Done() 并及时返回，长任务不得无视取消无界挂住关停。
type MessageHandlerV2 func(ctx context.Context, msg Message) error

// SubscriptionV2 是 v2 订阅句柄。
type SubscriptionV2 interface {
	// Unsubscribe 停止投递并释放订阅。
	//
	// ctx 是关停的**上界入口**（#251）：等待在途 handler / drain 不得无界
	// 阻塞；ctx 到期须回退强制停止，在途未确认消息按至少一次语义由
	// broker 重投（「可能重复」优于「无限挂起」）。ctx 到期不视为
	// Unsubscribe 失败——强制停止达成后返回 nil；清理/网络层错误如实返回。
	//
	// 实现须幂等：重复调用返回 nil。
	Unsubscribe(ctx context.Context) error
}

// SubscriberV2 是可表达 at-least-once 投递语义的订阅接口（#251）。
//
// 相比已删除的 v1 订阅面：Subscribe/QueueSubscribe 接收 ctx（界订阅
// 生命周期）、handler 返回 error（自动 Ack/Nak 的依据）、消息带显式处置
// 入口与 headers/ack deadline，Unsubscribe 带关停上界。
type SubscriberV2 interface {
	// Subscribe 在 subject 上注册消息处理函数，返回订阅句柄。
	//
	// ctx 界订阅的**完整生命周期**：取消即停止投递并释放订阅（等价于传
	// 同一 ctx 的 Unsubscribe）；已交给 handler 的消息按「消息处置契约」
	// 收尾。订阅建立阶段的后端错误经返回值上抛。
	Subscribe(ctx context.Context, subject string, handler MessageHandlerV2) (SubscriptionV2, error)

	// QueueSubscribe 是带队列组的 Subscribe：同一队列组内只有一个消费者
	// 收到消息（负载均衡）。ctx/投递语义与 [SubscriberV2.Subscribe] 相同。
	QueueSubscribe(ctx context.Context, subject, queue string, handler MessageHandlerV2) (SubscriptionV2, error)
}

// MQClientV2 是 v2 消息队列客户端接口：发布/请求两半（[Publisher]/
// [Requester]）语义未变，订阅侧为可表达 ack/重投的 [SubscriberV2]，
// 关停是带 ctx 上界的 Close（#143）。
type MQClientV2 interface {
	Publisher
	SubscriberV2
	Requester

	// Close 关闭客户端连接。
	//
	// ctx 是关停上界（矩阵硬规则「组件 Close 统一 Close(ctx) error」），
	// 到期回退强制关闭；实现须幂等，重复 Close 返回 nil。
	Close(ctx context.Context) error
}
