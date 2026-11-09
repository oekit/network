package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// JetStream 是 NATS JetStream v2 API 客户端封装。
// 使用新的 jetstream 包，提供持久化消息、至少一次投递保证、流和消费者管理。
type JetStream struct {
	js jetstream.JetStream
	// clientName 是建链时 WithName 的值（可空）：EnsureStream 缺省流名的派生源（#122）。
	clientName string
}

// NewJetStream 创建 JetStream v2 客户端。
func (c *Client) NewJetStream() (*JetStream, error) {
	js, err := jetstream.New(c.conn)
	if err != nil {
		return nil, fmt.Errorf("nats jetstream: %w", err)
	}
	return &JetStream{js: js, clientName: c.opts.name}, nil
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

// 家族缺省常量（#122）：fuxi 三仓 310 行近同手搓的 stream 装配缺省值在此
// 单源化——某仓改 MaxAge 其余仓不跟随的漂移面就此收口。
const (
	// DefaultStreamMaxAge 是 EnsureStream 的缺省消息保留时长。
	DefaultStreamMaxAge = 72 * time.Hour
	// DefaultStreamMaxBytes 是 EnsureStream 的缺省流字节上限（512 MB）。
	DefaultStreamMaxBytes = 512 << 20
)

// StreamSpec 是 EnsureStream 的声明式流规格（#122）。
//
// 零值字段一律取库缺省——消费方只写与缺省不同的字段；fuxi 三仓的近同手搓
// （stream 名缺省、subjects 通配格式、Storage/Retention、MaxAge/MaxBytes）
// 逐字段收敛到 StreamConfig 的映射规则见各字段文档。
type StreamSpec struct {
	// Name 流名。空值时从客户端名（WithName）派生：非字母数字折叠为 '_'、
	// 统一大写、补 "_EVENTS" 后缀（fuxi-engine → FUXI_ENGINE_EVENTS，与
	// 三仓既成缺省同形）。客户端也未设 WithName 时显式报错——流名是持久化
	// 对象的主键，不静默兜底。
	Name string
	// Subjects 流的 subject 通配集。空值时取 Name+".>"（三仓一致的缺省形态）。
	Subjects []string
	// Storage 存储策略。零值取 FileStorage（三仓一致）。⚠ nats.go 的
	// MemoryStorage 与零值同为 0、无法与「未设置」区分——内存流不是本原语
	// 的目标场景，需要时直接用 CreateOrUpdateStream 传完整 StreamConfig。
	Storage jetstream.StorageType
	// Retention 保留策略。零值即 LimitsPolicy（与库缺省一致）；显式
	// InterestPolicy / WorkQueuePolicy 直接生效。WorkQueue 存量流的不可原地
	// 更新冲突见 EnsureStream 的处置文案。
	Retention jetstream.RetentionPolicy
	// MaxAge 消息保留时长。零值取 DefaultStreamMaxAge。
	MaxAge time.Duration
	// MaxBytes 流字节上限。零值取 DefaultStreamMaxBytes；服务器原生「0=不限」
	// 的语义在本原语不可达（0 被解释为未设置），需要时用 CreateOrUpdateStream。
	MaxBytes int64
}

// EnsureStream 声明式流装配（#122）：确保一条业务流按 StreamSpec 存在——
// 不存在则创建，已存在则按 spec 原地更新（上游 CreateOrUpdateStream 语义）。
//
// 与 CreateStream 的关系：CreateStream 是「仅当不存在时创建」的窄原语（流已
// 存在时不动配置、无缺省派生）；EnsureStream 是声明式组合口——缺省派生
// （流名/subjects/Storage/Retention/MaxAge/MaxBytes，见 StreamSpec）+
// CreateOrUpdateStream + 存量流冲突修复文案，fuxi 三仓 310 行近同手搓的装配
// 知识在此单源。
//
// WorkQueue 存量冲突：服务端对存量流的若干维度不可原地变更（retention 到/
// 从 workqueue、storage 类型变更、sealed 流等）。此类错误会附「需手动删除后
// 重启」的修复文案——删除会丢弃未消费消息，先确认消费水位。新增/删除 subject
// 对 WorkQueue 存量流同样受限（部分版本服务端直接拒绝），冲突时同样按文案处置。
//
// nil ctx 按 context.Background 处理（#101，与 JetStream 系列兜底口径一致）。
func (js *JetStream) EnsureStream(ctx context.Context, spec StreamSpec) (jetstream.Stream, error) {
	ctx = ensureCtx(ctx)

	name := spec.Name
	if name == "" {
		if js.clientName == "" {
			return nil, errors.New("nats: EnsureStream: 流名必填——StreamSpec.Name 为空且客户端未设 WithName，无从派生缺省流名")
		}
		name = derivedStreamName(js.clientName)
	}
	subjects := spec.Subjects
	if len(subjects) == 0 {
		subjects = []string{name + ".>"}
	}

	cfg := jetstream.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Storage:   spec.Storage,
		Retention: spec.Retention,
		MaxAge:    spec.MaxAge,
		MaxBytes:  spec.MaxBytes,
	}
	// 零值 = 家族缺省（#122）：缺省常量集中一处，语义见 StreamSpec 字段文档。
	if cfg.Storage == 0 {
		cfg.Storage = jetstream.FileStorage
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = DefaultStreamMaxAge
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultStreamMaxBytes
	}

	s, err := js.js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		// 存量流冲突处置（#122，三仓各自的修复文案上移库层单源）：区分
		// 「流存在但更新被拒」与「流不存在的创建失败」——前者是配置冲突
		// （上游 CreateOrUpdateStream 的原地更新被服务端校验拒绝），补修复
		// 文案；后者原样上抛，不掩盖真实故障。
		if _, getErr := js.js.Stream(ctx, name); getErr == nil {
			return nil, fmt.Errorf("nats: update stream %s: %w（存量流配置冲突，不可原地更新——常见于 retention 到/从 workqueue、storage 类型变更；需手动删除该流后重启，删除会丢弃未消费消息，先确认消费水位）", name, err)
		}
		return nil, fmt.Errorf("nats: create/update stream %s: %w", name, err)
	}
	log.Info("stream ensured", "name", name, "subjects", subjects)
	return s, nil
}

// derivedStreamName 从客户端名（WithName）派生缺省流名（#122）：非字母数字
// 折叠为 '_'、统一大写、补 "_EVENTS" 后缀，与三仓既成缺省（FUXI_ENGINE_EVENTS
// 等）同形。
func derivedStreamName(clientName string) string {
	var b strings.Builder
	b.Grow(len(clientName) + len("_EVENTS"))
	for _, r := range clientName {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 32)
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	b.WriteString("_EVENTS")
	return b.String()
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
