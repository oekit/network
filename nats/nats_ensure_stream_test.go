package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
)

// newJSClient 启动一台带 JetStream 的内嵌 NATS server 并返回已连接的
// Client，name 传空串表示不设 WithName（派生缺省流名的报错路径用）。
func newJSClient(t *testing.T, name string) *Client {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natsserver.RunServer(&opts)
	t.Cleanup(func() { srv.Shutdown() })

	clientOpts := []NatsOption{WithMaxReconnects(0)}
	if name != "" {
		clientOpts = append(clientOpts, WithName(name))
	}
	c, err := NewClient(srv.ClientURL(), clientOpts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// TestJetStream_EnsureStream_DefaultsDerivedFromClientName 回归（#122）：
// StreamSpec 零值 ⇒ 流名从 WithName 派生（fuxi-engine → FUXI_ENGINE_EVENTS，
// 与三仓既成缺省同形）、subjects 取 Name+".>"、Storage/Retention/MaxAge/
// MaxBytes 取家族缺省——三仓 310 行手搓装配的缺省知识在此单源。
func TestJetStream_EnsureStream_DefaultsDerivedFromClientName(t *testing.T) {
	c := newJSClient(t, "fuxi-engine")
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := js.EnsureStream(ctx, StreamSpec{})
	if err != nil {
		t.Fatalf("EnsureStream (zero spec): %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info: %v", err)
	}

	if info.Config.Name != "FUXI_ENGINE_EVENTS" {
		t.Errorf("stream name = %q, want %q (derived from WithName)", info.Config.Name, "FUXI_ENGINE_EVENTS")
	}
	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "FUXI_ENGINE_EVENTS.>" {
		t.Errorf("subjects = %v, want [FUXI_ENGINE_EVENTS.>]", info.Config.Subjects)
	}
	if info.Config.Storage != jetstream.FileStorage {
		t.Errorf("storage = %v, want FileStorage", info.Config.Storage)
	}
	if info.Config.Retention != jetstream.LimitsPolicy {
		t.Errorf("retention = %v, want LimitsPolicy", info.Config.Retention)
	}
	if info.Config.MaxAge != DefaultStreamMaxAge {
		t.Errorf("maxAge = %v, want %v", info.Config.MaxAge, DefaultStreamMaxAge)
	}
	if info.Config.MaxBytes != DefaultStreamMaxBytes {
		t.Errorf("maxBytes = %d, want %d", info.Config.MaxBytes, DefaultStreamMaxBytes)
	}
}

// TestJetStream_EnsureStream_ExplicitConfigWins 回归（#122）：显式字段一律
// 覆盖缺省——声明式缺省只兜零值，不吞显式配置。
func TestJetStream_EnsureStream_ExplicitConfigWins(t *testing.T) {
	c := newJSClient(t, "fuxi-flow")
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	spec := StreamSpec{
		Name:      "ORDERS",
		Subjects:  []string{"orders.created", "orders.paid"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.InterestPolicy,
		MaxAge:    time.Hour,
		MaxBytes:  64 << 20,
	}
	_, err = js.EnsureStream(ctx, spec)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	info, err := js.StreamInfo(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if len(info.Config.Subjects) != 2 || info.Config.Subjects[0] != "orders.created" || info.Config.Subjects[1] != "orders.paid" {
		t.Errorf("subjects = %v, want [orders.created orders.paid]", info.Config.Subjects)
	}
	if info.Config.Retention != jetstream.InterestPolicy {
		t.Errorf("retention = %v, want InterestPolicy", info.Config.Retention)
	}
	if info.Config.MaxAge != time.Hour {
		t.Errorf("maxAge = %v, want 1h", info.Config.MaxAge)
	}
	if info.Config.MaxBytes != 64<<20 {
		t.Errorf("maxBytes = %d, want %d", info.Config.MaxBytes, int64(64<<20))
	}
}

// TestJetStream_EnsureStream_IdempotentThenUpdates 回归（#122）：同一 spec
// 重复调用必须幂等（启动期安全重入）；改字段后再调用走 CreateOrUpdateStream
// 的原地更新语义（与窄原语 CreateStream「已存在即不动」的区别所在）。
func TestJetStream_EnsureStream_IdempotentThenUpdates(t *testing.T) {
	c := newJSClient(t, "fuxi-sundial")
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	spec := StreamSpec{Name: "PANEL_EVENTS", MaxAge: DefaultStreamMaxAge}
	for i := 0; i < 2; i++ {
		_, err = js.EnsureStream(ctx, spec)
		if err != nil {
			t.Fatalf("EnsureStream (call %d, idempotent): %v", i+1, err)
		}
	}

	// 更新语义：MaxAge 改短后再装配 ⇒ 服务端配置随之更新。
	_, err = js.EnsureStream(ctx, StreamSpec{Name: "PANEL_EVENTS", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("EnsureStream (update): %v", err)
	}
	info, err := js.StreamInfo(ctx, "PANEL_EVENTS")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != time.Hour {
		t.Errorf("maxAge after update = %v, want 1h (CreateOrUpdateStream semantics)", info.Config.MaxAge)
	}
}

// TestJetStream_EnsureStream_WorkQueueConflictRemediation 回归（#122）：存量
// WorkQueue 流的 retention 不可原地改到 LimitsPolicy ⇒ EnsureStream 报错且
// 附「需手动删除后重启」的修复文案——三仓各自的处置文案上移库层单源。
func TestJetStream_EnsureStream_WorkQueueConflictRemediation(t *testing.T) {
	c := newJSClient(t, "fuxi-engine")
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 存量流：早年用 WorkQueue 策略建的业务流（fuxi 下游的真实冲突形态）。
	_, err = js.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "LEGACY_WQ",
		Subjects:  []string{"legacy.wq.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("seed workqueue stream: %v", err)
	}

	_, err = js.EnsureStream(ctx, StreamSpec{Name: "LEGACY_WQ", Subjects: []string{"legacy.wq.>"}})
	if err == nil {
		t.Fatal("EnsureStream on an existing workqueue stream must fail (retention cannot change to/from workqueue in place)")
	}
	if !strings.Contains(err.Error(), "手动删除") || !strings.Contains(err.Error(), "重启") {
		t.Errorf("error = %v, want the remediation text (手动删除…重启) attached to the conflict (#122)", err)
	}
	// 存量流必须原样保留——库不替业务做删除这类处置。
	if _, getErr := js.Stream(ctx, "LEGACY_WQ"); getErr != nil {
		t.Errorf("existing stream must survive the failed update: %v", getErr)
	}
}

// TestJetStream_EnsureStream_NoNameAndNoClientNameErrors 回归（#122）：流名
// 无从派生（spec.Name 空 + 客户端未设 WithName）时显式报错——流名是持久化
// 对象的主键，不静默兜底。
func TestJetStream_EnsureStream_NoNameAndNoClientNameErrors(t *testing.T) {
	c := newJSClient(t, "") // 不设 WithName
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.EnsureStream(ctx, StreamSpec{})
	if err == nil {
		t.Fatal("EnsureStream with no spec.Name and no WithName must error")
	}
	if !strings.Contains(err.Error(), "流名必填") {
		t.Errorf("error = %v, want an explicit name-required error", err)
	}
}

// TestDerivedStreamName 单元钉住派生规则：非字母数字折叠 '_'、统一大写、
// 补 _EVENTS 后缀。
func TestDerivedStreamName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"fuxi-engine", "FUXI_ENGINE_EVENTS"},
		{"fuxi_flow", "FUXI_FLOW_EVENTS"},
		{"FUXI", "FUXI_EVENTS"},
		{"svc.io", "SVC_IO_EVENTS"},
	} {
		if got := derivedStreamName(tc.in); got != tc.want {
			t.Errorf("derivedStreamName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
