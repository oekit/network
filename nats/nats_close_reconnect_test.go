package nats

// nats_close_reconnect_test.go 钉住 #147 的修复：Close(ctx) 落在重连窗口时
// 不得返回虚假错误。上游 nats.go 的 Drain() 在连接重连中时执行
// nc.Close(); return ErrConnectionReconnecting（nats.go@v1.55.0/nats.go:
// 6507-6510）——连接已被强制关闭、正是关停想要的结果（目标态已达成），
// 旧的错误豁免只认 ErrConnectionClosed，把它误记成
// 「nats: drain: nats: connection reconnecting」失败，与自家「目标态已达成
// 不算错误」口径相反。
//
// 时序全部用可控同步原语（StatusChanged 状态事件在 changeConnStatus 里同步
// 投递、channel 缓冲 10），不靠 sleep 赌时序。

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// TestClient_Close_ReconnectWindowTargetStateAchieved 回归（#147）：服务端
// 闪断后客户端停在 RECONNECTING，此时 Close(ctx) 必须返回 nil——连接已被
// 强制关闭、目标态已达成，不算错误。上层关停逻辑按错误处置会误报告警、
// 非零退出码、或跳过后续清理。
func TestClient_Close_ReconnectWindowTargetStateAchieved(t *testing.T) {
	// -1 → 随机端口；与 newTestClient 不同，本用例**要**走重连状态机，
	// 因此自建客户端：无限重连（窗口不因次数耗尽塌缩为 CLOSED）+ 短重试
	// 间隔（失败重试快速循环，状态稳定停在 RECONNECTING）。
	portOpts := natsserver.DefaultTestOptions
	portOpts.Port = -1
	srv := natsserver.RunServer(&portOpts)
	t.Cleanup(func() { srv.Shutdown() })

	c, err := NewClient(srv.ClientURL(),
		WithName("ekit-nats-reconnect-test"),
		WithMaxReconnects(-1),
		WithReconnectWait(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Conn().Close() })

	// StatusChanged(RECONNECTING)：状态迁移的确定性信号——先注册再关停，
	// 收到事件即证明连接已进入重连窗口。
	reconnecting := c.Conn().StatusChanged(nats.RECONNECTING)
	srv.Shutdown()
	select {
	case <-reconnecting:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never entered RECONNECTING after the server shutdown")
	}
	if status := c.Conn().Status(); status != nats.RECONNECTING {
		t.Fatalf("conn status = %v, want RECONNECTING", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close(ctx) in the reconnect window = %v, want nil — the conn is force-closed by upstream Drain (target state achieved, #147)", err)
	}
	if status := c.Conn().Status(); status != nats.CLOSED {
		t.Errorf("conn status = %v, want CLOSED after Close", status)
	}
}
