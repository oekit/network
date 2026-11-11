package nats

// nats_lifecycle_panic_test.go 钉住 #165 的修复：生命周期回调
// （onDisconnect/onReconnect/onClose）跑在 nats.go 内部 goroutine
// （flusher/dispatcher/reconnect loop）里，上游无 recover——用户回调一个
// panic 就穿透到 goroutine 顶，整个进程连同所有订阅、所有在途请求一并消灭。
// safeLifecycleCallback 以 pkg/errors.Recovered（#182 全矩阵单源）就地收编：
// Error 留痕（logx.ErrKey）+ 回调丢弃，连接生命周期照常推进。
//
// 断言口径（panic 类）：panic 后进程与连接状态机仍存活——断连/重连场景下
// 连接必须能走完状态迁移、恢复后收发照常。

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	pkgerrors "github.com/oekit/pkg/errors"
)

// TestLifecycleCallback_PanicDoesNotKillProcess 回归（#165）：onDisconnect/
// onClose panic 不得穿透 nats.go 内部 goroutine 打死进程；连接状态机走完
// DISCONNECT → CLOSED，两个回调都真实执行过（panic 确实发生过，而非没触发）。
func TestLifecycleCallback_PanicDoesNotKillProcess(t *testing.T) {
	// -1 → 随机端口；WithMaxReconnects(0)：断连后直接进 CLOSED（onClose
	// 一并触发），不走重连状态机。
	portOpts := natsserver.DefaultTestOptions
	portOpts.Port = -1
	srv := natsserver.RunServer(&portOpts)
	t.Cleanup(func() { srv.Shutdown() })

	var disconnectBoom, closeBoom atomic.Bool
	c, err := NewClient(srv.ClientURL(),
		WithName("ekit-nats-lifecycle-panic-test"),
		WithMaxReconnects(0),
		WithDisconnectHandler(func(conn *nats.Conn, err error) {
			disconnectBoom.Store(true)
			panic("disconnect callback panic (simulated)")
		}),
		WithCloseHandler(func(conn *nats.Conn) {
			closeBoom.Store(true)
			panic("close callback panic (simulated)")
		}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	// StatusChanged 是状态迁移的确定性信号（#147 同款）：先注册再关停。
	closed := c.Conn().StatusChanged(nats.CLOSED)
	srv.Shutdown()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never reached CLOSED after the server shutdown")
	}

	// 用户回调经 nats.go 的异步回调队列派发，可能略晚于状态迁移事件落地：
	// 轮询等两个回调都真实执行过（否则测试前提不成立）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if disconnectBoom.Load() && closeBoom.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !disconnectBoom.Load() {
		t.Error("onDisconnect was never invoked — test premise broken")
	}
	if !closeBoom.Load() {
		t.Error("onClose was never invoked — test premise broken")
	}

	// 进程活着才能走到这里：panic 若穿透，nats.go 内部 goroutine 直接杀死
	// 整个测试进程。连接状态机也已走完。
	if status := c.Conn().Status(); status != nats.CLOSED {
		t.Errorf("conn status = %v, want CLOSED", status)
	}
}

// TestLifecycleCallback_ReconnectPanicDoesNotBreakRecovery 回归（#165）：
// onReconnect panic 不得杀死 nats.go 的重连链路——服务端重启后连接必须恢复，
// 恢复后收发照常（订阅自动重挂）。
func TestLifecycleCallback_ReconnectPanicDoesNotBreakRecovery(t *testing.T) {
	portOpts := natsserver.DefaultTestOptions
	portOpts.Port = -1
	srv := natsserver.RunServer(&portOpts)
	t.Cleanup(func() { srv.Shutdown() })
	port := srv.Addr().(*net.TCPAddr).Port

	var reconnectBoom atomic.Bool
	c, err := NewClient(srv.ClientURL(),
		WithName("ekit-nats-reconnect-panic-test"),
		WithMaxReconnects(-1),
		WithReconnectWait(50*time.Millisecond),
		WithReconnectHandler(func(conn *nats.Conn) {
			reconnectBoom.Store(true)
			panic("reconnect callback panic (simulated)")
		}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	// 恢复后收发验证用的订阅：先挂上，重连后 nats.go 自动重挂。
	got := make(chan []byte, 1)
	sub, err := c.Subscribe("lifecycle.panic.recover", func(m *nats.Msg) {
		got <- append([]byte(nil), m.Data...)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	// 服务端闪断 → 进入重连窗口 → 同端口重启服务端 → 连接必须恢复。
	reconnecting := c.Conn().StatusChanged(nats.RECONNECTING)
	srv.Shutdown()
	select {
	case <-reconnecting:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never entered RECONNECTING after the server shutdown")
	}

	restartOpts := natsserver.DefaultTestOptions
	restartOpts.Port = port
	srv2 := natsserver.RunServer(&restartOpts)
	t.Cleanup(func() { srv2.Shutdown() })

	connected := c.Conn().StatusChanged(nats.CONNECTED)
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never recovered after the server restart — the reconnect loop died with the panicking onReconnect (#165)")
	}
	// 用户回调经 nats.go 的异步回调队列派发，可能略晚于状态迁移事件落地：
	// 轮询等回调真实执行过（否则测试前提不成立）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reconnectBoom.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !reconnectBoom.Load() {
		t.Error("onReconnect was never invoked — test premise broken")
	}

	// 恢复后收发照常（证明回调 panic 收编没有拖垮连接）。
	if err := c.Publish(context.Background(), "lifecycle.panic.recover", []byte("after-reconnect")); err != nil {
		t.Fatalf("Publish after reconnect: %v", err)
	}
	select {
	case data := <-got:
		if string(data) != "after-reconnect" {
			t.Errorf("got %q, want %q", data, "after-reconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the message published after reconnect")
	}
}

// TestRecoverCallback_ErrorIdentity 单测 recoverCallback 的收编契约：
// panic(err) 收编后 error 身份不丢（errors.Is/As 穿透 PanicError，#182 单源
// 的核心承诺），栈在场；无 panic 时返回 nil。
func TestRecoverCallback_ErrorIdentity(t *testing.T) {
	boom := errors.New("lifecycle-boom")
	err := recoverCallback(func() { panic(boom) })
	if err == nil {
		t.Fatal("expected the panicking callback to be converted to an error, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("errors.Is(recovered, boom) = false — the error identity was lost in the panic conversion: %v", err)
	}
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("errors.As(*PanicError) = false, got %T", err)
	}
	if len(perr.Stack) == 0 {
		t.Error("PanicError.Stack is empty — the panic site stack must be captured")
	}

	if nilErr := recoverCallback(func() {}); nilErr != nil {
		t.Errorf("recoverCallback without panic = %v, want nil", nilErr)
	}
}
