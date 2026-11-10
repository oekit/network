package sshx

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// TestDialWithPolicy_TimeoutThenSuccess 验收样例一（#125）：超时哨兵 × 默认
// 策略（Retries 零值 = 重拨 1 次）→ 第二次成功即返回，总耗时 ≤ 预算。
func TestDialWithPolicy_TimeoutThenSuccess(t *testing.T) {
	silent := holdListener(t)
	f := newFakeSSH(t)

	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, _ string) (net.Conn, error) {
		attempts++
		target := f.addr()
		if attempts == 1 {
			target = silent.Addr().String() // 第一次：连上但沉默 → 握手超时
		}
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, target)
	}

	start := time.Now()
	client, err := DialWithPolicy(context.Background(), f.addr(), clientConfig(),
		DialOptions{Handshake: 150 * time.Millisecond, Budget: 3 * time.Second, dialFunc: dialFn},
		DialPolicy{})
	if err != nil {
		t.Fatalf("预算内的超时重拨应当成功: %v", err)
	}
	_ = client.Close()
	if attempts != 2 {
		t.Fatalf("拨号 %d 次，期望 2 次（默认 Retries=1：超时重拨一次）", attempts)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("总耗时 %s，超出共享预算口径（3s 内不该出现）", elapsed)
	}
}

// TestDialWithPolicy_RejectionIsNotRetried 验收样例二（#125）：拒绝哨兵 →
// 立即返回不重拨。拒绝是判定不是抖动：重拨只是多打错凭据、把真错误盖掉。
func TestDialWithPolicy_RejectionIsNotRetried(t *testing.T) {
	ln := refuseSSH(t)
	cc := &ssh.ClientConfig{
		User:            "ops",
		Auth:            []ssh.AuthMethod{ssh.Password("wrong")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error) {
		attempts++
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
	}
	start := time.Now()
	_, err := DialWithPolicy(context.Background(), ln.Addr().String(), cc,
		DialOptions{dialFunc: dialFn},
		DialPolicy{Retries: 5}) // 上限给足：拦住重拨的必须是分类，不是预算
	if err == nil {
		t.Fatal("a refused handshake must fail")
	}
	if !errors.Is(err, ErrHandshakeRejected) {
		t.Fatalf("want a typed rejection, got %v", err)
	}
	if errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("a refusal must not be classifiable as a timeout: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("拨号 %d 次，期望 1 次（默认 Retryable 只认超时，拒绝绝不重拨）", attempts)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("拒绝应当立即返回，实际耗时 %s", elapsed)
	}
}

// TestDialWithPolicy_RejectionExplicitOptIn 拒绝类要调用方显式放行才重拨：
// 放行后按预算重拨至上限，最后一次的错误原样返回（分类口径与单次 Dial 一致）。
func TestDialWithPolicy_RejectionExplicitOptIn(t *testing.T) {
	ln := refuseSSH(t)
	cc := &ssh.ClientConfig{
		User:            "ops",
		Auth:            []ssh.AuthMethod{ssh.Password("wrong")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error) {
		attempts++
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
	}
	_, err := DialWithPolicy(context.Background(), ln.Addr().String(), cc,
		DialOptions{dialFunc: dialFn},
		DialPolicy{
			Retries: 2,
			Retryable: func(err error) bool {
				return errors.Is(err, ErrHandshakeTimeout) || errors.Is(err, ErrHandshakeRejected)
			},
		})
	if err == nil {
		t.Fatal("a refused handshake must fail")
	}
	if !errors.Is(err, ErrHandshakeRejected) {
		t.Fatalf("最后一次的错误应原样返回（typed rejection）, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("拨号 %d 次，期望 3 次（显式放行后 1 + Retries=2）", attempts)
	}
}

// TestDialWithPolicy_RetriesExhausted 沉默端 × Retries=2：三次尝试全超时，
// 最后一次的错误原样返回且类型可判。
func TestDialWithPolicy_RetriesExhausted(t *testing.T) {
	ln := holdListener(t)

	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error) {
		attempts++
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
	}
	_, err := DialWithPolicy(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Handshake: 80 * time.Millisecond, Budget: 3 * time.Second, dialFunc: dialFn},
		DialPolicy{Retries: 2})
	if err == nil {
		t.Fatal("重拨耗尽必须失败")
	}
	if attempts != 3 {
		t.Fatalf("拨号 %d 次，期望 3 次（1 + Retries=2）", attempts)
	}
	var hte *HandshakeTimeoutError
	if !errors.As(err, &hte) {
		t.Fatalf("最后一次的超时应保持类型化 (*HandshakeTimeoutError), got %v", err)
	}
}

// TestDialWithPolicy_BudgetExhaustedBetweenAttempts 验收样例三（#125）：预算
// 在第一次尝试内耗尽 → 不再重拨，返回的预算错误**包着超时链**（%w 保全
// ErrHandshakeTimeout 可判）。
func TestDialWithPolicy_BudgetExhaustedBetweenAttempts(t *testing.T) {
	ln := holdListener(t)

	slowConnect := func(ctx context.Context, timeout time.Duration, _, _ string) (net.Conn, error) {
		time.Sleep(100 * time.Millisecond) // 连接阶段耗掉大半预算
		return net.Dial("tcp", ln.Addr().String())
	}
	start := time.Now()
	_, err := DialWithPolicy(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Handshake: 10 * time.Second, Budget: 150 * time.Millisecond, dialFunc: slowConnect},
		DialPolicy{Retries: 3})
	if err == nil {
		t.Fatal("预算耗尽必须失败")
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Errorf("错误应说明预算耗尽: %v", err)
	}
	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("返回超时链（类型化错误在原因链上）: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Errorf("预算耗尽后不得再发起尝试，实际耗时 %s", elapsed)
	}
}

// TestDialWithPolicy_BackoffClampedToBudget 重拨等待从同一份预算里扣：Backoff
// 给出 10s 而预算只剩零头时，等待被夹到零——重拨不得成为超时放大器。
func TestDialWithPolicy_BackoffClampedToBudget(t *testing.T) {
	ln := holdListener(t)

	slowConnect := func(ctx context.Context, timeout time.Duration, _, _ string) (net.Conn, error) {
		time.Sleep(100 * time.Millisecond)
		return net.Dial("tcp", ln.Addr().String())
	}
	start := time.Now()
	_, err := DialWithPolicy(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Handshake: 10 * time.Second, Budget: 150 * time.Millisecond, dialFunc: slowConnect},
		DialPolicy{Retries: 3, Backoff: func(int) time.Duration { return 10 * time.Second }})
	if err == nil {
		t.Fatal("预算耗尽必须失败")
	}
	// 若等待未被夹住，这里会是 10s 量级；夹住则 ≈ 预算（150ms）。
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Errorf("Backoff 必须被夹到剩余预算内，实际耗时 %s（等待放大了总时长）", elapsed)
	}
	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("返回超时链: %v", err)
	}
}

// TestDialWithPolicy_BackoffRunsBetweenAttempts Backoff 真的在两次尝试之间
// 执行（attempt 从 1 起算）：总耗时下界 = 第一次超时 + 等待。
func TestDialWithPolicy_BackoffRunsBetweenAttempts(t *testing.T) {
	silent := holdListener(t)
	f := newFakeSSH(t)

	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, _ string) (net.Conn, error) {
		attempts++
		target := f.addr()
		if attempts == 1 {
			target = silent.Addr().String() // 第一次超时，重拨走健康端
		}
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, target)
	}
	const backoff = 60 * time.Millisecond
	start := time.Now()
	client, err := DialWithPolicy(context.Background(), f.addr(), clientConfig(),
		DialOptions{Handshake: 100 * time.Millisecond, Budget: 3 * time.Second, dialFunc: dialFn},
		DialPolicy{Backoff: func(attempt int) time.Duration {
			if attempt != 1 {
				t.Errorf("Backoff attempt = %d, want 1（首次重拨）", attempt)
			}
			return backoff
		}})
	if err != nil {
		t.Fatalf("重拨应当成功: %v", err)
	}
	_ = client.Close()
	if attempts != 2 {
		t.Fatalf("拨号 %d 次，期望 2 次", attempts)
	}
	// 下界不会假红：time.Timer 从不提前触发（超时 100ms + 等待 60ms）。
	if min := 100*time.Millisecond + backoff; time.Since(start) < min {
		t.Errorf("总耗时 %s，小于下界 %s（Backoff 没有真的等待）", time.Since(start), min)
	}
}

// TestDialWithPolicy_HealthyServerSucceedsOnFirstAttempt 健康端一次成功：
// 重拨原语不伤快路径。
func TestDialWithPolicy_HealthyServerSucceedsOnFirstAttempt(t *testing.T) {
	f := newFakeSSH(t)
	attempts := 0
	dialFn := func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error) {
		attempts++
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
	}
	client, err := DialWithPolicy(context.Background(), f.addr(), clientConfig(),
		DialOptions{dialFunc: dialFn}, DialPolicy{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = client.Close()
	if attempts != 1 {
		t.Fatalf("拨号 %d 次，期望 1 次（成功不重拨）", attempts)
	}
}

// TestDialWithPolicy_NilConfigAndContext nil cc / nil ctx 兜底与 Dial 一致。
func TestDialWithPolicy_NilConfigAndContext(t *testing.T) {
	if _, err := DialWithPolicy(context.Background(), "127.0.0.1:1", nil, DialOptions{}, DialPolicy{}); err == nil {
		t.Fatal("nil client config must be rejected")
	}
	// 类型化 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参（#79 同款）。
	var nilCtx context.Context
	if _, err := DialWithPolicy(nilCtx, "127.0.0.1:1", clientConfig(), DialOptions{}, DialPolicy{}); err == nil {
		t.Fatal("expected a dial error to the refused port (nil ctx falls back to Background)")
	}
}
