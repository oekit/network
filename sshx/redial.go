package sshx

// redial.go 预算内按类重拨（#125，#116 的下半场）。
//
// #116 把「抖动还是判定」分了类（ErrHandshakeTimeout / ErrHandshakeRejected）；
// 本文件给出拿到分类之后的标准答案——下游（jumpserver-exec
// internal/repository/koko/koko.go 的 dialWithHandshakeRetry）手搓过同型逻辑：
//   - 预算内的握手超时重拨：tailscale 链路实测 5s 内 1/4 会话开超时，
//     15s 界 + 一次重拨后 4/4 全过——超时是抖动，重拨一次就过了；
//   - 认证拒绝绝不重拨：那是判定不是抖动——重拨只是多打错凭据、把真错误盖掉。
//
// 适用边界（文档钉死）：重拨的前提是「连接从未建立 = 零投递」——Dial 阶段
// 天然满足：返回错误的 Dial 没有产出任何可用会话，重开一条连接不重放任何
// 已投递的东西。已投递绝不重放的那半边（会话中断重连）不在本原语的范围。
//
// 与 conn.Manager 的分工：conn.Manager 管「长连接断了重连」，本原语管
// 「一次性会话开不上时按类重试」——两者是不同的生命周期。不做连接池、
// 不做长连接管理。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/ssh"
)

// DialPolicy 决定哪类失败可以重拨、拨几次、预算怎么花（#125）。
//
// 零值可用：重拨 1 次、只认 ErrHandshakeTimeout、立即重拨——即「预算内的
// 握手超时重拨一次」这条下游实证出来的默认策略。
type DialPolicy struct {
	// Retries 预算内的重拨次数上限（总尝试 = 1 + Retries）。零值即默认 1。
	Retries int
	// Retryable 判定一次失败是否值得重拨。nil = 默认只认 ErrHandshakeTimeout
	// ——超时是抖动，「连接从未建立」的前提下重拨安全；拒绝类
	// （ErrHandshakeRejected）必须由调用方显式放行才重拨：凭据不对是判定
	// 不是抖动，重拨只是多打错凭据、把真错误盖掉。
	Retryable func(error) bool
	// Backoff 第 attempt 次重拨（从 1 起算）前的等待；nil = 立即重拨。等待
	// 与尝试共享同一份总预算与 ctx 截止时间：等待被夹到剩余预算/截止时间内，
	// 不因重拨放大。
	Backoff func(attempt int) time.Duration
}

// DialWithPolicy = Dial + 预算内按类重拨（#125）。
//
// 总预算（opts.Budget）与 ctx 截止时间**跨所有尝试共享**——每次尝试拿到的
// 是「此刻剩余的预算」，重拨不放大超时。这是本原语必须钉死的一点：预算若
// 按次重置，N 次重拨就是把调用方的等待放大 N 倍的超时放大器。Backoff 的
// 等待同样从这份预算里扣。
//
// 分类从 Dial 原样继承：超时返回 *HandshakeTimeoutError、拒绝返回
// *HandshakeRejectedError（errors.Is / errors.As 判别）。预算在两次尝试之间
// 耗尽时不再发起注定失败的尝试，返回包着最后一次失败的预算错误（类型化
// 错误仍在原因链上）；其余情况最后一次尝试的错误**原样返回**（不重包——
// 调用方的分类逻辑与单次 Dial 同一口径）。
//
// nil cc / nil ctx 的兜底与 Dial 一致。
func DialWithPolicy(ctx context.Context, addr string, cc *ssh.ClientConfig, opts DialOptions, policy DialPolicy) (*ssh.Client, error) {
	if cc == nil {
		return nil, fmt.Errorf("dial %s: no client config", addr)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	retries := policy.Retries
	if retries <= 0 {
		retries = 1
	}
	retryable := policy.Retryable
	if retryable == nil {
		retryable = func(err error) bool { return errors.Is(err, ErrHandshakeTimeout) }
	}

	start := time.Now()
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 && policy.Backoff != nil {
			wait := policy.Backoff(attempt)
			if opts.Budget > 0 {
				if remaining := opts.Budget - time.Since(start); remaining < wait {
					wait = remaining
				}
			}
			if deadline, ok := ctx.Deadline(); ok {
				if remaining := time.Until(deadline); remaining < wait {
					wait = remaining
				}
			}
			if err := sleepRedial(ctx, wait); err != nil {
				// 等待被 ctx 取消/到期打断：带着最后一次失败返回——它才是
				// 调用方要分类的信号；等待本身不产生新的失败面。
				return nil, lastErr
			}
		}
		// 预算在两次尝试之间耗尽：不再发起下一次尝试，把预算事实与最后一次
		// 失败一起交回（%w 保持类型化错误可判）。
		attemptOpts := opts
		if opts.Budget > 0 {
			remaining := opts.Budget - time.Since(start)
			if remaining <= 0 {
				if lastErr == nil {
					return nil, fmt.Errorf("dial %s: redial budget exhausted before the first attempt", addr)
				}
				return nil, fmt.Errorf("dial %s: redial budget exhausted after %d attempt(s) (took %s): %w",
					addr, attempt, time.Since(start).Round(time.Millisecond), lastErr)
			}
			attemptOpts.Budget = remaining
		}
		client, err := Dial(ctx, addr, cc, attemptOpts)
		if err == nil {
			return client, nil
		}
		lastErr = err
		if attempt >= retries || !retryable(err) {
			return nil, err
		}
	}
}

// sleepRedial 可被取消的等待。d <= 0 视为无需等待（预算/截止已把等待夹到零，
// 立即重拨），不算打断。
func sleepRedial(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
