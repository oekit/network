package nats

// nats_handler_panic_identity_test.go 钉住 #180：msg handler（core NATS 与
// JetStream 双路）的 panic 收编必须走 pkg/errors.Recovered 单源——此前
// fmt.Sprintf("%v", r) 把 panic 值拍平成纯文本：无栈、无类型信息、error
// 身份丢失（handler 以 panic(err) 失败时 errors.Is/As 无法穿透归因），
// 与同文件生命周期收编（recoverCallback，#171/#182 单源）两套形态并存。
//
// 断言口径（对齐 timertask #173 的行为面钉法）：panic(err) 的 handler →
// 日志 logx.ErrKey 字段上的 error 可 errors.Is 原始错误、errors.As 取回
// *PanicError 且栈在场。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/oekit/logx"
	pkgerrors "github.com/oekit/pkg/errors"
)

// capturedRecord 是一条被捕获的结构化日志。
type capturedRecord struct {
	level logx.Level
	msg   string
	kv    []any
}

// attr 按键取 kv 对里的值（kv 为交替 key/value 序列）。
func (r capturedRecord) attr(key string) (any, bool) {
	for i := 0; i+1 < len(r.kv); i += 2 {
		if k, ok := r.kv[i].(string); ok && k == key {
			return r.kv[i+1], true
		}
	}
	return nil, false
}

// identityCaptureLogger 捕获结构化日志的 logx.Logger 测试替身（形态对齐
// etcdx/distlock 的 captureLogger）：Debug/Info/Warn/Error 记录到内存，
// 其余方法经内嵌 logx.Nop 空转；With* 返回自身——nats 包 logger 是
// logx.Delegating("nats")，解析链 Default().WithAttrs(component) 后仍落回
// 本捕获器，不丢记录。
type identityCaptureLogger struct {
	logx.Logger

	mu      sync.Mutex
	records []capturedRecord
}

func newIdentityCaptureLogger() *identityCaptureLogger {
	return &identityCaptureLogger{Logger: logx.Nop()}
}

func (c *identityCaptureLogger) add(level logx.Level, msg string, kv []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, capturedRecord{level: level, msg: msg, kv: kv})
}

func (c *identityCaptureLogger) Debug(msg string, kv ...any) { c.add(logx.LevelDebug, msg, kv) }
func (c *identityCaptureLogger) Info(msg string, kv ...any)  { c.add(logx.LevelInfo, msg, kv) }
func (c *identityCaptureLogger) Warn(msg string, kv ...any)  { c.add(logx.LevelWarn, msg, kv) }
func (c *identityCaptureLogger) Error(msg string, kv ...any) { c.add(logx.LevelError, msg, kv) }

func (c *identityCaptureLogger) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *identityCaptureLogger) WithGroup(string) logx.Logger        { return c }
func (c *identityCaptureLogger) WithService(string) logx.Logger      { return c }
func (c *identityCaptureLogger) WithCtx(context.Context) logx.Logger { return c }
func (c *identityCaptureLogger) SetLevel(logx.Level)                 {}

// find 返回首条 level 与 msg 子串同时匹配的记录。
func (c *identityCaptureLogger) find(level logx.Level, msgSubstring string) (capturedRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.level == level && strings.Contains(r.msg, msgSubstring) {
			return r, true
		}
	}
	return capturedRecord{}, false
}

// swapIdentityLogger 临时把全局默认 logger 换成捕获替身，返回还原函数。
// nats 包 logger 走 logx.Delegating 懒解析，替换 Default() 即可捕获。
func swapIdentityLogger(t *testing.T) *identityCaptureLogger {
	t.Helper()
	cap := newIdentityCaptureLogger()
	prev := logx.Default()
	logx.SetDefault(cap)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return cap
}

// assertPanicErrIdentity 对收编日志的 logx.ErrKey 字段做身份断言：
// 值必须是 error、errors.Is 穿透到原始 sentinel、errors.As 取回 *PanicError
// 且 panic 点栈在场。
func assertPanicErrIdentity(t *testing.T, rec capturedRecord, sentinel error) {
	t.Helper()
	v, ok := rec.attr(logx.ErrKey)
	if !ok {
		t.Fatalf("收编日志缺 %s 字段（AGENTS.md 日志红线）: kv=%v", logx.ErrKey, rec.kv)
	}
	err, ok := v.(error)
	if !ok {
		t.Fatalf("%s 字段值类型 = %T，want error——panic 值未走 pkg/errors.Recovered 收编", logx.ErrKey, v)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(err, sentinel) = false——error 身份在收编中断链: %v", err)
	}
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("errors.As(*PanicError) = false, got %T——非 Recovered 单源产物", err)
	}
	if len(perr.Stack) == 0 {
		t.Error("PanicError.Stack 为空——panic 点栈必须在场（可排障）")
	}
}

// TestSafeMsgHandler_PanicErrorIdentityInLog 回归（#180）：core NATS 路
// （Subscribe/QueueSubscribe/Reply 共用的 safeMsgHandler）以 panic(err)
// 失败的 handler，收编日志的 err 字段必须保留 error 身份与栈。
func TestSafeMsgHandler_PanicErrorIdentityInLog(t *testing.T) {
	cap := swapIdentityLogger(t)

	sentinel := errors.New("nats handler biz boom")
	wrapped := safeMsgHandler("panic.identity", func(*nats.Msg) {
		panic(sentinel)
	})
	// handler 不读 msg，直接喂零值即可——本用例只钉收编转换，不走网络。
	wrapped(&nats.Msg{Subject: "panic.identity"})

	rec, ok := cap.find(logx.LevelError, "handler panic recovered")
	if !ok {
		t.Fatal("未捕获到 handler panic recovered 的 Error 日志——收编路径未触发")
	}
	assertPanicErrIdentity(t, rec, sentinel)
}

// TestSafeJetMsgHandler_PanicErrorIdentityInLog 回归（#180）：JetStream 路
// （Consume 共用的 safeJetMsgHandler）同口径——#166 补齐时仍是
// fmt.Sprintf("%v") 拍平形态，本条与 core NATS 路一起钉到单源。
func TestSafeJetMsgHandler_PanicErrorIdentityInLog(t *testing.T) {
	cap := swapIdentityLogger(t)

	sentinel := errors.New("jetstream handler biz boom")
	wrapped := safeJetMsgHandler("S", "C", func(jetstream.Msg) {
		panic(sentinel)
	})
	wrapped(nil)

	rec, ok := cap.find(logx.LevelError, "jetstream handler panic recovered")
	if !ok {
		t.Fatal("未捕获到 jetstream handler panic recovered 的 Error 日志——收编路径未触发")
	}
	assertPanicErrIdentity(t, rec, sentinel)
}

// TestRecoverMsgHandler_NoPanicReturnsNil 钉住零开销承诺：无 panic 时
// recoverMsgHandler 返回 nil（不产生包装错误），safeMsgHandler 据此跳过
// 日志——正常 handler 不得被误标为 panic。
func TestRecoverMsgHandler_NoPanicReturnsNil(t *testing.T) {
	if err := recoverMsgHandler(func() {}); err != nil {
		t.Fatalf("无 panic 时应返回 nil, got %v", err)
	}
	var called bool
	if err := recoverMsgHandler(func() { called = true }); err != nil || !called {
		t.Fatalf("正常执行应透传且返回 nil (called=%v, err=%v)", called, err)
	}
}
