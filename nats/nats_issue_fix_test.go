package nats

// nats_issue_fix_test.go 钉住 #99/#101/#103 的修复：
//   - #99：getOrCreateConsumer 建的消费者必须 durable——临时消费者空闲约
//     5s 被服务端回收，重建后 ack 水位丢失、整条流重投；
//   - #101：Request/RequestJSON 与 JetStream 系列对 nil ctx 兜底
//     Background（与 Publish 系列口径一致），不再 panic；
//   - #103：Fetch/Next 的 ctx 界住取消息阶段，取消不再等 30s 过期。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// replyOnce 起一个 RPC 服务端：收到请求回一条固定字节。
func replyOnce(t *testing.T, c *Client, subject, payload string) {
	t.Helper()
	sub, err := c.Reply(subject, func(msg *nats.Msg) []byte {
		return []byte(payload)
	})
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	unsubscribeOnCleanup(t, sub)
}

// TestRequest_NilContext 回归（#101）：nil ctx 兜底 Background，不再 panic
// "cannot create context from nil parent"。与 Publish 系列同仓口径：同一份
// 客户端对所有 API 容忍 nil ctx。
func TestRequest_NilContext(t *testing.T) {
	c := newTestClient(t)
	replyOnce(t, c, "rpc.echo", "pong")

	// 类型化的 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参，
	// 而 nil 值的 context.Context 变量在运行时就是 nil 接口——回归语义不变。
	var nilCtx context.Context
	resp, err := c.Request(nilCtx, "rpc.echo", []byte("ping"), 3*time.Second)
	if err != nil {
		t.Fatalf("Request(nil ctx): %v", err)
	}
	if string(resp.Data) != "pong" {
		t.Errorf("Request(nil ctx) resp = %q, want %q", resp.Data, "pong")
	}
}

// TestRequestJSON_NilContext 回归（#101）：泛型形态同样兜底。
func TestRequestJSON_NilContext(t *testing.T) {
	c := newTestClient(t)
	replyOnce(t, c, "rpc.json", `{"msg":"pong"}`)

	var nilCtx context.Context
	type payload struct {
		Msg string `json:"msg"`
	}
	resp, err := RequestJSON[payload, payload](nilCtx, c, "rpc.json", &payload{Msg: "ping"}, 3*time.Second)
	if err != nil {
		t.Fatalf("RequestJSON(nil ctx): %v", err)
	}
	if resp.Msg != "pong" {
		t.Errorf("RequestJSON(nil ctx) resp = %+v, want msg=pong", resp)
	}
}

// TestJetStream_NilContext_AllMethods 回归（#101）：JetStream 系列把 nil ctx
// 原样透传给上游 jetstream 时会在 ctx.Deadline() 处 nil 解引用——所有收
// ctx 的公开方法都必须容忍 nil。
func TestJetStream_NilContext_AllMethods(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	var nilCtx context.Context
	const streamName = "TEST_JS_NILCTX_ALL"

	if err = js.CreateStream(nilCtx, streamName, "nilall.js.>"); err != nil {
		t.Fatalf("CreateStream(nil ctx): %v", err)
	}
	if _, err = js.Stream(nilCtx, streamName); err != nil {
		t.Fatalf("Stream(nil ctx): %v", err)
	}
	if _, err = js.StreamInfo(nilCtx, streamName); err != nil {
		t.Fatalf("StreamInfo(nil ctx): %v", err)
	}
	if _, err = js.Publish(nilCtx, "nilall.js.one", []byte("m1")); err != nil {
		t.Fatalf("Publish(nil ctx): %v", err)
	}

	// Fetch（FetchMaxWait 路径）：nil ctx + 显式超时 ⇒ 走「显式超时优先」
	// 的回退分支，同样不得 panic。
	batch, err := js.Fetch(nilCtx, streamName, "c1", 10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch(nil ctx): %v", err)
	}
	got := 0
	for range batch.Messages() {
		got++
	}
	if err = batch.Error(); err != nil {
		t.Errorf("batch.Error: %v", err)
	}
	if got != 1 {
		t.Errorf("Fetch(nil ctx) delivered %d messages, want 1", got)
	}

	// Next：流里还有消息（上面只取一条……本条发布两次），nil ctx 必须能取到。
	if _, err = js.Publish(nilCtx, "nilall.js.one", []byte("m2")); err != nil {
		t.Fatalf("Publish(nil ctx) #2: %v", err)
	}
	msg, err := js.Next(nilCtx, streamName, "c1", jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Next(nil ctx): %v", err)
	}
	if string(msg.Data()) != "m2" {
		t.Errorf("Next(nil ctx) = %q, want %q", msg.Data(), "m2")
	}

	if _, err := js.ConsumerInfo(nilCtx, streamName, "c1"); err != nil {
		t.Fatalf("ConsumerInfo(nil ctx): %v", err)
	}
	if _, err := js.Messages(nilCtx, streamName, "c1"); err != nil {
		t.Fatalf("Messages(nil ctx): %v", err)
	}
	if err := js.DeleteStream(nilCtx, streamName); err != nil {
		t.Fatalf("DeleteStream(nil ctx): %v", err)
	}
}

// TestJetStream_ConsumerIsDurable 回归（#99）：getOrCreateConsumer 建的
// 消费者必须是 durable。只给 Name 时服务端按临时消费者处理（强制
// InactiveThreshold=5s），空闲即被回收；下次调用以默认配置重建 ⇒ ack
// 水位丢失、整条流重投。
func TestJetStream_ConsumerIsDurable(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const streamName, consumerName = "TEST_JS_DURABLE", "durable-c1"
	if err = js.CreateStream(ctx, streamName, "durable.js.>"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if _, err = js.Publish(ctx, "durable.js.one", []byte("m")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Fetch 内部经 getOrCreateConsumer 自动建消费者。
	batch, err := js.Fetch(ctx, streamName, consumerName, 10, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	for range batch.Messages() {
	}

	info, err := js.ConsumerInfo(ctx, streamName, consumerName)
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	if info.Config.Durable != consumerName {
		t.Errorf("consumer Durable = %q, want %q (#99: ephemeral consumers are reaped after ~5s idle)",
			info.Config.Durable, consumerName)
	}
}

// TestJetStream_ConsumerSurvivesIdleGap 回归（#99）的**行为面**：空闲超过
// nats-server 对临时消费者的回收阈值（JsDeleteWaitTimeDefault=5s）后，
// 消费者必须仍在位，且 ack 水位不丢——空闲前已 ack 的消息不再重投。
// 旧实现（临时消费者）在此场景下消费者被删，重建后 DeliverAll ⇒ 两条
// 消息都会再投一遍。
func TestJetStream_ConsumerSurvivesIdleGap(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const streamName, consumerName = "TEST_JS_IDLE_GAP", "idle-c1"
	if err = js.CreateStream(ctx, streamName, "idle.js.>"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if _, err = js.Publish(ctx, "idle.js.one", []byte("first")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// 消费并 ack 第一条。
	batch, err := js.Fetch(ctx, streamName, consumerName, 10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch #1: %v", err)
	}
	n := 0
	for msg := range batch.Messages() {
		n++
		if string(msg.Data()) != "first" {
			t.Errorf("first delivery = %q, want %q", msg.Data(), "first")
		}
		if ackErr := msg.Ack(); ackErr != nil {
			t.Errorf("Ack: %v", ackErr)
			t.Errorf("Ack: %v", err)
		}
	}
	if n != 1 {
		t.Fatalf("Fetch #1 delivered %d messages, want 1", n)
	}

	// 空闲 > 5s：临时消费者在此被服务端删除，durable 不受影响。
	time.Sleep(6 * time.Second)

	// 消费者仍在位（没有被回收重建）。
	info, err := js.ConsumerInfo(ctx, streamName, consumerName)
	if err != nil {
		t.Fatalf("ConsumerInfo after %s idle: %v (consumer was reaped, #99)", 6*time.Second, err)
	}
	if info.Config.Durable != consumerName {
		t.Errorf("consumer Durable = %q after idle, want %q", info.Config.Durable, consumerName)
	}

	// 新消息只投一次——不整条流重投。
	if _, err = js.Publish(ctx, "idle.js.one", []byte("second")); err != nil {
		t.Fatalf("Publish #2: %v", err)
	}
	batch2, err := js.Fetch(ctx, streamName, consumerName, 10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch #2: %v", err)
	}
	seen := 0
	for msg := range batch2.Messages() {
		seen++
		if string(msg.Data()) != "second" {
			t.Errorf("delivery after idle = %q, want %q only (整条流重投 = ack 水位丢失, #99)", msg.Data(), "second")
		}
	}
	if seen != 1 {
		t.Errorf("Fetch #2 delivered %d messages, want 1 (重投 = #99 症状)", seen)
	}
}

// TestJetStream_Fetch_RespectsContextCancellation 回归（#103）：ctx 取消
// 必须终止取消息阶段，而不是挂在 SDK 的 DefaultExpires（30s）上。
func TestJetStream_Fetch_RespectsContextCancellation(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const streamName, consumerName = "TEST_JS_FETCH_CANCEL", "fetch-c1"
	if err = js.CreateStream(ctx, streamName, "fetchcancel.js.>"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// 流里没有消息；ctx 300ms 后到期。取消息阶段必须跟着终止。
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	start := time.Now()
	batch, err := js.Fetch(fetchCtx, streamName, consumerName, 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancelFetch()
	}()
	for range batch.Messages() {
	}
	berr := batch.Error()
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Fetch returned after %s; ctx cancellation must not wait the SDK's 30s expiry (#103)", elapsed)
	}
	if berr == nil {
		t.Error("batch.Error = nil, want the cancellation reason")
	} else if !errors.Is(berr, context.Canceled) {
		t.Errorf("batch.Error = %v, want context.Canceled", berr)
	}
}

// TestJetStream_Next_RespectsContextCancellation 回归（#103）：同 Fetch 的
// 单条形态。
func TestJetStream_Next_RespectsContextCancellation(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const streamName, consumerName = "TEST_JS_NEXT_CANCEL", "next-c1"
	if err = js.CreateStream(ctx, streamName, "nextcancel.js.>"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// ctx 300ms 后到期；Next 必须在到期时立即返回。
	nextCtx, cancelNext := context.WithCancel(ctx)
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancelNext()
	}()

	start := time.Now()
	_, err = js.Next(nextCtx, streamName, consumerName)
	if err == nil {
		t.Fatal("Next returned nil error with no message and a cancelled ctx")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Next returned after %s; ctx cancellation must not wait the SDK's 30s expiry (#103)", elapsed)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Next err = %v, want context.Canceled", err)
	}
}

// TestJetStream_Fetch_NilContext_WithFetchMaxWait 钉住 #103 的回退口径：
// 调用方显式给了 FetchMaxWait 时（与 FetchContext 互斥，SDK 硬限制），以
// 显式超时为准回退——不报错、不挂 30s。
func TestJetStream_Fetch_NilContext_WithFetchMaxWait(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const streamName, consumerName = "TEST_JS_MAXWAIT", "maxwait-c1"
	if err = js.CreateStream(ctx, streamName, "maxwait.js.>"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if _, err = js.Publish(ctx, "maxwait.js.one", []byte("m")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	start := time.Now()
	batch, err := js.Fetch(ctx, streamName, consumerName, 10, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch with FetchMaxWait: %v", err)
	}
	n := 0
	for range batch.Messages() {
		n++
	}
	if err = batch.Error(); err != nil {
		t.Errorf("batch.Error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Fetch with FetchMaxWait(500ms) took %s; the explicit timeout must win", elapsed)
	}
	if n != 1 {
		t.Errorf("delivered %d messages, want 1", n)
	}
}

// TestRequest_NotConnected_WordsError 兜底检查：ensureCtx 不改变错误语义
// （错误包装仍带 subject）。
func TestRequest_NotConnected_WordsError(t *testing.T) {
	c := &Client{} // 未连接：conn 为 nil
	_, err := c.Request(context.Background(), "rpc.x", []byte("p"), time.Second)
	if err == nil {
		t.Fatal("expected an error on a disconnected client")
	}
	if !strings.Contains(err.Error(), "rpc.x") {
		t.Errorf("error does not name the subject: %v", err)
	}
}
