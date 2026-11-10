package nats

// nats_handler_panic_test.go 钉住 #145 的修复：订阅 handler 的 panic 必须就地
// 收编——handler 跑在 nats.go 的内部派发 goroutine（waitForMsgs）里，上游 SDK
// 的派发循环直接 mcb(m)、没有任何 recover（nats.go@v1.55.0/nats.go:3861），
// 用户 handler 一个 panic 就穿透到 goroutine 顶，整个进程连同其上所有订阅、
// 所有在途请求一并消灭。SubscribeJSON 连坏 JSON 都兜了底（log 后跳过），
// 唯独更常见的 handler 内 panic 没兜。
//
// 断言口径（panic 类）：panic 后进程/订阅仍存活且后续消息照常投递。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestSubscribe_HandlerPanicDoesNotKillProcess 回归（#145）：裸 Subscribe 的
// handler panic 不得穿透 nats.go 的 waitForMsgs 打死进程；同一条订阅必须存活，
// 后续消息照常投递。
func TestSubscribe_HandlerPanicDoesNotKillProcess(t *testing.T) {
	c := newTestClient(t)

	var boomSeen atomic.Int32
	got := make(chan string, 2)
	sub, err := c.Subscribe("panic.raw", func(m *nats.Msg) {
		if string(m.Data) == "boom" {
			boomSeen.Add(1)
			panic("handler panic (simulated)")
		}
		got <- string(m.Data)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	// 先发一条会 panic 的消息，再发一条正常的：同一订阅按序派发，
	// 正常消息到达即证明「panic 已被收编、订阅仍存活」。
	if err := c.Publish(context.Background(), "panic.raw", []byte("boom")); err != nil {
		t.Fatalf("Publish boom: %v", err)
	}
	if err := c.Publish(context.Background(), "panic.raw", []byte("ok")); err != nil {
		t.Fatalf("Publish ok: %v", err)
	}

	select {
	case data := <-got:
		if data != "ok" {
			t.Errorf("got %q, want %q", data, "ok")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the message after the panicking one — the subscription died with the handler panic (#145)")
	}
	if n := boomSeen.Load(); n != 1 {
		t.Errorf("boom handler invoked %d times, want 1 (the panic must actually have happened)", n)
	}
}

// TestSubscribeJSON_HandlerPanicDoesNotKillProcess 回归（#145）的主诉入口：
// SubscribeJSON 对坏 JSON 特意做了「log 后跳过」的健壮性，唯独 handler 内
// panic（JSON 反序列化成功但语义残缺、nil 解引用）没兜。panic 后订阅存活。
func TestSubscribeJSON_HandlerPanicDoesNotKillProcess(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Name string `json:"name"`
	}
	var boomSeen atomic.Int32
	got := make(chan string, 2)
	sub, err := SubscribeJSON[payload](c, "panic.json", func(ctx context.Context, msg *payload) {
		if msg.Name == "boom" {
			boomSeen.Add(1)
			panic("handler panic (simulated)")
		}
		got <- msg.Name
	})
	if err != nil {
		t.Fatalf("SubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.PublishJSON(context.Background(), "panic.json", payload{Name: "boom"}); err != nil {
		t.Fatalf("PublishJSON boom: %v", err)
	}
	if err := c.PublishJSON(context.Background(), "panic.json", payload{Name: "ok"}); err != nil {
		t.Fatalf("PublishJSON ok: %v", err)
	}

	select {
	case name := <-got:
		if name != "ok" {
			t.Errorf("got %q, want %q", name, "ok")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the message after the panicking one — the subscription died with the handler panic (#145)")
	}
	if n := boomSeen.Load(); n != 1 {
		t.Errorf("boom handler invoked %d times, want 1 (the panic must actually have happened)", n)
	}
}

// TestQueueSubscribeJSON_HandlerPanicDoesNotKillProcess 回归（#145）：队列
// 订阅漏斗（QueueSubscribe → 上游 QueueSubscribe）同口径收编。
func TestQueueSubscribeJSON_HandlerPanicDoesNotKillProcess(t *testing.T) {
	c := newTestClient(t)

	type payload struct {
		Name string `json:"name"`
	}
	var boomSeen atomic.Int32
	got := make(chan string, 2)
	sub, err := QueueSubscribeJSON[payload](c, "panic.queue", "g", func(ctx context.Context, msg *payload) {
		if msg.Name == "boom" {
			boomSeen.Add(1)
			panic("handler panic (simulated)")
		}
		got <- msg.Name
	})
	if err != nil {
		t.Fatalf("QueueSubscribeJSON: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	if err := c.PublishJSON(context.Background(), "panic.queue", payload{Name: "boom"}); err != nil {
		t.Fatalf("PublishJSON boom: %v", err)
	}
	if err := c.PublishJSON(context.Background(), "panic.queue", payload{Name: "ok"}); err != nil {
		t.Fatalf("PublishJSON ok: %v", err)
	}

	select {
	case name := <-got:
		if name != "ok" {
			t.Errorf("got %q, want %q", name, "ok")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the message after the panicking one — the subscription died with the handler panic (#145)")
	}
	if n := boomSeen.Load(); n != 1 {
		t.Errorf("boom handler invoked %d times, want 1 (the panic must actually have happened)", n)
	}
}

// TestReply_HandlerPanicDoesNotKillProcess 回归（#145）：Reply 的 RPC handler
// panic 同样就地收编——它是外部不可信载荷触发的另一形态（handler 返回前
// panic），进程与后续请求存活。
func TestReply_HandlerPanicDoesNotKillProcess(t *testing.T) {
	c := newTestClient(t)

	var boomSeen atomic.Int32
	sub, err := c.Reply("panic.rpc", func(msg *nats.Msg) []byte {
		if string(msg.Data) == "boom" {
			boomSeen.Add(1)
			panic("handler panic (simulated)")
		}
		return []byte("pong")
	})
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	unsubscribeOnCleanup(t, sub)

	// boom 请求：handler panic ⇒ 无应答（超时是预期的），进程必须活着。
	if _, err := c.Request(context.Background(), "panic.rpc", []byte("boom"), 300*time.Millisecond); err == nil {
		t.Error("the panicking request unexpectedly got a reply")
	}

	// 后续请求照常应答：订阅与进程存活。
	resp, err := c.Request(context.Background(), "panic.rpc", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("the request after the panicking one failed — the process/subscription died with the handler panic (#145): %v", err)
	}
	if string(resp.Data) != "pong" {
		t.Errorf("resp = %q, want %q", resp.Data, "pong")
	}
	if n := boomSeen.Load(); n != 1 {
		t.Errorf("boom handler invoked %d times, want 1 (the panic must actually have happened)", n)
	}
}
