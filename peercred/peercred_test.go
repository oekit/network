package peercred

import (
	"context"
	"net"
	"testing"
)

// TestPeerTrust_UnknownIsNeverTrusted 核心语义：**取不到凭据 ⇒ 不信任**。
func TestPeerTrust_UnknownIsNeverTrusted(t *testing.T) {
	trust := NewPeerTrust(0, 1000)

	if trust.Trusted(0, false) {
		t.Error("known=false 时不得判为受信（取不到凭据 ≠ 无人可疑）")
	}
	if !trust.Trusted(0, true) {
		t.Error("uid=0 且 known=true 时应受信（受信集合含 0）")
	}
	if !trust.Trusted(1000, true) {
		t.Error("uid=1000 在受信集合里，应受信")
	}
	if trust.Trusted(1001, true) {
		t.Error("uid=1001 不在受信集合里，不得受信")
	}
}

// TestPeerTrust_EmptySetTrustsNothing 空受信集合 ⇒ 一切都不可信。
func TestPeerTrust_EmptySetTrustsNothing(t *testing.T) {
	trust := NewPeerTrust()
	if trust.Trusted(0, true) {
		t.Error("空受信集合不应信任任何人")
	}
	if len(trust.UIDs()) != 0 {
		t.Errorf("空受信集合的 UIDs 应为空，实得 %v", trust.UIDs())
	}
}

// TestPeerTrust_DuplicateUID 重复 uid 无副作用。
func TestPeerTrust_DuplicateUID(t *testing.T) {
	trust := NewPeerTrust(1000, 1000, 1000)
	if !trust.Trusted(1000, true) {
		t.Error("重复登记的 uid 应受信")
	}
	if len(trust.UIDs()) != 1 {
		t.Errorf("UIDs 应去重，实得 %v", trust.UIDs())
	}
}

// TestPeerUIDContext 往返：注入后能取回；未注入时 ok=false。
func TestPeerUIDContext(t *testing.T) {
	if _, ok := PeerUIDFromContext(context.Background()); ok {
		t.Error("未注入时 PeerUIDFromContext 应返回 ok=false")
	}
	ctx := WithPeerUID(context.Background(), 4242)
	uid, ok := PeerUIDFromContext(ctx)
	if !ok || uid != 4242 {
		t.Errorf("PeerUIDFromContext = (%d, %v)，期望 (4242, true)", uid, ok)
	}
}

// TestConnContext_UnknownInjectsZero 非 unix 连接 ⇒ 注入 0（未知）⇒ 按不可信处理。
func TestConnContext_UnknownInjectsZero(t *testing.T) {
	// 一对 TCP 连接（不是 unix socket）⇒ 取不到凭据。
	client, server := tcpPair(t)
	defer func() { _ = client.Close(); _ = server.Close() }()

	ctx := ConnContext(nil)(context.Background(), server)
	uid, ok := PeerUIDFromContext(ctx)
	if ok {
		t.Errorf("非 unix 连接应注入「未知」(ok=false)，实得 uid=%d ok=%v", uid, ok)
	}
	if uid != 0 {
		t.Errorf("未知对端的 uid 应为 0，实得 %d", uid)
	}
}

// TestConnContext_KeepsBase 保留调用方原有的 ConnContext 逻辑。
func TestConnContext_KeepsBase(t *testing.T) {
	type key struct{}
	client, server := tcpPair(t)
	defer func() { _ = client.Close(); _ = server.Close() }()

	ctx := ConnContext(func(ctx context.Context, _ net.Conn) context.Context {
		return context.WithValue(ctx, key{}, "base")
	})(context.Background(), server)

	if v, _ := ctx.Value(key{}).(string); v != "base" {
		t.Errorf("调用方原有的 ConnContext 逻辑被丢了（value=%v）", v)
	}
	if _, ok := PeerUIDFromContext(ctx); ok {
		t.Error("TCP 连接应注入「未知」")
	}
}

// tcpPair 起一对本地 TCP 连接（用于「取不到对端凭据」的形态）。
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	return client, server
}
