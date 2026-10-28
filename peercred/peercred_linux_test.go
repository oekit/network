//go:build linux

package peercred

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestPeerUID_UnixSocket 真实 unix socket：对端凭据必须取得到，且等于
// **连接发起方**的 uid（SO_PEERCRED 报的是 peer 的凭据）。
//
// 注意方向：服务端 accept 到的连接上取到的 uid 是**客户端**的 uid；
// 本测试两端都在同一进程 ⇒ uid 相同 ⇒ 用进程自身的 uid 做断言。
func TestPeerUID_UnixSocket(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "t.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close(); _ = os.Remove(sock) }()

	type result struct {
		uid uint32
		ok  bool
		err error
	}
	got := make(chan result, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		uid, ok := PeerUID(conn)
		got <- result{uid: uid, ok: ok}
	}()

	client, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	res := <-got
	if res.err != nil {
		t.Fatalf("accept: %v", res.err)
	}
	if !res.ok {
		t.Fatal("unix socket 的对端凭据取不到（SO_PEERCRED 应可用）")
	}

	self, err := strconv.Atoi(os.Getenv("UID"))
	if err != nil {
		// 容器里可能没有 UID 环境变量 ⇒ 直接读 /proc/self/status 的 Uid。
		self = procSelfUID(t)
	}
	if int(res.uid) != self {
		t.Errorf("取到的对端 uid = %d，期望 %d（对端就是本进程）", res.uid, self)
	}
}

// TestPeerUID_NonUnixConn 非 unix 连接 ⇒ 取不到 ⇒ ok=false（不静默放行）。
func TestPeerUID_NonUnixConn(t *testing.T) {
	client, server := tcpPair(t)
	defer func() { _ = client.Close(); _ = server.Close() }()
	if uid, ok := PeerUID(server); ok {
		t.Errorf("TCP 连接不应取到对端凭据（uid=%d ok=%v）", uid, ok)
	}
}

// TestConnContext_UnixSocketInjectsUID 真实 unix socket ⇒ ctx 里应能取到 uid。
func TestConnContext_UnixSocketInjectsUID(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "c.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close(); _ = os.Remove(sock) }()

	ctxCh := make(chan context.Context, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		ctxCh <- ConnContext(nil)(context.Background(), conn)
	}()

	client, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	select {
	case ctx := <-ctxCh:
		uid, ok := PeerUIDFromContext(ctx)
		if !ok {
			t.Fatal("unix socket 的 uid 未注入 ctx")
		}
		if uid == 0 && os.Geteuid() != 0 {
			t.Errorf("注入的 uid=%d，期望非 root 的本进程 uid", uid)
		}
	case <-context.Background().Done():
		t.Fatal("unreachable")
	}
}

// procSelfUID 从 /proc/self/status 读本进程 uid（容器里未必有 UID 环境变量）。
func procSelfUID(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatalf("read /proc/self/status: %v", err)
	}
	for _, line := range splitLines(string(b)) {
		if len(line) > 4 && line[:4] == "Uid:" {
			fields := splitFields(line[4:])
			if len(fields) == 0 {
				break
			}
			n, err := strconv.Atoi(fields[0])
			if err != nil {
				break
			}
			return n
		}
	}
	t.Fatalf("/proc/self/status 里没找到 Uid 行")
	return 0
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func splitFields(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
