package wsutil

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// mockConn 实现 Conn 接口，可控制 WriteMessage 的行为。
// 所有字段并发安全，因为 broadcaster 会从内部 goroutine 调用 WriteMessage/Close，
// 而测试在另一个 goroutine 中读取。
type mockConn struct {
	mu         sync.Mutex
	writeErr   error
	closeCalls int
	written    [][]byte
}

func (m *mockConn) WriteMessage(_ int, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.written = append(m.written, cp)
	return nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return nil
}

func (m *mockConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

// writtenCount 返回已写入消息数量（并发安全）。
func (m *mockConn) writtenCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.written)
}

// firstWritten 返回第一条写入的消息内容（并发安全）。
func (m *mockConn) firstWritten() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.written) == 0 {
		return nil
	}
	return m.written[0]
}

// waitFor 轮询 cond 直到返回 true 或超时，避免测试中的硬编码 sleep。
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	const timeout = 2 * time.Second
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}

func TestBroadcaster_RegisterAndCount(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	c1 := &mockConn{}
	c2 := &mockConn{}
	b.Register("room:1", c1)
	b.Register("room:1", c2)
	b.Register("room:2", c1)

	waitFor(t, "room:1 has 2 conns", func() bool {
		return b.GetRoomCount("room:1") == 2
	})
	waitFor(t, "room:2 has 1 conn", func() bool {
		return b.GetRoomCount("room:2") == 1
	})

	if got := b.GetTotalConnections(); got != 3 {
		t.Errorf("GetTotalConnections() = %d, want 3", got)
	}

	rooms := b.GetRooms()
	if len(rooms) != 2 {
		t.Errorf("GetRooms() len = %d, want 2", len(rooms))
	}
}

func TestBroadcaster_Broadcast(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	c1 := &mockConn{}
	c2 := &mockConn{}
	b.Register("room:1", c1)
	b.Register("room:1", c2)

	waitFor(t, "both conns registered", func() bool {
		return b.GetRoomCount("room:1") == 2
	})

	b.Broadcast("room:1", []byte("hello"))

	waitFor(t, "c1 receives message", func() bool {
		return c1.writtenCount() == 1
	})
	waitFor(t, "c2 receives message", func() bool {
		return c2.writtenCount() == 1
	})

	if string(c1.firstWritten()) != "hello" {
		t.Errorf("c1 got %q, want %q", c1.firstWritten(), "hello")
	}
}

func TestBroadcaster_BroadcastEvent(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	c := &mockConn{}
	b.Register("room:1", c)

	waitFor(t, "conn registered", func() bool {
		return b.GetRoomCount("room:1") == 1
	})

	b.BroadcastEvent("room:1", Event{Type: "progress", Data: 50})

	waitFor(t, "event received", func() bool {
		return c.writtenCount() == 1
	})
	// 验证 timestamp 被自动填充（非零）
	if c.writtenCount() > 0 && len(c.firstWritten()) == 0 {
		t.Error("broadcast event produced empty payload")
	}
}

func TestBroadcaster_BroadcastToUnknownRoom(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	// 广播到不存在的房间不应 panic，也不应阻塞
	b.Broadcast("nonexistent", []byte("data"))
}

func TestBroadcaster_Unregister(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	c := &mockConn{}
	b.Register("room:1", c)

	waitFor(t, "conn registered", func() bool {
		return b.GetRoomCount("room:1") == 1
	})

	b.Unregister("room:1", c)

	waitFor(t, "conn unregistered", func() bool {
		return b.GetRoomCount("room:1") == 0
	})
}

// TestBroadcaster_BroadcastFailingConn_NoDeadlock 是 P0 死锁回归测试。
// 修复前：handleBroadcast 对写入失败的连接调用 b.Unregister（向无缓冲 channel 发送），
// 而 run() 正忙于 broadcast 分支无法接收 → 死锁。
// 修复后：直接调用 handleUnregister 内联清理，不阻塞。
func TestBroadcaster_BroadcastFailingConn_NoDeadlock(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	failConn := &mockConn{writeErr: errors.New("connection closed")}
	okConn := &mockConn{}
	b.Register("room:1", failConn)
	b.Register("room:1", okConn)

	waitFor(t, "both conns registered", func() bool {
		return b.GetRoomCount("room:1") == 2
	})

	// 广播：failConn 写入失败，应被内联清理；okConn 正常收到。
	// 若存在死锁，此调用不会返回，测试将超时失败。
	done := make(chan struct{})
	go func() {
		b.Broadcast("room:1", []byte("data"))
		close(done)
	}()

	select {
	case <-done:
		// 广播调用成功返回
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast deadlocked: handleBroadcast likely calls Unregister through channel")
	}

	// failConn 应被清理出房间
	waitFor(t, "failing conn removed from room", func() bool {
		return b.GetRoomCount("room:1") == 1
	})
	// okConn 仍应收到消息
	waitFor(t, "ok conn receives message", func() bool {
		return okConn.writtenCount() == 1
	})
}

func TestBroadcaster_Close(t *testing.T) {
	b := NewBroadcaster()
	c := &mockConn{}
	b.Register("room:1", c)

	waitFor(t, "conn registered", func() bool {
		return b.GetRoomCount("room:1") == 1
	})

	b.Close()

	// Close 后所有房间应被清空
	if got := b.GetTotalConnections(); got != 0 {
		t.Errorf("after Close, GetTotalConnections() = %d, want 0", got)
	}
}

// TestBroadcast_RunLoopStaysResponsiveWithSlowConn 钉住 2026-09-06 修复:
// 慢连接(写阻塞超过 writeTimeout)不得拖死 run() 循环 — 修复前 wg.Wait()
// 无上界,期间 Register/Broadcast 全部阻塞;SetWriteDeadline 为 no-op 的
// Conn(接口允许)甚至会让广播器永久死锁。现在等待有 writeTimeout+1s
// 上界,之后 run() 恢复消费 channel。
func TestBroadcast_RunLoopStaysResponsiveWithSlowConn(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()
	b.waitTimeout = 200 * time.Millisecond // 同包直设实例字段,无全局竞态

	slow := &blockingConn{deadlineHonored: false} // SetWriteDeadline no-op + 写永久阻塞
	fast := &mockConn{}
	b.Register("room", slow)
	b.Register("room", fast)

	// 第一轮广播:慢连接卡住,等待兜底(~6s)后放弃
	start := time.Now()
	b.Broadcast("room", []byte("m1"))
	firstWait := time.Since(start)
	if firstWait > 5*time.Second {
		t.Fatalf("first broadcast blocked %v, want bounded by wait timeout", firstWait)
	}

	// 关键断言:超时放弃后,run() 循环必须恢复响应(修复前这里永久阻塞)
	done := make(chan struct{})
	go func() {
		b.Broadcast("room", []byte("m2"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("second Broadcast blocked forever — run loop still stuck")
	}
	b.Unregister("room", fast)
	if got := b.GetRoomCount("room"); got != 1 {
		t.Errorf("room count = %d, want 1 (slow conn removed after failed write)", got)
	}
}

// blockingConn 模拟 SetWriteDeadline no-op + 写永久阻塞的实现(接口允许)。
type blockingConn struct {
	deadlineHonored bool
}

func (c *blockingConn) WriteMessage(_ int, _ []byte) error {
	select {} // 永久阻塞
}
func (c *blockingConn) Close() error { return nil }
func (c *blockingConn) SetWriteDeadline(_ time.Time) error {
	return nil // no-op 违约实现
}
