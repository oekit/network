package conn

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type mockConn struct {
	closed atomic.Bool
}

func (m *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (m *mockConn) Write(b []byte) (int, error)        { return len(b), nil }
func (m *mockConn) Close() error                       { m.closed.Store(true); return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestManager_Take(t *testing.T) {
	conn := &mockConn{}
	m := NewManager(
		func(network, address string) (net.Conn, error) { return conn, nil },
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return nil },
	)

	c := m.Take()
	if c == nil {
		t.Fatal("expected non-nil conn")
	}
}

func TestManager_Put_NilIsNoop(t *testing.T) {
	conn := &mockConn{}
	m := NewManager(
		func(network, address string) (net.Conn, error) { return conn, nil },
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return nil },
	)

	m.Take()
	m.Put(nil) // should not reconnect

	c := m.Take()
	if c == nil {
		t.Error("expected conn still available after nil Put")
	}
}

func TestManager_Put_ErrorTriggersReconnect(t *testing.T) {
	callCount := 0
	m := NewManager(
		func(network, address string) (net.Conn, error) {
			callCount++
			return &mockConn{}, nil
		},
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		},
	)

	c := m.Take()
	if c == nil {
		t.Fatal("expected conn")
	}

	m.Put(errors.New("connection broken"))
	time.Sleep(100 * time.Millisecond) // wait for reconnect

	c2 := m.Take()
	if c2 == nil {
		t.Error("expected reconnected conn")
	}
	if callCount < 2 {
		t.Errorf("expected at least 2 dial calls, got %d", callCount)
	}
}

func TestManager_Write(t *testing.T) {
	conn := &mockConn{}
	m := NewManager(
		func(network, address string) (net.Conn, error) { return conn, nil },
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return nil },
	)

	n, err := m.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}
}

func TestExponential(t *testing.T) {
	d := Exponential(time.Second)
	if d < time.Second || d > 3*time.Second {
		t.Errorf("expected ~2s with jitter, got %v", d)
	}

	d = Exponential(time.Minute)
	if d > time.Minute {
		t.Errorf("expected capped at 1 minute, got %v", d)
	}
}

// TestManager_InitialDialFailure_Retries 钉住 #64：初始拨号失败后必须排定
// 退避重连——依赖端短暂不可达（服务启动最常见场景）后恢复，Manager 必须自愈；
// 修复前初始失败 ⇒ reconnectc 永不排定 ⇒ 拨号次数恒 1，永不重试。
func TestManager_InitialDialFailure_Retries(t *testing.T) {
	var attempts atomic.Int32
	after := make(chan time.Time)
	m := NewManager(func(network, address string) (net.Conn, error) {
		if attempts.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return &mockConn{}, nil
	}, "tcp", "127.0.0.1:1", func(time.Duration) <-chan time.Time { return after })

		// 初始失败（attempts=1）⇒ 退避排定；触发重连①失败（attempts=2）⇒ 再排定；
	// 触发重连②成功（attempts=3）⇒ Take 拿到连接
	go func() {
		for i := 0; i < 4; i++ {
			after <- time.Now()
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c := m.Take(); c != nil {
			c.Close()
			if got := attempts.Load(); got < 3 {
				t.Fatalf("成功前拨号次数应 ≥3, got %d", got)
			}
			m.Stop()
			return
		}
	}
	m.Stop()
	t.Fatalf("初始失败后从未重连成功（attempts=%d）——#64 回归", attempts.Load())
}
