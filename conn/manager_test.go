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
