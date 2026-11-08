package conn

import (
	"errors"
	"math"
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

// TestManager_PutWhileDisconnectedDoesNotBlockRecovery 钉住 #81/#84 指名的
// 路径：初始拨号失败后 conn==nil，调用方 Put(err) 因 `err != nil && conn != nil`
// 走不进重连分支——#64 修复前 reconnectc 从未排定，这一路径直接导致 Manager
// 永久瘫痪（Take 恒 nil，服务端恢复也不自愈）。#64 之后重连在初始失败时即已
// 排定；本测试钉住「失联期间的 Put(err) 必须是无害 no-op——不吞掉、不推迟
// 已排定的重连」，服务端恢复后 Take 必须能拿到连接。
func TestManager_PutWhileDisconnectedDoesNotBlockRecovery(t *testing.T) {
	var attempts atomic.Int32
	after := make(chan time.Time)
	m := NewManager(func(network, address string) (net.Conn, error) {
		if attempts.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return &mockConn{}, nil
	}, "tcp", "127.0.0.1:1", func(time.Duration) <-chan time.Time { return after })

	// feeder 无限供给退避触发（而非固定次数）：若重连恰好落在「Take 返回
	// nil 之后、Put(err) 送达之前」，Put 会按契约关闭这条新连接并再触发
	// 一次重连——固定次数的 feeder 会被烧光，测试就假死了。
	feederDone := make(chan struct{})
	defer close(feederDone)
	go func() {
		for {
			select {
			case after <- time.Now():
			case <-feederDone:
				return
			}
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
		// #81/#84 指名路径：conn==nil 时调用方报告错误。conn 为 nil ⇒
		// 走不进重连分支，但也绝不能干扰初始失败时已排定的退避重连。
		m.Put(errors.New("still down"))
	}
	m.Stop()
	t.Fatalf("失联期间 Put(err) 后再未重连成功（attempts=%d）——#81/#84 回归", attempts.Load())
}

// TestManager_StopDuringReconnectDial_ClosesDialedConn 钉住 #98：Stop 发生
// 在「重连已拨通、尚未交付」的窗口里时，那条连接必须被 Close。
//
// 带缓冲的 connc（cap=1）下，Stop 之后 `connc <- nc` 与 `<-m.stopc` 同时
// 就绪，select 随机挑一个——挑中 send 就把连接塞进再无人消费的缓冲区，
// 约 50% 概率永不 Close（fd 泄漏）。无缓冲 connc 下 send 只能与 loop 的
// 接收会合：loop 已退出 ⇒ send 永不就绪 ⇒ 必走 stopc 分支就地 Close。
// 30 轮统计性复现：旧实现约一半轮次泄漏。
func TestManager_StopDuringReconnectDial_ClosesDialedConn(t *testing.T) {
	const rounds = 30
	// after 每次返回一个已触发的通道：Put 报错后重连立即起跑。
	after := func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	for i := 0; i < rounds; i++ {
		var calls atomic.Int32
		dialed := make(chan *mockConn, 1)
		release := make(chan struct{})
		m := NewManager(func(network, address string) (net.Conn, error) {
			if calls.Add(1) == 1 {
				return &mockConn{}, nil // 初始连接成功
			}
			// 重连路径：拨通一条新连接，挂起在「已拨通、未交付」的窗口里
			nc := &mockConn{}
			dialed <- nc
			<-release
			return nc, nil
		}, "tcp", "127.0.0.1:1", after)

		if c := m.Take(); c == nil {
			t.Fatalf("round %d: expected the initial conn", i)
		}
		// 报告连接错误 ⇒ 关闭当前连接并触发重连；等拨号真正拨通。
		m.Put(errors.New("broken"))
		nc := <-dialed

		// Stop 落在窗口里，然后放行拨号——此刻 goroutine 的投递 select
		// 两个分支同时就绪（旧实现在此随机泄漏）。
		m.Stop()
		close(release)

		deadline := time.Now().Add(2 * time.Second)
		for !nc.closed.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: the conn dialed during Stop was never closed (fd leak, #98)", i)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// TestExponential_NeverNegativeOrAboveCap 钉住 #102：导出函数对任何 int64
// 输入都返回 (0, 1 分钟] 的退避——旧实现在输入接近 MaxInt64 时 `d *= 2`
// 回绕成负数，负数躲过「> time.Minute」的钳制直接漏出，交给 time.After
// 等价于无退避的立即重连。
func TestExponential_NeverNegativeOrAboveCap(t *testing.T) {
	inputs := []time.Duration{
		time.Nanosecond,
		time.Second,
		30 * time.Second,
		time.Minute,
		time.Hour,
		math.MaxInt64,
		math.MinInt64,
		-1,
		0,
	}
	for _, in := range inputs {
		for i := 0; i < 500; i++ {
			d := Exponential(in)
			if d <= 0 {
				t.Fatalf("Exponential(%v) = %v at iter %d, want a positive backoff (#102)", in, d, i)
			}
			if d > time.Minute {
				t.Fatalf("Exponential(%v) = %v at iter %d, want ≤ 1 minute (#102)", in, d, i)
			}
		}
	}
}
