package conn

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestManager_ReconnectNotLostByStaleConn 钉住「Put(err) 排定的重连不许被
// 旁路抹掉」。
//
// **背景（2026-10-29）**：CI 上 TestManager_Loop_Reconnect 偶发红
// （"expected at least 2 dials (initial + reconnect), got 1"），本地恒绿。
// 根因在 Manager.loop()：初始连接除了存在 conn 变量里，还被**多塞了一份**
// 进 connc（`connc <- conn`）。于是 connc 里那份重复副本与 `m.putc`
// 同时就绪，select 挑中哪个不确定：
//
//	先挑 putc ⇒ conn=nil、reconnectc=m.after(...)（重连已排定）；
//	下一轮若挑中 connc 那份**非 nil 的旧副本** ⇒ 走 else 分支 ⇒
//	reconnectc = nil ⇒ **刚排定的重连被抹掉**，连接断了却再也不重连。
//
// **为什么本测试要手工编排时序**：自然时序下 loop 往往在 Put 之前就把
// connc 排空了（本地压测 500 轮 0 丢失），撞不上那条路径——只有 CI 的
// 机器负载下才偶发。所以这里用阻塞 dialer 把 loop 精确停在 select 之前，
// 再让 Put 提前就位，逼出「connc 与 putc 同时 ready」的窗口。
//
// 修法：connc 只承载「重连拨号的结果」，初始连接不进 connc。
func TestManager_ReconnectNotLostByStaleConn(t *testing.T) {
	const rounds = 100
	lost := 0

	for i := 0; i < rounds; i++ {
		dialEntered := make(chan struct{})
		releaseDial := make(chan struct{})
		var dialCount int32
		afterCh := make(chan time.Time, 1)

		m := NewManager(
			func(network, address string) (net.Conn, error) {
				atomic.AddInt32(&dialCount, 1)
				close(dialEntered)
				<-releaseDial // 把 loop 卡在 doDial，connc 此刻仍空
				return &mockConn{}, nil
			},
			"tcp", "localhost:8080",
			func(d time.Duration) <-chan time.Time { return afterCh },
		)
		<-dialEntered

		// Put 先就位：它会阻塞在 m.putc 发送上，让接收侧在 loop 进 select
		// 时已经 ready。
		putDone := make(chan struct{})
		go func() {
			m.Put(errors.New("broken"))
			close(putDone)
		}()
		time.Sleep(20 * time.Millisecond)

		// 放行 dialer ⇒ loop 执行 connc <- conn 并进入 select，
		// 此时「connc 有副本」与「putc 可收」同时 ready。
		close(releaseDial)
		<-putDone
		afterCh <- time.Now()

		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			if atomic.LoadInt32(&dialCount) >= 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if atomic.LoadInt32(&dialCount) < 2 {
			lost++
		}
		m.Stop()
	}

	if lost > 0 {
		t.Errorf("重连被抹掉 %d/%d 次（connc 的重复副本吞掉了 reconnectc）", lost, rounds)
	}
}

// TestNewDefaultManager_TCP exercises the net.Dial + time.After path
// by dialing a real local TCP listener.
func TestNewDefaultManager_TCP(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Accept in background to unblock Dial.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c
		}
	}()

	m := NewDefaultManager("tcp", ln.Addr().String())
	defer m.Stop()

	c := m.Take()
	if c == nil {
		t.Fatal("expected conn from NewDefaultManager")
	}
}

// TestNewDefaultManager_DialFails covers the doDial error path:
// dialer returns an error → log.Error fires, returns nil conn → loop sends nil
// to connc → takec<-nil path makes Take() return nil.
func TestNewDefaultManager_DialFails(t *testing.T) {
	m := NewManager(
		func(network, address string) (net.Conn, error) {
			return nil, errors.New("dial refused")
		},
		"tcp", "127.0.0.1:1", // unreachable
		func(d time.Duration) <-chan time.Time { return nil },
	)
	defer m.Stop()

	c := m.Take()
	if c != nil {
		t.Errorf("expected nil conn on dial failure, got %T", c)
	}
}

// TestManager_Stop_ClosesCurrentConn verifies Stop closes the held conn
// and exits the loop goroutine.
func TestManager_Stop_ClosesCurrentConn(t *testing.T) {
	conn := &mockConn{}
	m := NewManager(
		func(network, address string) (net.Conn, error) { return conn, nil },
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return nil },
	)

	if c := m.Take(); c == nil {
		t.Fatal("expected conn")
	}

	m.Stop()
	// Give the loop goroutine a moment to drain.
	time.Sleep(50 * time.Millisecond)

	if !conn.closed.Load() {
		t.Error("expected conn to be closed by Stop")
	}
}

// TestManager_Stop_Idempotent verifies calling Stop multiple times is safe.
func TestManager_Stop_Idempotent(t *testing.T) {
	m := NewManager(
		func(network, address string) (net.Conn, error) { return &mockConn{}, nil },
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return nil },
	)
	m.Take() // drain the takec so the loop is in a stable state

	m.Stop()
	m.Stop() // must not panic
}

// TestManager_Write_ConnNil covers the Write branch when Take returns nil
// (returns ErrConnectionUnavailable instead of calling Write).
func TestManager_Write_ConnNil(t *testing.T) {
	m := NewManager(
		func(network, address string) (net.Conn, error) {
			return nil, errors.New("dial refused")
		},
		"tcp", "127.0.0.1:1",
		func(d time.Duration) <-chan time.Time { return nil },
	)
	defer m.Stop()

	_, err := m.Write([]byte("hello"))
	if !errors.Is(err, ErrConnectionUnavailable) {
		t.Errorf("expected ErrConnectionUnavailable, got %v", err)
	}
}

// TestManager_Loop_Reconnect covers the <-reconnectc branch:
// after Put(err) sets reconnectc=time.Nanosecond, the after channel
// fires and the loop spawns a doDial goroutine.
func TestManager_Loop_Reconnect(t *testing.T) {
	var dialCount int32
	afterCh := make(chan time.Time, 1)

	m := NewManager(
		func(network, address string) (net.Conn, error) {
			atomic.AddInt32(&dialCount, 1)
			return &mockConn{}, nil
		},
		"tcp", "localhost:8080",
		func(d time.Duration) <-chan time.Time { return afterCh },
	)

	if c := m.Take(); c == nil {
		t.Fatal("expected initial conn")
	}

	// Trigger reconnect: Put(err) → conn.Close, conn=nil, reconnectc=after(1ns)
	m.Put(errors.New("broken"))

	// Fire the after channel so the <-reconnectc case runs.
	afterCh <- time.Now()

	// Give the async.GoSafe dial goroutine a moment to run.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&dialCount) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&dialCount); got < 2 {
		t.Errorf("expected at least 2 dials (initial + reconnect), got %d", got)
	}
	m.Stop()
}

// TestExponential_Progression verifies the backoff doubles on each call
// and stays within ±50% jitter.
func TestExponential_Progression(t *testing.T) {
	prev := time.Second
	for i := 0; i < 5; i++ {
		d := Exponential(prev)
		// expected range: prev*2*0.5 .. prev*2*1.5
		lo := prev
		hi := prev * 3
		if d < lo || d > hi {
			t.Errorf("iter %d: backoff %v out of expected [%v, %v]", i, d, lo, hi)
		}
		prev = d
	}
}
