package sshx

import (
	"errors"
	"testing"
	"time"
)

// TestStartKeepAlive_CallsOnDead 断言连接反复失败之后 onDead 被调用。
func TestStartKeepAlive_CallsOnDead(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	dead := make(chan struct{})
	attempts := 0
	StartKeepAlive(func() error {
		attempts++
		return errors.New("the connection is gone")
	}, done, func() { close(dead) }, 10*time.Millisecond)

	select {
	case <-dead:
	case <-time.After(5 * time.Second):
		t.Fatal("onDead was never called")
	}
	if attempts < 2 {
		t.Errorf("attempts = %d，期望至少 2（一次失败不算死）", attempts)
	}
}

// TestStartKeepAlive_ToleratesOneMiss 断言一次失败不判死。
//
// 一次写失败不能证明连接没了，而换掉一条好连接的代价是一次握手。
func TestStartKeepAlive_ToleratesOneMiss(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	dead := make(chan struct{})
	calls := 0
	StartKeepAlive(func() error {
		calls++
		if calls == 1 {
			return errors.New("one miss")
		}
		return nil
	}, done, func() { close(dead) }, 10*time.Millisecond)

	select {
	case <-dead:
		t.Fatal("one miss should not be taken for a dead connection")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestStartKeepAlive_BusyIsNotDead 断言「连接正忙着」不算死。
func TestStartKeepAlive_BusyIsNotDead(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	dead := make(chan struct{})
	StartKeepAlive(func() error { return ErrBusy }, done, func() { close(dead) }, 10*time.Millisecond)

	select {
	case <-dead:
		t.Fatal("a busy connection must not be taken for a dead one")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestStartKeepAlive_StopsOnDone 断言 done 关掉之后保活停下来。
func TestStartKeepAlive_StopsOnDone(t *testing.T) {
	done := make(chan struct{})
	stopped := make(chan struct{})
	StartKeepAlive(func() error {
		close(stopped)
		return nil
	}, done, nil, 5*time.Millisecond)

	<-stopped
	close(done)
	// 关掉之后不再有戳连接的动作：等一小会儿确认它安静了。
	time.Sleep(50 * time.Millisecond)
}

// TestStartKeepAlive_NilArguments 断言空参数不炸。
func TestStartKeepAlive_NilArguments(t *testing.T) {
	StartKeepAlive(nil, nil, nil, time.Second)
	StartKeepAlive(func() error { return nil }, nil, nil, time.Second)
}
