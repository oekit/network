package wsutil

import (
	"sync"
	"testing"
	"time"
)

// collector 是一个收集写出帧的假写入端。
type collector struct {
	mu   sync.Mutex
	msgs [][]byte
	// failAt 是第几次写时开始失败（-1 表示不失败）。
	failAt int
	calls  int
}

func newCollector() *collector { return &collector{failAt: -1} }

func (c *collector) write(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.failAt >= 0 && c.calls > c.failAt {
		return errWriteFailed
	}
	c.msgs = append(c.msgs, append([]byte(nil), msg...))
	return nil
}

func (c *collector) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.msgs))
	copy(out, c.msgs)
	return out
}

var errWriteFailed = errFailed{}

type errFailed struct{}

func (errFailed) Error() string { return "write failed" }

// TestOutbox_WritesInOrder 断言帧按入队顺序写出。
func TestOutbox_WritesInOrder(t *testing.T) {
	c := newCollector()
	o := NewOutbox(c.write, OutboxOptions{QueueSize: 8})
	defer o.Close()

	for i := 0; i < 5; i++ {
		if !o.Send([]byte{byte('a' + i)}) {
			t.Fatalf("frame %d was refused", i)
		}
	}
	waitFor(t, "all frames written", func() bool { return len(c.snapshot()) == 5 })

	got := c.snapshot()
	for i, msg := range got {
		if len(msg) != 1 || msg[0] != byte('a'+i) {
			t.Fatalf("frame %d = %q, want %q (order matters)", i, msg, byte('a'+i))
		}
	}
}

// TestOutbox_OverflowNeverBlocks 断言队列满时不阻塞发送方。
//
// 这一条是整个原语存在的理由：一个读得慢的客户端不能把发送方的主路径卡住。
func TestOutbox_OverflowNeverBlocks(t *testing.T) {
	// 写入端会卡住，于是队列很快满。
	block := make(chan struct{})
	taken := make(chan struct{})
	o := NewOutbox(func([]byte) error {
		select {
		case <-taken:
		default:
			close(taken)
		}
		<-block
		return nil
	}, OutboxOptions{QueueSize: 2})
	defer func() {
		close(block)
		o.Close()
	}()

	// 第一个帧被写协程取走（写端卡住），接下来两个填满队列。
	o.Send([]byte("1"))
	waitFor(t, "the writer to take the first frame", func() bool {
		select {
		case <-taken:
			return true
		default:
			return false
		}
	})
	o.Send([]byte("2"))
	o.Send([]byte("3"))

	start := time.Now()
	ok := o.Send([]byte("4")) // 这一帧必然满
	elapsed := time.Since(start)
	if ok {
		t.Error("a full queue must refuse the frame")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Send on a full queue blocked for %s", elapsed)
	}
	if o.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", o.Dropped())
	}
}

// TestOutbox_OverflowOnlyReports 断言队列满时**只报告、不处置**。
//
// 处置是业务决定：同一个「满了」，断开这条连接 / 断开对端 / 丢帧 / 退出本端
// 都可能对。库替业务选一个，就把差异藏起来了。
func TestOutbox_OverflowOnlyReports(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	taken := make(chan struct{})
	o := NewOutbox(func([]byte) error {
		select {
		case <-taken:
		default:
			close(taken)
		}
		<-block
		return nil
	}, OutboxOptions{QueueSize: 1})
	defer o.Close()

	o.Send([]byte("1"))
	waitFor(t, "the writer to take the first frame", func() bool {
		select {
		case <-taken:
			return true
		default:
			return false
		}
	})
	o.Send([]byte("2"))
	if o.Send([]byte("3")) {
		t.Error("a full queue must refuse the frame")
	}
	if o.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", o.Dropped())
	}
	// 关键：它没有被处置——管道还活着，调用方自己决定怎么办。
	select {
	case <-o.Done():
		t.Error("the overflow must not close the pipe: the disposition is the caller's call")
	default:
	}
}

// TestOutbox_SendAfterCloseIsSafe 断言关闭之后再 Send 不 panic。
func TestOutbox_SendAfterCloseIsSafe(t *testing.T) {
	c := newCollector()
	o := NewOutbox(c.write, OutboxOptions{})
	o.Close()
	o.Close() // 幂等

	if o.Send([]byte("x")) {
		t.Error("Send after Close must refuse the frame")
	}
	o.Wait()
}

// TestOutbox_CloseDrainsPendingFrames 断言关闭时把队列里剩下的帧冲刷出去。
//
// 正在推的最后一帧不该因为对端刚要走就被丢掉。
func TestOutbox_CloseDrainsPendingFrames(t *testing.T) {
	c := newCollector()
	o := NewOutbox(c.write, OutboxOptions{QueueSize: 8})
	for i := 0; i < 3; i++ {
		o.Send([]byte{byte('a' + i)})
	}
	// 等写协程把第一帧取走，剩下的留在队列里。
	waitFor(t, "the first frame to be written", func() bool { return len(c.snapshot()) >= 1 })
	o.Close()
	o.Wait()

	got := c.snapshot()
	if len(got) != 3 {
		t.Errorf("wrote %d frames, want 3 (the pending ones must be drained)", len(got))
	}
}

// TestOutbox_WriteFailureStopsThePump 断言写失败后写协程退出。
func TestOutbox_WriteFailureStopsThePump(t *testing.T) {
	c := newCollector()
	c.failAt = 0 // 第一次写就失败
	o := NewOutbox(c.write, OutboxOptions{QueueSize: 4})
	o.Send([]byte("x"))

	select {
	case <-o.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the pump did not stop after a write failure")
	}
	o.Wait()
}

// TestOutbox_ConcurrentSenders 断言并发 Send 不 panic、帧不丢。
func TestOutbox_ConcurrentSenders(t *testing.T) {
	c := newCollector()
	o := NewOutbox(c.write, OutboxOptions{QueueSize: 1024})
	defer o.Close()

	const senders, each = 8, 50
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				o.Send([]byte("x"))
			}
		}()
	}
	wg.Wait()
	waitFor(t, "every frame written", func() bool { return len(c.snapshot()) == senders*each })
}
