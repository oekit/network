package wsutil

// outbox.go 是单条连接的有界出站管道。
//
// 为什么需要它。gorilla/websocket（以及绝大多数 websocket 实现）**不允许并发
// 写**——两个写者同时写同一条连接会互相踩帧，症状是客户端收到乱序或半截的
// 帧，而服务端这边什么错都不报。所以每条连接必须有且只有一个写者，而那个
// 写者只能由一个 goroutine 驱动 ⇒ 出站帧必须先进队列，由它独家取走。
//
// 队列还必须有界。客户端读得慢时，写者会卡在写上（TCP 背压），于是队列
// 涨满；无界的队列会把「一个慢客户端」变成「服务端内存涨满」。
//
// 满了怎么办是调用方的决定：默认断开这条连接（websocket 推送的通行做法——
// 宁可不推，也不拖着所有人一起卡住）。
//
// 它**不依赖任何 websocket 库**：写入动作就是一个函数，所以 gorilla、
// hertz-contrib、乃至任何自定义传输都能用同一份。

import (
	"sync"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
)

// DefaultOutboxQueueSize 是出站队列的默认长度。
//
// 64 帧够应付正常的突发（一次状态刷新往往连带发好几帧），又不至于让一个
// 慢客户端占住太多内存。
const DefaultOutboxQueueSize = 64

// Outbox 是单条连接的有界出站管道。
//
// 零值不可用，用 NewOutbox。
type Outbox struct {
	// write 是真正的写入动作。它**只会被一个 goroutine 调用**，所以调用方
	// 不必自己加锁（websocket 实现本身也不许并发写）。
	write func([]byte) error

	queue chan []byte
	done  chan struct{}
	wg    sync.WaitGroup

	closeOnce sync.Once
	closed    atomic.Bool
	dropped   atomic.Int64

	// onOverflow 是队列满时的回调。nil 时默认是关闭这条连接。
	onOverflow func()
}

// OutboxOptions 是 NewOutbox 的可选项。
type OutboxOptions struct {
	// QueueSize 出站队列长度。零即 DefaultOutboxQueueSize。
	QueueSize int
	// OnOverflow 队列满时被调用（默认：关闭这条连接）。
	//
	// 它在**发送方**的 goroutine 上被调用，所以要短——它不能阻塞，也不该
	// 做重活。
	OnOverflow func()
}

// NewOutbox 建一条出站管道并启动它的写协程。
//
// write 是真正的写入动作（通常就是 conn.WriteMessage）。它只被写协程调用。
func NewOutbox(write func([]byte) error, opts OutboxOptions) *Outbox {
	size := opts.QueueSize
	if size <= 0 {
		size = DefaultOutboxQueueSize
	}
	o := &Outbox{
		write:      write,
		queue:      make(chan []byte, size),
		done:       make(chan struct{}),
		onOverflow: opts.OnOverflow,
	}
	o.wg.Add(1)
	// GoSafe：写协程 panic 只许死这条连接，不许死整个进程——它在一个服务
	// 进程里跑着，挂着的是所有客户端。
	async.GoSafeLabeled("wsutil-outbox", o.run)
	return o
}

// Send 把一帧放进队列。
//
// 它**永不阻塞**：队列满时按 OnOverflow 的策略处理（默认关闭这条连接），
// 返回 false。返回 true 只说明「进了队列」，不说明「已经写到网络上」。
//
// 关闭之后再 Send 返回 false，不会 panic。
func (o *Outbox) Send(msg []byte) bool {
	if o == nil || o.closed.Load() {
		return false
	}
	select {
	case o.queue <- msg:
		return true
	default:
		o.dropped.Add(1)
		if o.onOverflow != nil {
			o.onOverflow()
		} else {
			o.Close()
		}
		return false
	}
}

// Close 关闭这条管道。
//
// 幂等。关闭后写协程会**冲刷队列里剩余的帧**再退出——正在推的最后一帧不该
// 因为对端刚要走就被丢掉。
func (o *Outbox) Close() {
	if o == nil {
		return
	}
	o.closeOnce.Do(func() {
		o.closed.Store(true)
		close(o.done)
	})
}

// Done 在管道关闭后被关闭。
//
// 把它接到连接的生命周期上：连接要断时，先 Close 这条管道，等 Done 再关
// 底层连接——顺序反了会把还没写出去的帧截断。
func (o *Outbox) Done() <-chan struct{} {
	if o == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return o.done
}

// Wait 等写协程退出。
func (o *Outbox) Wait() {
	if o == nil {
		return
	}
	o.wg.Wait()
}

// Dropped 是被丢弃（队列满）的帧数。
//
// 它非零说明有客户端读得比服务端推得快——那是**该去看的东西**：要么客户端
// 该自己慢点读，要么推送的频率该降。
func (o *Outbox) Dropped() int64 {
	if o == nil {
		return 0
	}
	return o.dropped.Load()
}

// run 是写协程：队列的独家消费者。
func (o *Outbox) run() {
	defer o.wg.Done()
	// 写协程退出 ⇒ 这条连接到此为止（写失败、被关闭、或 panic 收编）。
	defer o.Close()
	for {
		select {
		case msg := <-o.queue:
			if err := o.write(msg); err != nil {
				return
			}
		case <-o.done:
			o.drain()
			return
		}
	}
}

// drain 冲刷队列里剩下的帧。
//
// 只在关闭后跑一次：正在推的最后一帧不该因为对端刚要走就被丢掉。
func (o *Outbox) drain() {
	for {
		select {
		case msg := <-o.queue:
			if err := o.write(msg); err != nil {
				return
			}
		default:
			return
		}
	}
}
