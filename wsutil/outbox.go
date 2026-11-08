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
// 满了怎么办是**调用方**的决定：Send 只报告「入队成功没有」，不替业务选
// 策略。同一个「满了」在不同场景下有多种正确处置——断开这条连接、断开对端、
// 丢掉这一帧、退出本端——而库替业务选一个，就把差异藏起来了。
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
	written   atomic.Int64
}

// OutboxOptions 是 NewOutbox 的可选项。
type OutboxOptions struct {
	// QueueSize 出站队列长度。零即 DefaultOutboxQueueSize。
	QueueSize int
}

// NewOutbox 建一条出站管道并启动它的写协程。
//
// write 是真正的写入动作（通常就是 conn.WriteMessage）。它只被写协程调用。
//
// ⚠️ **write 必须自带超时或可中断**：Close() 只关队列，打不断已经在跑的
// 一次 write。对端半开时，没有超时的 write 会让 Wait() 永远不返回 ⇒ 调用方
// 的关停路径挂死、goroutine 泄漏。传一个带写超时（SetWriteDeadline）的写入
// 动作，是调用方的责任。
func NewOutbox(write func([]byte) error, opts OutboxOptions) *Outbox {
	size := opts.QueueSize
	if size <= 0 {
		size = DefaultOutboxQueueSize
	}
	o := &Outbox{
		write: write,
		queue: make(chan []byte, size),
		done:  make(chan struct{}),
	}
	o.wg.Add(1)
	// GoSafe：写协程 panic 只许死这条连接，不许死整个进程——它在一个服务
	// 进程里跑着，挂着的是所有客户端。
	async.GoSafeLabeled("wsutil-outbox", o.run)
	return o
}

// Send 把一帧放进队列。
//
// 它**永不阻塞**：队列满时返回 false，返回 true 只说明「进了队列」，不说明
// 「已经写到网络上」。
//
// **它不替调用方做处置**——满了之后是断开这条连接、断开对端、丢掉这一帧，
// 还是退出本端，取决于场景。所以「满了」只报告、不动作：
//
//	if !ob.Send(frame) {
//		closePeer(p) // 慢消费者：断开这条连接
//	}
//
// 关闭之后再 Send 返回 false，不会 panic。两种 false 可区分（#105）：
// 管道已关（ob.Closed() 为 true——帧根本没进队列，不算丢帧）与队列满
// （Closed() 为 false——慢消费者，按文档示例处置对端）。
//
// ⚠️ **帧会被拷贝一份**。写协程是**稍后**才写它的，而调用方很可能复用
// 同一个 buffer（buf = buf[:0] 后重新填）——不拷贝的话，上线的是调用方改过
// 之后的字节，症状是「客户端收到坏 JSON」，而服务端这边什么错都不报。
// 拷贝换来的是一次分配；相比「坏字节难查」，这笔买卖划算。
func (o *Outbox) Send(msg []byte) bool {
	if o == nil || o.closed.Load() {
		return false
	}
	select {
	case o.queue <- cloneBytes(msg):
		return true
	default:
		o.dropped.Add(1)
		return false
	}
}

// cloneBytes 拷贝一帧。
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
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
// ⚠️ **它不代表队列已冲刷** —— 它只表示「已关闭，不再接受新帧」。写协程
// 可能还在冲刷队列里剩下的帧。要确认「帧都写出去了」，必须等 Wait() 返回。
//
// 正确顺序是：**Close() → Wait() → 关底层连接**。
//
// （这一段曾经写着「等 Done 再关底层连接」—— 而那恰恰是会截断待发帧的
// 顺序：Done 在 Close() 里就关闭了，那时一帧都还没冲刷。）
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

// Dropped 是**没有写出去**的帧数。
//
// 三种情形都算（#105 闭合口径）：队列满时拒收的帧、正在写但写失败的那一帧、
// 以及写失败后随队列一起消失的帧。Send 返回 true 的每一帧都必然闭合进
// Written 或 Dropped 之一——「Send(true) 次数 − Written」恒等于 Dropped。
// （管道已关时的 Send 拒收不算：那一帧从未进过队列，Send 返回的是 false。）
//
// 它非零说明有客户端读得比服务端推得快，或连接已经写不动了——那是**该去看
// 的东西**：要么客户端该自己慢点读，要么推送的频率该降，要么连接该重连。
func (o *Outbox) Dropped() int64 {
	if o == nil {
		return 0
	}
	return o.dropped.Load()
}

// Written 是已经写到网络上的帧数。
//
// 拿它和「Send 返回 true 的次数」相减，就是没写出去的帧数（= Dropped）。
// 这条差值在写失败时才是唯一能看见的线索。
func (o *Outbox) Written() int64 {
	if o == nil {
		return 0
	}
	return o.written.Load()
}

// Closed 报告管道是否已关闭。
//
// 它拆开 Send 的两种 false（#105）：Send 返回 false 且 Closed() 为 true ⇒
// 管道已关（那一帧没进队列，也不计入 Dropped）；Closed() 为 false ⇒ 队列满
// （慢消费者，按 Send 文档的示例处置）。文档给的示例把 false 一律当慢消费
// 者断对端——管道已关时那样做是误伤，先用这个分清楚。
func (o *Outbox) Closed() bool {
	if o == nil {
		return false
	}
	return o.closed.Load()
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
				// 先置 closed（不再接受新帧），再记账 ——否则并发的 Send
				// 还能往里塞，记账就不准了。
				o.Close()
				o.failInFlight()
				return
			}
			o.written.Add(1)
		case <-o.done:
			o.drain()
			return
		}
	}
}

// failInFlight 把「正在写、写失败的那一帧」计入 Dropped（#105）。
//
// 那一帧 Send 返回过 true（=「进了队列」），调用方据此认为帧已交付；旧实现
// 只把**队列剩下**的帧计入 Dropped，正在写的这一帧两头都不记 ⇒ 「Send(true)
// 次数 − Written」比 Dropped 恒大 1，文档承诺的差值公式闭合不了。
// 顺序必须是先置 closed 再 failInFlight + countPending：写失败 ⇒ 立刻关闭
// （不再接受新帧）⇒ 然后才能把在途帧与剩余帧一次性记准。
func (o *Outbox) failInFlight() {
	o.dropped.Add(1)
	o.countPending()
}

// countPending 把队列里还没写出去的帧计入 Dropped。
//
// 写失败时它们会**无声消失**：Send 已经返回过 true（=「进了队列」），而调用
// 方据此认为帧已交付。不记账的话，丢了多少帧只有天知道。
func (o *Outbox) countPending() {
	for {
		select {
		case <-o.queue:
			o.dropped.Add(1)
		default:
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
				// 冲刷阶段写失败的那一帧同样计入 Dropped（#105 同口径）。
				o.failInFlight()
				return
			}
			o.written.Add(1)
		default:
			return
		}
	}
}
