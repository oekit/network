package conn

import (
	"errors"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
)

var log = logx.Delegating("conn")

// Dialer 模仿 net.Dial，创建网络连接。
// 返回的连接必须是并发安全的。
type Dialer func(network, address string) (net.Conn, error)

// AfterFunc 模仿 time.After，用于控制重连退避时间。
type AfterFunc func(time.Duration) <-chan time.Time

// Manager 管理一个 net.Conn 的生命周期。
//
// 通过 Take 获取连接，通过 Put 报告错误。
// 当 Put 收到非 nil error 时，当前连接失效，Manager 自动重连（指数退避）。
type Manager struct {
	dialer  Dialer
	network string
	address string
	after   AfterFunc

	takec    chan net.Conn
	putc     chan error
	stopc    chan struct{}
	stopOnce sync.Once
}

// NewManager 创建一个连接管理器。
//
// 参数：
//   - d: 连接创建函数
//   - network/address: 传递给 Dialer
//   - after: 时间控制函数（测试可注入，生产用 time.After）
func NewManager(d Dialer, network, address string, after AfterFunc) *Manager {
	m := &Manager{
		dialer:  d,
		network: network,
		address: address,
		after:   after,

		takec: make(chan net.Conn),
		putc:  make(chan error),
		stopc: make(chan struct{}),
	}
	async.GoSafe(m.loop)
	return m
}

// NewDefaultManager 创建一个使用默认配置的连接管理器。
// 使用 net.Dial 和 time.After。
func NewDefaultManager(network, address string) *Manager {
	return NewManager(
		net.Dial,
		network,
		address,
		time.After,
	)
}

// Take 返回当前连接，可能为 nil（连接断开或尚未建立）。
// Stop 后调用返回 nil。
func (m *Manager) Take() net.Conn {
	select {
	case conn := <-m.takec:
		return conn
	case <-m.stopc:
		return nil
	}
}

// Put 报告连接使用中产生的错误。
// 非 nil error 会导致当前连接被关闭并触发重连。
// Stop 后调用是空操作。
func (m *Manager) Put(err error) {
	select {
	case m.putc <- err:
	case <-m.stopc:
	}
}

// Stop 停止管理器，关闭当前连接，解除所有阻塞的 Take/Put 调用。
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopc)
	})
}

// Write 是便捷方法：自动 Take → Write → Put。
// 当连接不可用时返回 ErrConnectionUnavailable。
//
// 注意：Write 不是并发安全的。Take/Put 设计为单 goroutine 使用，
// 多 goroutine 调用 Write 会导致连接竞争。并发场景应由调用方加锁。
func (m *Manager) Write(b []byte) (int, error) {
	conn := m.Take()
	if conn == nil {
		return 0, ErrConnectionUnavailable
	}
	n, err := conn.Write(b)
	if err != nil {
		m.Put(err)
	}
	return n, err
}

func (m *Manager) loop() {
	var (
		conn       = m.doDial()
		connc      = make(chan net.Conn, 1)
		reconnectc <-chan time.Time
		backoff    = time.Second
	)

	// 初始连接**不**进 connc：connc 只承载「重连拨号的结果」。
	//
	// 此前这里有一行 `connc <- conn`，把初始连接又塞进了 connc——而 conn
	// 变量里本来就有同一份。于是 connc 里留下一份**重复副本**，它与
	// `m.putc` 同时就绪，select 挑中哪个是不确定的：
	//
	//	若先挑 putc：conn=nil、reconnectc=m.after(...)（重连已排定）；
	//	下一轮若挑中 connc 那份**非 nil 的旧副本** ⇒ 走 else 分支 ⇒
	//	reconnectc = nil ⇒ **刚排定的重连被抹掉**，连接断了却再也不重连。
	//
	// 复现见 TestManager_ReconnectNotLostByStaleConn（100 轮约 30 次丢失）。
	// 修法：connc 的唯一写者是下面的重连 goroutine，语义单一才不会串味。
	// 初始连接直接由 `m.takec <- conn` 分支交付给调用方。

	for {
		select {
		case <-m.stopc:
			if conn != nil {
				conn.Close()
			}
			return

		case <-reconnectc:
			reconnectc = nil
			async.GoSafe(func() {
				nc := m.doDial()
				if nc == nil {
					connc <- nil
					return
				}
				// Stop 后 loop 已退出、无人接收——已拨通的连接必须就地
				// Close，否则 fd 泄漏（缓冲 chan 只吞不关）
				select {
				case connc <- nc:
				case <-m.stopc:
					_ = nc.Close()
				}
			})

		case conn = <-connc:
			if conn == nil {
				backoff = Exponential(backoff)
				reconnectc = m.after(backoff)
			} else {
				backoff = time.Second
				reconnectc = nil
			}

		case m.takec <- conn:

		case err := <-m.putc:
			if err == nil {
				break // nil error = success, no-op
			}
			if err != nil && conn != nil {
				log.Error("connection error", "err", err)
				conn.Close()
				conn = nil
				reconnectc = m.after(time.Nanosecond)
			}
		}
	}
}

func (m *Manager) doDial() net.Conn {
	conn, err := m.dialer(m.network, m.address)
	if err != nil {
		log.Error("dial failed", "err", err)
		return nil
	}
	return conn
}

// Exponential 计算指数退避时间（带 ±50% 抖动），上限 1 分钟。
// 用于避免重连风暴（thundering herd）。
func Exponential(d time.Duration) time.Duration {
	d *= 2
	jitter := rand.Float64() + 0.5
	d = time.Duration(int64(float64(d.Nanoseconds()) * jitter))
	if d > time.Minute {
		d = time.Minute
	}
	return d
}

// ErrConnectionUnavailable 在连接不可用时由 Write 方法返回。
var ErrConnectionUnavailable = errors.New("connection unavailable")
