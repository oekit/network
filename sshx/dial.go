package sshx

// dial.go 打开到对端的 SSH 连接。
//
// 两道界限，因为它们界的是不同的东西。
//
// 「连接」由 connectTimeout 界。一个丢包的端口否则会挂在操作系统自己的
// 超时上——macOS 上是 75 秒——而扫十台机器就要付十次。
//
// 「握手」单独界，而且必须单独界：x/crypto/ssh 的 ClientConfig.Timeout 只
// 管 TCP 连接，握手一概不管。握手跑在裸连接上，**没有任何截止时间**。一个
// 接受连接之后就不说 SSH 的端口——透明代理、空转的端口转发、回了 SYN 就
// 沉默的中间盒——会让调用方一直等下去。
//
// 那种等待是最危险的一种：降级路径本该在快路径用不了时接手，可快路径还没
// 「失败」——它还在等。于是降级永远轮不上，调用永远不返回。
//
// 截止时间设在 socket 上，握手做完就撤掉：于是它只界握手，不界别的——
// 一条跑几分钟的命令不能被那个「为了握手」的截止时间砍断。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
)

// 拨号的两道界限。
const (
	// DefaultConnectTimeout 界 TCP 连接。
	DefaultConnectTimeout = 15 * time.Second

	// DefaultHandshakeTimeout 界 SSH 协议握手。
	//
	// 健康的握手是毫秒级的，所以这个数是它的许多倍：它在这里是为了接住
	// 病态情况，不是为了催健康的那个。
	//
	// 不对称决定了这个数。误判超时会把快路径推去降级路径，每次调用多花
	// 十秒；而漏了超时会让调用永远不返回，永远不返回的调用没有降级可言。
	// 宁可偏向长的一边——那是便宜的错误。
	DefaultHandshakeTimeout = 5 * time.Second
)

// DialOptions 界住打开一次连接的过程。
//
// 三个值都是零值即默认（见各自的默认常量）。它们是参数而不是常量，这样
// 测试才能用短的值：要等五秒才看得到一次超时的测试，没人跑。
type DialOptions struct {
	// Connect 界 TCP 连接。零值即 DefaultConnectTimeout。
	Connect time.Duration
	// Handshake 界 SSH 握手。零值即 DefaultHandshakeTimeout。
	Handshake time.Duration
	// Budget 是调用方给这次调用的**总预算**（#106）：锚定在 Dial 入口，
	// 压住的是「连接 + 握手」的**总和**——连接阶段花掉的每一毫秒都从
	// 预算里扣，握手窗口按剩余预算夹取。给了两秒的调用方不该坐在一个
	// 五秒的握手里等一个自己不会用的结果，也不该等 2×预算。
	Budget time.Duration

	// dialFunc 覆盖 TCP 拨号（仅测试注入「连得上但慢」的连接用；零值即
	// 带 Timeout 的 net.Dialer）。导出结构体上的非导出字段：不影响键控
	// 字面量构造，生产路径不可见。
	dialFunc func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error)
}

// Dial 打开到 addr 的 SSH 连接。
//
// ctx 对连接阶段有效：放弃的调用方不会留下一个开着的 socket。握手由固定
// 界限、调用方剩余预算与 ctx 剩余时间三者中最小的那个界定——三者都在
// **拨号完成之后**求值（#106）：预算是总预算、锚定在 Dial 入口，ctx 的
// 截止时间同样压住握手；连接阶段耗掉的 T 不会再让总耗时可接近 2×预算、
// 或超出 ctx 截止时间约 T。
// nil ctx 按 context.Background 处理（#79，与同仓 ratelimit.TokenBucket.WaitN /
// syncx.Gate.Acquire / retry.DoValue 的兜底口径一致），不留给调用方一个
// nil 解引用 panic。
//
// 返回的 client 拥有这条连接：Close 它就关掉 socket。
//
// 失败分两种，措辞不同，因为调用方对它们的反应不同：一种是超时（对端没
// 说 SSH），一种是拒绝（对端说了但不让进）。调用方常常要在这两者之间做
// 降级决定，所以它们不能被揉成同一句话。
func Dial(ctx context.Context, addr string, cc *ssh.ClientConfig, opts DialOptions) (*ssh.Client, error) {
	if cc == nil {
		return nil, fmt.Errorf("dial %s: no client config", addr)
	}
	// nil ctx 兜底 Background（#79）：ctx 只取 Deadline/取消语义，
	// Background（无截止、永不取消）即「调用方没有额外约束」。
	if ctx == nil {
		ctx = context.Background()
	}
	// 总预算锚定在 Dial 入口（#106）：从这里起算「这次调用花了多久」。
	start := time.Now()
	connect := opts.Connect
	if connect <= 0 {
		connect = DefaultConnectTimeout
	}
	handshake := opts.Handshake
	if handshake <= 0 {
		handshake = DefaultHandshakeTimeout
	}
	// 预算先夹连接阶段：单是连接就不许花掉超过预算的时间。
	// 握手窗口在拨号完成后按**剩余**预算与 ctx 剩余时间夹取（见下）。
	if opts.Budget > 0 && opts.Budget < connect {
		connect = opts.Budget
	}
	dialFn := opts.dialFunc
	if dialFn == nil {
		dialFn = func(ctx context.Context, timeout time.Duration, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
		}
	}
	conn, err := dialFn(ctx, connect, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	// 握手窗口在**拨号完成之后**求值（#106）：旧的实现在拨号**前**就用
	// 当时的剩余时间把握手窗口定死、又把窗口锚定在拨号完成时刻——连接
	// 阶段耗掉 T 时，总耗时上界 ≈ T + min(握手, 预算, T0 时的剩余)，可
	// 接近 2×预算、并可超出 ctx 截止时间约 T。现在按此刻的剩余夹取：
	// 预算是总预算，ctx 的截止时间真正压住握手。
	if opts.Budget > 0 {
		if remaining := opts.Budget - time.Since(start); remaining < handshake {
			handshake = remaining
		}
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < handshake {
			handshake = remaining
		}
	}
	if handshake <= 0 {
		// 连接阶段已把预算/ctx 耗尽：立即放弃，不做一场没人会等结果的握手。
		_ = conn.Close()
		return nil, fmt.Errorf("dial %s: budget or context deadline exhausted after connect (took %s)",
			addr, time.Since(start).Round(time.Millisecond))
	}
	// 截止时间必须设在 socket 上：那是唯一能从外面界住 x/crypto/ssh 握手
	// 的地方。
	if err := conn.SetDeadline(time.Now().Add(handshake)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set the handshake deadline on %s: %w", addr, err)
	}
	clientConn, chans, reqs, herr := ssh.NewClientConn(conn, addr, cc)
	if herr != nil {
		_ = conn.Close()
		// 超时与拒绝是不同的失败，措辞要把它们分开：拒绝（"unable to
		// authenticate"）是让调用方去降级的那个信号，而超时绝不能被打扮
		// 成一个拒绝。
		// errors.As, not a type assertion: x/crypto/ssh always wraps the
		// handshake error ("ssh: handshake failed: %w"), so the value is a
		// *fmt.wrapError and a direct assertion to net.Error NEVER matches
		// — the timeout wording below was dead code, and the timeout came
		// out dressed as a refusal.
		var ne net.Error
		if errors.As(herr, &ne) && ne.Timeout() {
			return nil, fmt.Errorf("the SSH handshake with %s did not finish within %s: %w",
				addr, handshake, herr)
		}
		return nil, fmt.Errorf("the SSH handshake with %s failed: %w", addr, herr)
	}
	// 截止时间已经办完事。连接不能被那个「为了握手」的截止时间砍断。
	_ = conn.SetDeadline(time.Time{})
	return ssh.NewClient(clientConn, chans, reqs), nil
}
