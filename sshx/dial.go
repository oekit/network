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
	// Budget 是调用方给这次调用的总预算，它同时压住上面两个界限：
	// 给了两秒的调用方不该坐在一个五秒的握手里等一个自己不会用的结果。
	Budget time.Duration
}

// Dial 打开到 addr 的 SSH 连接。
//
// ctx 对连接阶段有效：放弃的调用方不会留下一个开着的 socket。握手由固定
// 界限、调用方预算与 ctx 的截止时间三者中最小的那个界定。
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
	connect := opts.Connect
	if connect <= 0 {
		connect = DefaultConnectTimeout
	}
	handshake := opts.Handshake
	if handshake <= 0 {
		handshake = DefaultHandshakeTimeout
	}
	// 调用方的预算压住连接与握手 alike。
	if opts.Budget > 0 && opts.Budget < connect {
		connect = opts.Budget
	}
	if opts.Budget > 0 && opts.Budget < handshake {
		handshake = opts.Budget
	}
	// ctx 的截止时间同样压住握手：调用方已经放弃的调用，不该再等一个没人
	// 会读的握手结果。
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < handshake {
			handshake = remaining
		}
	}
	dialer := net.Dialer{Timeout: connect}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
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
		if ne, ok := herr.(net.Error); ok && ne.Timeout() {
			return nil, fmt.Errorf("the SSH handshake with %s did not finish within %s: %w",
				addr, handshake, herr)
		}
		return nil, fmt.Errorf("the SSH handshake with %s failed: %w", addr, herr)
	}
	// 截止时间已经办完事。连接不能被那个「为了握手」的截止时间砍断。
	_ = conn.SetDeadline(time.Time{})
	return ssh.NewClient(clientConn, chans, reqs), nil
}
