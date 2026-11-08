package sshx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// fakeSSH 是一个最小的 SSH 服务端：完成握手、回应全局请求，并数它们。
//
// 它存在是为了让传输层的界限对着一次真实的 SSH 对话来测，而不是对着一个
// mock——被界住的就是这次对话，对 mock 测只能证明 mock 被界住了。
type fakeSSH struct {
	ln       net.Listener
	requests chan string

	mu    sync.Mutex
	conns []net.Conn
}

// newFakeSSH 在随机端口上起一个服务端。
func newFakeSSH(t *testing.T) *fakeSSH {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate a host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("sign the host key: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeSSH{ln: ln, requests: make(chan string, 64)}
	go f.serve(signer)
	t.Cleanup(func() {
		_ = ln.Close()
		f.dropConnections()
	})
	return f
}

// addr 是服务端监听的地址。
func (f *fakeSSH) addr() string { return f.ln.Addr().String() }

// serve 接受连接直到监听器被关掉。
func (f *fakeSSH) serve(signer ssh.Signer) {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn, signer)
	}
}

// handle 处理一条连接：握手，然后是全局请求。
func (f *fakeSSH) handle(conn net.Conn, signer ssh.Signer) {
	f.mu.Lock()
	f.conns = append(f.conns, conn)
	f.mu.Unlock()
	defer conn.Close()

	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(signer)
	_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		return
	}
	go func() {
		for req := range reqs {
			select {
			case f.requests <- req.Type:
			default:
			}
			if req.WantReply {
				_ = req.Reply(true, nil)
			}
		}
	}()
	for ch := range chans {
		_ = ch.Reject(ssh.Prohibited, "this server has no channels")
	}
}

// dropConnections 掐掉服务端持有的每一条连接。
func (f *fakeSSH) dropConnections() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

// clientConfig 是假服务端肯接受的客户端配置。
func clientConfig() *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User:            "ops",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}

// TestDial_GivesUpOnAHandshakeThatNeverFinishes 断言「接受了连接但从不说
// SSH」的端口不能把调用方挂死。
//
// 这就是界限堵上的那个洞。x/crypto/ssh 的 ClientConfig.Timeout 只管 TCP
// 连接；之后的握手跑在裸连接上，没有任何截止时间——所以没有界限的时候这
// 个调用会永远等下去，而降级路径永远轮不到它，因为快路径并没有失败，它还在
// 等。
func TestDial_GivesUpOnAHandshakeThatNeverFinishes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// 连接被接受并**握在手里**。这正是重点：对端在，它只是不说话。
	hold := make(chan struct{})
	defer close(hold)
	go func() {
		for {
			conn, cerr := ln.Accept()
			if cerr != nil {
				return
			}
			go func() {
				<-hold
				_ = conn.Close()
			}()
		}
	}()

	start := time.Now()
	_, err = Dial(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Connect: 2 * time.Second, Handshake: 150 * time.Millisecond})
	if err == nil {
		t.Fatal("the handshake was not bounded: this call would have waited forever")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the handshake took %s to give up; the bound was 150ms", elapsed)
	}
	if !strings.Contains(err.Error(), "handshake") {
		t.Errorf("the error does not say what went wrong: %v", err)
	}
	// The timeout has to be told apart from a refusal. x/crypto/ssh always
	// wraps the handshake error, so finding the timeout means walking the
	// error chain — a direct type assertion to net.Error never matches, and
	// the timeout comes out dressed as a refusal ("unable to authenticate"
	// is the string the caller watches for).
	if !strings.Contains(err.Error(), "did not finish within") {
		t.Errorf("the timeout was not told apart from a refusal: %v", err)
	}
	// #116：措辞之外还要有类型——集成方的重试分类不能被迫对措辞做字符串
	// 匹配（上游任何措辞调整都会静默翻转分类）。
	var hte *HandshakeTimeoutError
	if !errors.As(err, &hte) {
		t.Fatalf("the timeout must be typed (*HandshakeTimeoutError, #116): %v", err)
	}
	if hte.Addr != ln.Addr().String() {
		t.Errorf("Addr = %q, want %q", hte.Addr, ln.Addr().String())
	}
	if hte.Bound != 150*time.Millisecond {
		t.Errorf("Bound = %s, want 150ms (the effective handshake bound)", hte.Bound)
	}
	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("errors.Is(err, ErrHandshakeTimeout) = false, want true (#116)")
	}
	if errors.Is(err, ErrHandshakeRejected) {
		t.Errorf("a timeout must not be classifiable as a rejection (#116)")
	}
	// 原因链保留：net.Error 仍能从链上 As 出来——类型化不改底层事实。
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Errorf("the chain must still surface a net.Error with Timeout() = true: %v", err)
	}
}

// TestDial_CompletesARealHandshake 断言界限不伤健康路径。
func TestDial_CompletesARealHandshake(t *testing.T) {
	f := newFakeSSH(t)
	client, err := Dial(context.Background(), f.addr(), clientConfig(), DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
}

// TestDial_BoundsTheWholeOpening 断言整个打开过程有界，不管慢的是哪一段。
//
// 没人 accept 的监听器仍然完成 TCP 握手（内核的 backlog 干这件事），所以
// 这里救下调用方的是握手界限。连接界限管的是另一种情况：丢包的端口，连接
// 本身永远完不成。两者对调用方是同一个承诺：这个调用会返回。
func TestDial_BoundsTheWholeOpening(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	start := time.Now()
	_, err = Dial(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Connect: 200 * time.Millisecond, Handshake: time.Second})
	if err == nil {
		t.Fatal("the opening was not bounded")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the opening took %s to give up; the bounds were 200ms and 1s", elapsed)
	}
}

// TestDial_BudgetCapsTheBounds 断言调用方的总预算压住两道界限。
//
// 这一条是「给两秒预算的调用方不该坐在五秒握手里等一个自己不会用的结果」
// 的实现。少了它，预算只是个注释。
func TestDial_BudgetCapsTheBounds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	hold := make(chan struct{})
	defer close(hold)
	go func() {
		for {
			conn, cerr := ln.Accept()
			if cerr != nil {
				return
			}
			go func() {
				<-hold
				_ = conn.Close()
			}()
		}
	}()

	start := time.Now()
	// 预算 200ms，而握手界限给的是 10 秒——预算必须赢。
	_, err = Dial(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Handshake: 10 * time.Second, Budget: 200 * time.Millisecond})
	if err == nil {
		t.Fatal("the budget did not cap the handshake bound")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the call took %s to give up; the budget was 200ms", elapsed)
	}
}

// TestDial_ContextDeadlineCapsTheHandshake 断言 ctx 的截止时间也压住握手。
func TestDial_ContextDeadlineCapsTheHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	hold := make(chan struct{})
	defer close(hold)
	go func() {
		for {
			conn, cerr := ln.Accept()
			if cerr != nil {
				return
			}
			go func() {
				<-hold
				_ = conn.Close()
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := Dial(ctx, ln.Addr().String(), clientConfig(), DialOptions{Handshake: 10 * time.Second}); err == nil {
		t.Fatal("the context deadline did not cap the handshake")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the call took %s to give up; the context deadline was 200ms", elapsed)
	}
}

// TestDial_NilConfig 断言空配置是调用方的错，不是网络故障。
func TestDial_NilConfig(t *testing.T) {
	if _, err := Dial(context.Background(), "127.0.0.1:1", nil, DialOptions{}); err == nil {
		t.Fatal("a nil client config should be rejected")
	}
}

// TestDial_NilContext 回归（#79）：nil ctx 兜底 context.Background，不再
// nil 解引用 panic——与同仓 ratelimit.TokenBucket.WaitN / syncx.Gate.Acquire /
// retry.DoValue 的兜底口径一致。拨一个立即 refused 的本地端口：期望拿到
// 拨号错误（而非 panic），Background（无截止、永不取消）即「调用方没有
// 额外约束」。
func TestDial_NilContext(t *testing.T) {
	// 类型化的 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参，
	// 而 nil 值的 context.Context 变量在运行时就是 nil 接口——回归语义不变。
	var nilCtx context.Context
	client, err := Dial(nilCtx, "127.0.0.1:1", clientConfig(), DialOptions{})
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("expected a dial error to the refused port, got nil")
	}
	if client != nil {
		t.Fatal("expected a nil client on error")
	}
}

// holdListener 起一个「连接被接受、对端永不说话」的监听器：连接完成 TCP
// 握手，但 SSH 握手会一直挂到调用方的截止时间。这正是 #106 需要的形态：
// 「连得上但慢」的连接。
func holdListener(t *testing.T) *net.TCPListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold); _ = ln.Close() })
	go func() {
		for {
			conn, cerr := ln.Accept()
			if cerr != nil {
				return
			}
			go func() {
				<-hold
				_ = conn.Close()
			}()
		}
	}()
	return ln.(*net.TCPListener)
}

// TestDial_BudgetCoversConnectPlusHandshake 回归（#106）：Budget 是**总
// 预算**——连接阶段耗掉的每一毫秒都从预算里扣，握手窗口按剩余预算夹取。
//
// 旧实现在拨号**前**把握手窗口按 T0 时刻的剩余预算定死、又锚定在拨号完成
// 时刻 ⇒ 连接耗 T 时总耗时 ≈ T + Budget（可接近 2×预算）。这里注入「慢
// 连接 350ms + 对端沉默」：预算 500ms，总耗时必须 ≤ 预算 + 余量，而不是
// ≈ 850ms。
func TestDial_BudgetCoversConnectPlusHandshake(t *testing.T) {
	ln := holdListener(t)

	slowConnect := func(ctx context.Context, timeout time.Duration, _, _ string) (net.Conn, error) {
		time.Sleep(350 * time.Millisecond) // 连接阶段耗 350ms
		// net.Dial（不是 ln.Accept()——那会和 holdListener 的 accept
		// 循环抢同一条连接，抢输的 Accept 永远等不到下一条）。
		return net.Dial("tcp", ln.Addr().String())
	}

	start := time.Now()
	_, err := Dial(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Handshake: 10 * time.Second, Budget: 500 * time.Millisecond, dialFunc: slowConnect})
	if err == nil {
		t.Fatal("the call must fail at the budget, not sit in a handshake nobody will use")
	}
	if elapsed := time.Since(start); elapsed > 650*time.Millisecond {
		t.Errorf("the call took %s; the budget was 500ms and must cover connect + handshake (#106)", elapsed)
	}
	// 慢在握手 ⇒ 超时要按超时说，不能打扮成拒绝。
	if !strings.Contains(err.Error(), "did not finish within") {
		t.Errorf("the error must say the handshake timed out: %v", err)
	}
}

// TestDial_ContextDeadlineAnchoredAfterConnect 回归（#106）的另一半：ctx
// 的截止时间压住握手，锚点必须在拨号**之后**。旧实现用 T0 时刻的剩余时间
// 定握手窗口、又从拨号完成时刻起算 ⇒ 总耗时超出 ctx 截止时间约 T。
// 这里 ctx 截止 500ms、慢连接耗 350ms：总耗时必须 ≈ 截止时间，而不是
// ≈ 850ms。
func TestDial_ContextDeadlineAnchoredAfterConnect(t *testing.T) {
	ln := holdListener(t)

	slowConnect := func(ctx context.Context, timeout time.Duration, _, _ string) (net.Conn, error) {
		time.Sleep(350 * time.Millisecond)
		return net.Dial("tcp", ln.Addr().String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := Dial(ctx, ln.Addr().String(), clientConfig(),
		DialOptions{dialFunc: slowConnect})
	if err == nil {
		t.Fatal("the call must respect the context deadline")
	}
	if elapsed := time.Since(start); elapsed > 650*time.Millisecond {
		t.Errorf("the call took %s; the context deadline was 500ms and must bound the whole call (#106)", elapsed)
	}
}

// TestDial_BudgetExhaustedByConnect_FailsFast 钉住总预算的另一面：连接阶段
// 恰好耗尽预算时，不再做一场没人会等结果的握手——立即放弃并说清原因。
func TestDial_BudgetExhaustedByConnect_FailsFast(t *testing.T) {
	ln := holdListener(t)

	slowConnect := func(ctx context.Context, timeout time.Duration, _, _ string) (net.Conn, error) {
		time.Sleep(120 * time.Millisecond)
		return net.Dial("tcp", ln.Addr().String())
	}

	start := time.Now()
	_, err := Dial(context.Background(), ln.Addr().String(), clientConfig(),
		DialOptions{Budget: 120 * time.Millisecond, dialFunc: slowConnect})
	if err == nil {
		t.Fatal("an exhausted budget must fail fast, not attempt a handshake")
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Errorf("the call took %s after the budget was already spent; it must give up immediately", elapsed)
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Errorf("the error must say the budget was exhausted: %v", err)
	}
}

// refuseSSH 是一个「说了 SSH 但不让进」的服务端：完成版本交换，走到认证
// 一步就拒绝。#116 需要的另一种形态——与「接受连接但沉默」的超时相对。
func refuseSSH(t *testing.T) net.Listener {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate a host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("sign the host key: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, cerr := ln.Accept()
			if cerr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				cfg := &ssh.ServerConfig{
					// 凭据永远不对：让调用方拿到的是「拒绝」，不是超时。
					PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
						return nil, errors.New("wrong password")
					},
				}
				cfg.AddHostKey(signer)
				_, _, _, _ = ssh.NewServerConn(c, cfg)
			}(conn)
		}
	}()
	return ln
}

// TestDial_HandshakeRejectionIsTyped 回归（#116）：拒绝必须走类型。
// 集成方的重试分类要分「抖动（重拨）」与「判定（不重拨）」——拒绝是判定
// 那一类：重拨只是多打几次凭据、掩盖真错误。旧的实现只有措辞区分，上游
// 任何措辞调整都会静默翻转分类。
func TestDial_HandshakeRejectionIsTyped(t *testing.T) {
	ln := refuseSSH(t)

	cc := &ssh.ClientConfig{
		User:            "ops",
		Auth:            []ssh.AuthMethod{ssh.Password("wrong")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	_, err := Dial(context.Background(), ln.Addr().String(), cc, DialOptions{})
	if err == nil {
		t.Fatal("a refused handshake must fail")
	}

	var hre *HandshakeRejectedError
	if !errors.As(err, &hre) {
		t.Fatalf("the refusal must be typed (*HandshakeRejectedError, #116): %v", err)
	}
	if hre.Addr != ln.Addr().String() {
		t.Errorf("Addr = %q, want %q", hre.Addr, ln.Addr().String())
	}
	if !errors.Is(err, ErrHandshakeRejected) {
		t.Errorf("errors.Is(err, ErrHandshakeRejected) = false, want true (#116)")
	}
	if errors.Is(err, ErrHandshakeTimeout) {
		t.Errorf("a refusal must not be classifiable as a timeout (#116)")
	}
	// 原因链保留：拒绝本身（"unable to authenticate"）仍在链上。
	if !strings.Contains(err.Error(), "unable to authenticate") {
		t.Errorf("the refusal's cause must stay on the chain: %v", err)
	}
	// 措辞兼容：与类型化之前的文本逐字一致（人读的那路不受影响）。
	if !strings.HasPrefix(err.Error(), "the SSH handshake with "+ln.Addr().String()+" failed: ") {
		t.Errorf("the refusal wording must stay compatible: %v", err)
	}
	// 底层不是超时：拒绝绝不能被打扮成一个超时。
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Errorf("a refusal must not carry a net.Error with Timeout() = true: %v", err)
	}
}

// TestHandshakeErrorTypes 钉住两种类型化错误的合同：措辞与既有文本逐字
// 一致（兼容只做字符串匹配的既有调用方）、Is/As 双路可判、Unwrap 保留
// 原因链。
func TestHandshakeErrorTypes(t *testing.T) {
	base := errors.New("ssh: handshake failed: boom")

	to := &HandshakeTimeoutError{Addr: "10.0.0.1:22", Bound: 150 * time.Millisecond, Err: base}
	wantTo := "the SSH handshake with 10.0.0.1:22 did not finish within 150ms: ssh: handshake failed: boom"
	if to.Error() != wantTo {
		t.Errorf("Error() = %q, want %q (the wording must stay byte-compatible, #116)", to.Error(), wantTo)
	}
	if !errors.Is(to, ErrHandshakeTimeout) {
		t.Error("errors.Is(to, ErrHandshakeTimeout) = false, want true")
	}
	if errors.Is(to, ErrHandshakeRejected) {
		t.Error("the timeout type must not match ErrHandshakeRejected")
	}
	if !errors.Is(to, base) {
		t.Error("Unwrap must keep the cause on the chain")
	}

	re := &HandshakeRejectedError{Addr: "10.0.0.1:22", Err: base}
	wantRe := "the SSH handshake with 10.0.0.1:22 failed: ssh: handshake failed: boom"
	if re.Error() != wantRe {
		t.Errorf("Error() = %q, want %q (the wording must stay byte-compatible, #116)", re.Error(), wantRe)
	}
	if !errors.Is(re, ErrHandshakeRejected) {
		t.Error("errors.Is(re, ErrHandshakeRejected) = false, want true")
	}
	if errors.Is(re, ErrHandshakeTimeout) {
		t.Error("the rejection type must not match ErrHandshakeTimeout")
	}
	if !errors.Is(re, base) {
		t.Error("Unwrap must keep the cause on the chain")
	}

	// As 走链：外层再包一层也照样判得出类型。
	wrapped := fmt.Errorf("retry budget exhausted: %w", to)
	var hte *HandshakeTimeoutError
	if !errors.As(wrapped, &hte) {
		t.Error("errors.As must find the typed error through wrapping")
	}
}
