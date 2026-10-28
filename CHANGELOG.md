# Changelog — oekit/network

## [v0.7.0] — 2026-10-29

- feat(wsutil): 补 `Outbox`——单条连接的有界出站管道

websocket 实现（gorilla 等）**不允许并发写**：两个写者同时写同一条连接会互相踩帧，症状是客户端收到乱序或半截的帧，而服务端这边什么错都不报。所以每条连接必须有且只有一个写者，而那个写者只能由一个 goroutine 驱动 ⇒ 出站帧必须先进队列，由它独家取走。

队列还必须有界：客户端读得慢时，写者会卡在写上（TCP 背压），队列涨满；无界的队列会把「一个慢客户端」变成「服务端内存涨满」。满了怎么办是**调用方**的决定：`Send` 只报告「入队成功没有」，**不替业务选策略**。同一个「满了」在不同场景下有多种正确处置——断开这条连接、断开对端、丢掉这一帧、退出本端——而库替业务选一个，就把差异藏起来了。

`Outbox` 把这套做成库无关的原语（写入动作就是一个函数 ⇒ gorilla / hertz-contrib / 任何自定义传输都能用）：

- `Send` **永不阻塞**：队列满时返回 false，**不替调用方做处置**（断开这条 / 断开对端 / 丢帧 / 退出本端，由调用方按返回值决定）
- `Close` 幂等；关闭后写协程**冲刷队列里剩余的帧**再退出（正在推的最后一帧不该因为对端刚要走就被丢掉）
- `Send` 在关闭之后返回 false，**不 panic**（队列通道永不 close ⇒ 没有「往已关闭的 channel 发送」这个坑）
- `Dropped()` 报被丢弃的帧数：非零说明有客户端读得比服务端推得快
- 写协程 panic 由 `async.GoSafeLabeled` 收编：只死这条连接，不死整个进程

**为什么值得做成原语**：这套「唯一写者 + 有界队列 + 慢消费者策略」是每个 websocket 服务端都要重写一遍的（此前每个仓各写一份：单广播循环串行写、或每连接一个写协程）。

## [v0.6.1] — 2026-10-29

- feat(sshx): 补 `ParseSinglePublicKey`——解析**恰好一把**公钥

`ParsePublicKey` 按 authorized_keys 的语法逐行扫描：解析不了的行被跳过，取第一条能解析的。于是「第一行 ed25519、第二行 rsa」会被判成 rsa——而调用方接下来通常是把**整个字符串**存下去，堡垒机存的是两把钥匙，且会安静地接受那把 ed25519。**被检查的东西与被存下的东西不是同一个**，这类漏洞极难查。

`ParseSinglePublicKey` 拒掉这种输入（多行 / rest 非空），并说清它与 `ParsePublicKey` 的分工：后者答「这里面有**一把**能用的钥匙吗」，前者答「这里面**只有**一把能用的钥匙吗」。要把这段字符串原样存进某个系统时，后者才是该问的那个。

- fix(sshx): 握手超时的判定改走 `errors.As` —— 原来那条分支**是死代码**

`x/crypto/ssh` 的 `NewClientConn` **永远**把错误包一层（`fmt.Errorf("ssh: handshake failed: %w", err)`）⇒ 返回值的动态类型是 `*fmt.wrapError`，**不实现 `net.Error`** ⇒ 直接类型断言 `herr.(net.Error)` **恒假** ⇒ 「did not finish within」这句话**永远不会出现**，握手超时被说成了「handshake failed」。

为什么这条措辞值得修：**调用方靠它区分「超时」与「认证被拒」**，两者应对方式不同（超时换一条路重试，被拒则是凭据问题）。上游 `sshx.Dial` 的措辞与它取代的那份本地实现逐字相同，所以**本仓没有行为变化**；但那份本地实现搬上来时带着同一条假断言——现在一次修对，两边都对。

判据从类型断言换成走错误链：`var ne net.Error; if errors.As(herr, &ne) && ne.Timeout()`。测试里加了一条断言（超时必须有专属措辞）——修之前它会红。

## [v0.6.0] — 2026-10-29

- feat(sshx): SSH 客户端原语 —— 拨号、私钥、密钥类型识别、保活

`x/crypto/ssh` 给的是协议原语，**拨号这一步要做的判断一个都没替你做**：连接超时、握手超时、调用方的总预算、以及「超时」与「被拒绝」这两种失败的区分，都要自己写一遍。写错一次的后果不是报错难看，而是调用方永远等下去。

本 PR 把这几件事做成可复用原语（此前每个要连 SSH 的仓都要重写一遍）：

- `Dial(ctx, addr, cc, opts)`：**两段界限**的拨号。连接与握手分开界——`ClientConfig.Timeout` 只管 TCP 连接，**之后的握手跑在裸连接上没有任何截止时间**：一个接受连接却从不说 SSH 的端口（透明代理、空转的端口转发、回了 SYN 就沉默的中间盒）会让调用方永远等下去，而降级路径永远轮不到它（快路径没有「失败」，它还在等）。握手界限设在 socket 上、握完就撤（一条跑几分钟的命令不能被那个「为了握手」的截止时间砍断）。`DialOptions.Budget` 是调用方给这次调用的总预算，同时压住两段界限：给了两秒预算的调用方不该坐在五秒握手里等一个自己不会用的结果；ctx 的截止时间同样压住握手
- **超时与拒绝分开说**：拒绝（`unable to authenticate`）是让调用方去降级的信号，超时绝不能被打扮成一个拒绝——两者的应对方式不同，揉成同一句话就等于把降级也关掉了
- `LoadSigner` / `DetectKeyType` / `PublicKeyType` / `ParsePublicKey`：凭据这一侧。**「密钥类型不对」在部署里极难排查**——服务端常常接受并报告成功（字段写进去了、状态是好的），而客户端在登录时才拒绝它；两端对同一个密钥的判断不一致时，中间没有任何东西会说明原因。所以「这个密钥是什么类型」得有一个能提前问的答案。`DetectKeyType` 三级回落（直接解析私钥最准且不需要外部进程 → `ssh-keygen -y` 兜带口令的私钥 → 文件头嗅探兜底）
- `StartKeepAlive`：空闲连接保活。一条池化的连接在调用之间闲着一两秒，而 NAT 表或防火墙的空闲超时可能远在这之前就把它关掉 ⇒ 连接静默地死了，下次调用拿到的是一条死掉的 socket。保活用的是 OpenSSH 自己的 keepalive 请求，**不等回包**（等回包意味着一个既不应也不关的服务端会让这个请求卡在连接的整个余生里）

**为什么落在 network 而不是 pkg**：`pkg` 是零第三方依赖仓（go.mod 零 require，CI 强制），SSH 需要 `x/crypto` ⇒ 进不了它。network 本就是「连接与凭据」层（`conn` 连接管理、`peercred` 对端凭据），`x/crypto` 在它这里已是被依赖方（nats 链带入）⇒ 新增子包不新增仓库、不动依赖地板，只把 `x/crypto` 从 indirect 提升为直接依赖（版本守恒，未降级任何依赖）。

## [v0.5.7] — 2026-10-29

- feat(peercred): unix socket 对端凭据原语 + 受信判定（取不到 ⇒ 不信任）

以 unix socket 对外提供服务的模块都要回答「这条连接的对端是不是可信的」，而通行做法「socket 文件权限就是边界」**实测不成立**（fnOS 真机 2026-10-28：socket 权限 755、others 只有 r-x，非属主的本地用户实际 connect 成功）⇒ 任何「能连上就算可信」的设计，任意本地用户都能冒充任意对端身份。

本 PR 把「按对端凭据判定」做成可复用原语（此前每个做 local socket 的仓都要重写一遍：消费方 pigeonbox/fnos 为此新增 peercred_linux.go / peer.go / 网关身份剥离逻辑，≈120 行生产代码 + 4 条测试）：

- `PeerUID(c net.Conn) (uid uint32, ok bool)`：Linux 上走 `syscall.Conn` → `SyscallConn()` → `getsockopt(SOL_SOCKET, SO_PEERCRED)`；**只依赖标准库**（`syscall.Ucred` / `syscall.GetsockoptUcred` 是 Linux 专有 ⇒ 放在 `//go:build linux` 文件里）⇒ network 不新增任何依赖
- 非 Linux：返回「未知对端」（ok=false），**不静默放行**
- `PeerTrust`：受信 uid 集合 + 判定。**取不到凭据 ⇒ 一律不可信**（`Trusted(uid, false)` ⇒ false）⇒ 宁可显式失败，不可无来源采信身份
- `ConnContext`：可直接挂到 `http.Server.ConnContext`，把对端 uid 注入请求 ctx

两个实现取舍：

- **零新增依赖**：用标准库的 Linux 专有符号，network 的 go.mod 不动
- **一个刻意的不对称**：`ConnContext` 在**取不到凭据时不注入**（而不是注入 0）——uid=0 在 Linux 上是 root，把未知对端写成 0 等于「未知 ⇒ 按 root 采信」，是最危险的错法（调用方拿到 ok=true 就会判 trusted）；不注入 ⇒ 读出来就是「未知」⇒ fail-closed。这条是本 PR 的测试自己抓出来的：初版无条件注入 0，两条用例立刻变红。

两个由 CI 抓到的 Linux 专属缺陷（本地 darwin 测不出）：

- 对**非 unix** 连接（TCP）调 SO_PEERCRED，Linux 上会「成功」返回一个**垃圾** ucred（Uid=4294967295）而不是报错 ⇒ 只看 getsockopt 成败会把 TCP 对端判成「uid=4294967295 的已知身份」⇒ 修法：先用 getsockname 拿地址族（unix 连接返回 *syscall.SockaddrUnix），不是 unix 就判「未知」（fail-closed，不依赖魔数常量）
- Linux 专项用例里 Accept/Atoi 的 err 遮蔽外层 err（govet shadow）

## [v0.5.6] — 2026-10-23

- docs: 补 v0.1.1 旁支版本事实声明——该版本不在主线版本线上（发布时刻晚于 v0.5.1），当前请用主线版本（#24）

## [v0.5.5] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0

## [v0.5.4] — 2026-10-18

- fix(deps): tracing v0.2.2 地板对齐（零代码改动）

## [v0.5.3] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.5.2] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.5.1] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——concurrency v0.5.2、logx v0.5.0、tracing v0.2.1；go 维持 1.26.0（tracing/OTel 生态传导）

## [v0.5.0] — 2026-10-17

全新起点：历史与旧版本号已全部清空，自本版重新起算。

- **模块布局收平**：包从 `network/<name>` 双层目录提为仓库根一级
  （`conn/`、`mq/`、`nats/`、`streamx/`、`wsutil/`），import 路径为
  `github.com/oekit/network/<name>`，不再多一层 `network/network/`。
- **模块边界**：通用轻量网络客户端集——连接管理、消息队列抽象、NATS、
  流工具、WebSocket 广播。业务推送客户端不属于本模块，不在范围内。
- 包能力面：
  | 包 | 做什么 |
  |---|---|
  | `conn` | 连接管理 |
  | `mq` | 消息队列抽象 |
  | `nats` | NATS 客户端（嵌入式测试友好）+ JetStream |
  | `streamx` | 流工具（事件、Hub、SSE sink） |
  | `wsutil` | WebSocket 广播器（房间注册、背压安全） |
## [v0.1.1] — 2026-10-17

- fix(deps): 内部依赖地板 logx v0.2.0 → v0.5.0；go 维持 1.26.0——k8s.io/client-go 生态声明 go1.26，暂不做生态级回退

> **⚠️ 本版本不在主线版本线上**（旁支版本，事实声明见 [oekit/network#24](https://github.com/oekit/network/issues/24)）
>
> - 本仓主线自 **v0.5.0**（「全新起点：旧版本号已全部清空，自本版重新起算」）起算；**v0.1.1 不是该主线上的版本**。
> - 发布时刻 **2026-10-17 16:53:24 +0800**，**晚于**同线的 `v0.5.1`（16:51:45）——版本号在时间线上回退，系发布时的笔误。
> - 它的**内容实为 `v0.5.1` 前 2 分钟的树**（`18d76ce`「fix(deps): logx v0.5.0 地板」），只是版本号看起来像「远古版本」。
> - **当前请使用主线版本**（`v0.5.5+`）。
> - tag **保留不删**：已发布 tag 在 Go module proxy 上有缓存，删 tag / 重推 = 已缓存消费方集体中毒。

