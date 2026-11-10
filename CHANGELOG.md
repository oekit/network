# Changelog — oekit/network

## [v0.9.2] — 2026-11-11

- fix(nats): 订阅 handler 的 panic 就地收编——handler 跑在 nats.go 无 recover 的派发 goroutine（waitForMsgs 直接 mcb(m)）里，一条语义残缺消息的 panic 会穿透炸掉整个进程、连同其上全部订阅与在途请求一并消灭；现在 Subscribe/QueueSubscribe（覆盖 SubscribeJSON/QueueSubscribeJSON/Reply 全部漏斗）的 handler 调用点统一 panic 收编（Error 日志 + 该条消息丢弃留痕，与 wsutil「只死这条连接」同口径），订阅与进程存活（#145）

关联问题单：oekit/network#145。

- fix(wsutil): Outbox 写动作 panic 同样闭合记账——write panic 时旧实现整套记账与冲刷（failInFlight/countPending/drain）被跳过，「Send(true) − Written ≡ Dropped」恒等式破口、帧静默消失且 Dropped 恒为 0（#105/#117/#123 的口径只对 error 路径成立）；现在 write 调用点 panic 收编为 error 走既有失败分支（run 与 drain 两径同口径），写动作 panic 计入 Dropped（#146）

关联问题单：oekit/network#146。

- fix(nats): Close(ctx) 重连窗口内返回 nil——上游 Drain() 在连接重连中自己执行 nc.Close()（目标态已达成）却返回 ErrConnectionReconnecting，旧豁免只认 ErrConnectionClosed，把已关误记成 "drain: connection reconnecting" 虚假错误（上层关停误报告警/非零退出码/跳过清理）；现在 ErrConnectionReconnecting 与 ErrConnectionDraining 同列「目标态已达成」豁免（#147）

关联问题单：oekit/network#147。

## [v0.9.1] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.9.0] — 2026-11-10

- - feat(sshx): DialPolicy/DialWithPolicy 预算内按类重拨原语——预算跨尝试共享、拒绝默认不重拨（#125）

关联问题单：network#125。

## [v0.8.1] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.8.0] — 2026-11-10

- - feat(nats): EnsureStream 声明式流装配——流名缺省派生/subjects 缺省/存储策略与容量上限常量单源（#122）
- - fix(wsutil): Outbox 拆 Rejected 独立计数器，Send(true)−Written≡Dropped 恒等式在背压下成立（#123）

关联问题单：network#122、network#123。

## [v0.7.14] — 2026-11-09

- fix(sshx): 握手超时/拒绝类型化错误——errors.Is 可判别且措辞逐字兼容（#116）
- fix(wsutil): Outbox.Send 检查与入队原子化——Close 后帧不滞留（#117）

关联问题单：network#116、network#117。

## [v0.7.13] — 2026-11-08

- fix(conn): Manager.Stop 与重连竞态下已拨通连接约 50% 概率永不 Close 的 fd 泄漏——`connc` 改无缓冲，投递只能与 loop 接收会合，Stop 后必走 stopc 分支就地 Close（注释承诺的「必须就地 Close」真正兑现）（#98）
- fix(conn): `Exponential` 整数溢出回绕——导出函数对任何 int64 输入返回 (0, 1 分钟] 的退避；此前输入接近 MaxInt64 时 `d *= 2` 回绕成负数、躲过「> 1 分钟」钳制直接漏出负退避（#102）
- fix(nats): `JetStream.getOrCreateConsumer` 建 durable 消费者——此前只给 Name 的临时消费者空闲约 5s 被服务端回收，重建后 ack 水位丢失、整条流重投（#99）
- fix(nats): `Request`/`RequestJSON` 与 JetStream 系列对 nil ctx 兜底 Background——与 Publish 系列同仓口径，同一份客户端不再「一半容忍 nil、一半崩进程」（#101）
- fix(nats): `JetStream.Fetch`/`Next` 经 `jetstream.FetchContext` 把 ctx 注入取消息阶段——调用方取消不再等 SDK DefaultExpires（30s）；显式 FetchMaxWait 时以显式超时为准回退（SDK 禁止两者同用）；Consume/Messages 文档化「只由 Stop() 停止」（#103）
- fix(sshx): `DetectKeyType` 入口对 nil ctx 兜底 Background——带口令/损坏私钥必走的 ssh-keygen 分支此前被 exec.CommandContext 的 nil ctx panic 打穿（#79 口径补全）（#100）
- fix(sshx): `Dial` 的 Budget 按**总预算**实现、ctx 截止时间在拨号**之后**求值——慢连接下总耗时不再可接近 2×预算、不再超出 ctx 截止时间约 T；预算/ctx 被连接耗尽时立即放弃而非做一场没人等结果的握手（#106）
- fix(streamx): `SliceSource` 哨兵对齐 Source 接口契约——耗尽返回 io.EOF（`ErrStreamExhausted` 保留为别名），关闭返回新增 `ErrSourceClosed`、不再复用 Sink 语义的 `ErrSinkClosed`；Hub 侧「正常收尾不留痕」同步换新哨兵口径（#104）
- fix(wsutil): `Outbox` 写失败时「正在写的那一帧」计入 Dropped——「Send(true) − Written = Dropped」的计数闭合恒等式成立（drain 冲刷路径同口径）；新增 `Closed()` 拆分 Send 两种 false（队列满 vs 管道已关）（#105）

关联问题单：network#98、network#99、network#100、network#101、network#102、network#103、network#104、network#105、network#106。

## [v0.7.12] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.7.11] — 2026-11-07

- fix(conn): 失联期间 Put(err) 无害 no-op 且不吞已排定重连——初始拨号失败自愈路径补齐（主体已随 #64/v0.7.9 落地）（#81 #84）
- fix(streamx): Hub 每 Source 独立拉取 goroutine 并发汇聚——长驻 Source 阻塞 Next 不再饿死后续源（#82 #85）
- fix(nats): Close(ctx) error 等待真正排空完成、超时回退强制关闭——Drain 无限阻塞关停关闭（BREAKING：Close 签名改带 ctx，PR #91）（#83 #86）
- fix(wsutil): 按 Conn 串行写锁——广播等待超时后不再与下一轮并发写同一 Conn 损坏 websocket 帧（#87）
- fix(nats): RequestJSON 的 ctx 移至首参——与 Request 及 Go 惯例一致（BREAKING，PR #91）（#88）

关联问题单：network#81、network#82、network#83、network#84、network#85、network#86、network#87、network#88。

## [v0.7.10] — 2026-11-07

- fix(conn): NewDefaultManager 拨号带 DefaultDialTimeout=10s——裸 net.Dial 对黑洞对端不再挂 OS 级时长（#77）
- fix(wsutil): Broadcaster.Broadcast 拷贝调用方 buffer——buffer 复用不再串包，与 Outbox.Send 口径对齐（#78）
- fix(sshx/nats): Dial/Publish 系列对 nil ctx 兜底 Background——与同仓 WaitN/Acquire/DoValue 口径统一（#79）

关联问题单：network#77、network#78、network#79。

## [v0.7.9] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.7.8] — 2026-11-06

- fix(conn): 初始拨号失败排定退避重连（[#64](https://github.com/oekit/network/issues/64)）——reconnectc 此前只在「重连失败」与「putc 错误且 conn 非 nil」两处被赋值，初始拨号失败时两条路径均不可达 ⇒ loop 再无任何拨号被排定 ⇒ 服务启动时依赖端短暂不可达（最常见场景），之后即使对端恢复 Manager 也永不重试，Take 恒 nil；而「已建连后断开」路径有完整指数退避重连——明显不对称。修法：初始失败同样排定退避重连（与断连路径对称）。回归：注入「前 2 次失败、第 3 次成功」的 dialer + 手动 after 通道，拨号次数必须 ≥3
- fix(streamx): Hub 多源调度改轮询 + 源错误留痕（[#65](https://github.com/oekit/network/issues/65)、[#67](https://github.com/oekit/network/issues/67)）——此前 `for _, src := range sources { for { ... } }` 是逐源耗尽式：长活源挂在前时其余源**一条都不会被转发**（issue 实测源 1 产出 782 万条、源 2 转发 0 条）；且非耗尽的源错误与耗尽同走同一个 break，**无任何日志**（sink 侧反而有 Warn）——类型文档承诺的「任一失败仅记录」在源侧是空头支票。修法：活跃源下标表 round-robin（每源每轮取一条，耗尽/出错源退出轮询集，其余源继续）；非耗尽错误补 Warn（与 sink 侧同面）

关联问题单：network#64、network#65、network#67。

## [v0.7.7] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.7.6] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.7.5] — 2026-11-02

- fix(wsutil): 时序敏感的 flaky 测试——写失败记账用例改为自控写协程时机（#54）

## [v0.7.4] — 2026-11-02

- build: 矩阵内依赖地板 `concurrency` v0.23.2 → v0.23.3（维护轮全量对齐；本版不含 API 变更）

## [v0.7.3] — 2026-10-30

- build: 矩阵内依赖地板 `tracing` v0.3.1 → v0.3.2（维护轮全量对齐；本版不含 API 变更）

## [v0.7.2] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

## [v0.7.1] — 2026-10-29

- fix(wsutil): `Outbox` 的契约补齐——`Done` 的说明、丢帧记账、`Send` 拷贝、`write` 的硬约束

独立审阅抓到 4 条，逐条修（都是**契约面**的，机制本身没错）：

1. **`Done()` 的注释给的是一条会截断帧的处方**：它写着「等 Done 再关底层连接」，而 `done` 是在 `Close()` 里**立刻**关闭的——那时写协程还卡在 select 里，队列里的帧**一帧都没冲刷**。照文档做 ⇒ 关连接 ⇒ 帧全丢。而**库自己的测试用的是 `Wait()` 不是 `Done()`** ⇒ 文档与测试互相矛盾。现在写明：`Done` 只表示「已关闭，不再接受新帧」，**不代表已冲刷**；正确顺序是 `Close()` → `Wait()` → 关底层连接。
2. **写协程死掉时已入队的帧无声消失**：`Dropped()` 只在「队列满」那条分支上加，而写失败退出时队列里剩下的帧被直接丢弃——`Send` 已经返回过 true（=「进了队列」），调用方据此认为帧已交付。现在写失败时先把剩余帧计入 `Dropped()`，另加 `Written()` 让调用方能自己算差值。
3. **`Send` 不拷贝入参，所有权契约没写**：写协程是**稍后**才写帧的，而调用方很可能复用同一个 buffer——不拷贝的话上线的是调用方改过之后的字节，症状是「客户端收到坏 JSON」，而服务端什么错都不报。现在 `Send` 拷贝一份（一次分配，换来一整类隐蔽坏字节的消除），并写明代价。
4. **`Wait()` 无上限，`Close()` 打不断在途写**：`write` 必须自带超时或可中断，否则对端半开时 `Wait()` 永远不返回 ⇒ 调用方的关停路径挂死、goroutine 泄漏。现在写进 `NewOutbox` 的契约。

测试新增：Send 拷贝（调用方复用 buffer 后上线仍是原字节）、写失败时丢帧被记账、Done 已响但帧未冲刷（证明 Done ≠ 已冲刷）。

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

- feat(peercred): unix socket 对端凭据原语 + 受信判定（取不到 ⇒ 不信任）（#30）

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

