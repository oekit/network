# Changelog — oekit/network

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

