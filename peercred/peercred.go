// Package peercred 提供 unix socket 对端的**可核验身份**原语。
//
// # 为什么需要它
//
// 以 unix socket 对外提供服务的模块（网关代理、IPC 守护、sidecar 通信）都要回答：
// **这条连接的对端是不是可信的？**
//
// 通行做法是「socket 文件权限就是边界」——但**实测不成立**：非属主的本地用户
// 在某些 ACL/挂载环境下能成功 connect（fnOS 真机实测 2026-10-28：socket 权限
// 755、others 只有 r-x，非属主用户实际连上了）⇒ 任何「能连上就算可信」的设计，
// 任意本地用户都能冒充任意对端身份。
//
// ⇒ 身份类断言必须按**对端凭据**判定（Linux：`SO_PEERCRED` 取对端 uid），
// 而不是按「连上了」推断。
//
// # 语义：**取不到 ⇒ 不信任**
//
// PeerUID 取不到凭据时返回 ok=false；PeerTrust.Trusted 对 known=false 一律返回
// false ⇒ **宁可显式失败，不可无来源采信身份**。
//
// # 跨平台
//
// 非 Linux 平台返回「未知对端」（ok=false），**不静默放行**。
//
// # 文档警示
//
// socket 文件权限在部分平台/ACL 环境下**不是**可靠边界 ⇒ 身份类断言一律按对端凭据
// 判定。本包与「把 socket 权限收紧（0660 + 专属组）」是**互补**关系，不是替代。
package peercred

import (
	"context"
	"net"
)

// PeerUID 返回连接对端的 uid（Linux 上是发起 connect() 的那个进程的 uid）。
//
// 取不到凭据时 ok=false —— 调用方**必须**按「不可信」处理（见 PeerTrust）。
//
// 注意：这是**连接级**的身份（connect 时的快照），不是对端当前的身份；
// 且 uid 可被复用 ⇒ 长时间持有连接的场景请按业务需要叠加其它校验。
func PeerUID(c net.Conn) (uid uint32, ok bool) {
	return peerUID(c)
}

// PeerTrust 受信对端判定：受信 uid 集合 + 判定。
//
// 用法：
//
//	trust := peercred.NewPeerTrust(0, 1000) // root 与 worker 用户
//	uid, ok := peercred.PeerUID(conn)
//	if !trust.Trusted(uid, ok) {
//	    // 对端不可信 ⇒ 剥离其携带的身份断言并告警
//	}
type PeerTrust struct {
	uids map[uint32]bool
}

// NewPeerTrust 建立受信 uid 集合（重复 uid 无副作用）。
func NewPeerTrust(uids ...uint32) *PeerTrust {
	t := &PeerTrust{uids: make(map[uint32]bool, len(uids))}
	for _, uid := range uids {
		t.uids[uid] = true
	}
	return t
}

// Trusted 判定某次连接是否受信。
//
// **known=false（一律取不到凭据）⇒ false**：取不到凭据不是「无人可信」，而是
// 「无法证明可信」⇒ 按不可信处理。
func (t *PeerTrust) Trusted(uid uint32, known bool) bool {
	if !known {
		return false
	}
	return t.uids[uid]
}

// UIDs 返回受信 uid 集合的副本（便于日志/调试，不影响判定）。
func (t *PeerTrust) UIDs() []uint32 {
	out := make([]uint32, 0, len(t.uids))
	for uid := range t.uids {
		out = append(out, uid)
	}
	return out
}

// peerUIDContextKey 是 ctx 里存对端 uid 的键（私有类型 ⇒ 外部无法伪造）。
type peerUIDContextKey struct{}

// WithPeerUID 把**已知的**对端 uid 注入 ctx。
//
// 调用方只应在 PeerUID 返回 ok=true 时调用它——**未知对端不要注入**：
// uid=0 在 Linux 上是 root ⇒ 把未知对端注入成 0 等于「未知 ⇒ 按 root 采信」，
// 是最危险的一种错法（调用方拿到 ok=true 就会判 trusted）。未知就不注入 ⇒
// PeerUIDFromContext 返回 ok=false ⇒ 按不可信处理（fail-closed）。
func WithPeerUID(ctx context.Context, uid uint32) context.Context {
	return context.WithValue(ctx, peerUIDContextKey{}, uid)
}

// PeerUIDFromContext 取出 ctx 里的对端 uid；未注入时 ok=false。
func PeerUIDFromContext(ctx context.Context) (uid uint32, ok bool) {
	v := ctx.Value(peerUIDContextKey{})
	if v == nil {
		return 0, false
	}
	u, k := v.(uint32)
	return u, k
}

// ConnContext 返回一个可直接挂到 http.Server.ConnContext 的函数：把对端 uid
// 注入请求 ctx。
//
// **取不到凭据时不注入**（⇒ ctx 里没有 uid ⇒ PeerUIDFromContext 返回 ok=false
// ⇒ 按不可信处理）。不注入成 0 是有意的：uid=0 是 root，把未知对端写成 0 等于
// 「未知 ⇒ 按 root 采信」，是最危险的错法。
//
// 用法：
//
//	srv := &http.Server{ConnContext: peercred.ConnContext(nil)}
//
// 传 nil 表示不保留调用方原有的 ConnContext 逻辑。
func ConnContext(base func(ctx context.Context, c net.Conn) context.Context) func(context.Context, net.Conn) context.Context {
	return func(ctx context.Context, c net.Conn) context.Context {
		if base != nil {
			ctx = base(ctx, c)
		}
		uid, ok := PeerUID(c)
		if !ok {
			return ctx // 未知 ⇒ 不注入 ⇒ 按不可信处理
		}
		return WithPeerUID(ctx, uid)
	}
}
