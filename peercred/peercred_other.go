//go:build !linux

package peercred

import "net"

// peerUID 非 Linux 平台：返回「未知对端」。
//
// **不静默放行**：调用方拿到的 ok=false ⇒ 按「不可信」处理（见 PeerTrust.Trusted）。
//
// 其它平台各有各的对端凭据机制（macOS 的 LOCAL_PEERCRED / FreeBSD 的
// getpeereid 等），本包暂不实现 ⇒ 语义统一为「取不到」。
func peerUID(_ net.Conn) (uint32, bool) {
	return 0, false
}
