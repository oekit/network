//go:build linux

package peercred

import (
	"errors"
	"net"
	"syscall"
)

// peerUID 用 SO_PEERCRED 取对端凭据。
//
// 路径：net.Conn → syscall.Conn（Unix 域 socket 支持）→ SyscallConn() →
// 原样取出 fd → getsockopt(SOL_SOCKET, SO_PEERCRED)。
//
// **必须先确认连接确实是 unix 域 socket**（2026-10-28 Linux CI 实测）：
// 对**非 unix** 连接（TCP）调 SO_PEERCRED，Linux 上会「成功」返回一个**垃圾**
// ucred（Uid=4294967295 = ^uint32(0)）而不是报错 ⇒ 若只看 getsockopt 的成败，
// 就会把 TCP 对端判成「uid=4294967295 的已知身份」⇒ 判定完全失效。
// ⇒ 这里先用 getsockname 拿地址族（unix 连接返回 *syscall.SockaddrUnix），
// 不是 unix 就直接判「未知」（fail-closed）。
//
// 取不到（不是 unix socket / 不支持 SO_PEERCRED / 系统调用失败）⇒ ok=false。
func peerUID(c net.Conn) (uint32, bool) {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return 0, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var uid uint32
	var credErr error
	// 控制钩子里不能做阻塞操作，但 getsockopt/getsockname 是即时的 ⇒ 安全。
	ctrlErr := raw.Control(func(fd uintptr) {
		sa, err := syscall.Getsockname(int(fd))
		if err != nil {
			credErr = err
			return
		}
		if _, isUnix := sa.(*syscall.SockaddrUnix); !isUnix {
			credErr = errNotUnixSocket
			return
		}
		ucred, err := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if err != nil {
			credErr = err
			return
		}
		uid = ucred.Uid
	})
	if ctrlErr != nil || credErr != nil {
		return 0, false
	}
	return uid, true
}

// errNotUnixSocket 表示连接不是 unix 域 socket（取不到对端凭据）。
var errNotUnixSocket = errors.New("peercred: not a unix domain socket")
