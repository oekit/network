//go:build linux

package peercred

import (
	"net"
	"syscall"
)

// peerUID 用 SO_PEERCRED 取对端凭据。
//
// 路径：net.Conn → syscall.Conn（Unix 域 socket 支持）→ SyscallConn() →
// 原样取出 fd → getsockopt(SOL_SOCKET, SO_PEERCRED)。
//
// 取不到（连接不是 unix socket / 不支持 SO_PEERCRED / 系统调用失败）⇒ ok=false。
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
	// 控制钩子里不能做阻塞操作，但 getsockopt 是即时的 ⇒ 安全。
	ctrlErr := raw.Control(func(fd uintptr) {
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
