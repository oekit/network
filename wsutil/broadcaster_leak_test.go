package wsutil

import (
	"runtime"
	"testing"
	"time"
)

// broadcaster_leak_test.go 是 #142 的**泄漏检测**面（与 broadcaster_test.go
// 的计数断言互补）：不触碰 writeMu 内部表示，在「修复前源码 + 本文件」上
// 也能编译并以断言失败复现——修复前 sync.Map 条目持强引用，Unregister
// 后 Conn 永不可 GC；修复后条目随断开回收，finalizer 正常触发。

// TestBroadcaster_UnregisterReleasesConnForGC 回归（#142 泄漏本体）：
// Unregister（最后一个房间）后 Broadcaster 对 Conn 的引用必须全部释放。
//
// 轮换型推送服务（断线重连、前后台切换、滚动发布）下，每轮换泄漏一个
// Conn 全量对象（含读写缓冲与底层 net.Conn 匡装）：RSS 随历史连接总数
// 线性增长、永不回落。finalizer 不触发 = 强引用仍在 = 泄漏本体。
func TestBroadcaster_UnregisterReleasesConnForGC(t *testing.T) {
	b := NewBroadcaster()
	defer b.Close()

	finalized := make(chan struct{})
	conn := &mockConn{}
	runtime.SetFinalizer(conn, func(*mockConn) { close(finalized) })

	b.Register("room", conn)
	b.Broadcast("room", []byte("m"))
	waitFor(t, "write delivered", func() bool { return conn.writtenCount() == 1 })
	b.Unregister("room", conn)
	waitFor(t, "room emptied", func() bool { return b.GetRoomCount("room") == 0 })
	// Unregister 收尾会关物理连接（离开最后一个房间时）
	waitFor(t, "conn closed", func() bool { return mockConnCloseCount(conn) >= 1 })

	// 丢掉测试侧引用，轮询 GC 直到 finalizer 触发。
	conn = nil
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-finalized:
			return // Conn 可回收（修复后）
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("Broadcaster 仍钉住已断开的 Conn（finalizer 未触发）——writeMu 条目泄漏（#142）")
}

// mockConnCloseCount 读 mockConn 的 Close 计数（同包，跨文件测试助手）。
func mockConnCloseCount(m *mockConn) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}
