package wsutil

// outbox_panic_test.go 钉住 #146 的修复：写动作 write **panic**（不是返回
// error）时，整套记账（failInFlight/countPending/drain）不得被跳过——
// 「Send(true) − Written ≡ Dropped」恒等式必须在 panic 路径上同样闭合。
//
// issue 复现形态：write 对某帧 panic ⇒ GoSafe 收编后 run() 直接 unwind，
// 正在写的帧与队列里余下的帧既不计 Written 也不计 Dropped，帧静默消失且
// Dropped 恒为 0；调用方按 Close → Wait → 关底层连接 收尾，Wait 正常返回，
// 以为「都写完了」——写坏路径完全失明（#105/#117/#123 三轮修复闭合的口径
// 只对 error 路径成立，对 panic 路径仍假）。

import (
	"sync/atomic"
	"testing"
)

// TestOutbox_WritePanicClosesAccounting 回归（#146）：write panic 与写失败
// 同口径——正在写的那一帧 + 队列余帧全部计入 Dropped，恒等式闭合。
//
// 时序（channel 同步，无 sleep）：第一次写被 gate 挡住，等 3 帧全部入队后
// 放行 ⇒ write panic ⇒ 记账必须照常闭合。
func TestOutbox_WritePanicClosesAccounting(t *testing.T) {
	gate := make(chan struct{})
	o := NewOutbox(func([]byte) error {
		<-gate
		panic("write panic (simulated)")
	}, OutboxOptions{QueueSize: 8})

	sentTrue := 0
	for i := 0; i < 3; i++ {
		if o.Send([]byte("x")) {
			sentTrue++
		}
	}
	if sentTrue != 3 {
		t.Fatalf("Send returned true %d times, want 3", sentTrue)
	}
	close(gate) // 放行：这次 write panic
	o.Wait()    // 写协程退场后计数静止

	if o.Written() != 0 {
		t.Errorf("Written = %d, want 0 (the write panicked)", o.Written())
	}
	// 恒等式（#146 的核心断言）：panic 路径同样闭合——1 帧在途 panic +
	// 2 帧随队列消失，全部计入 Dropped。
	if got := o.Dropped(); got != 3 {
		t.Errorf("Dropped = %d, want 3 (1 in-flight panicked frame + 2 left in the queue — the write panic must close the accounting, #146)", got)
	}
	if diff := int64(sentTrue) - o.Written(); diff != o.Dropped() {
		t.Errorf("Send-true(%d) − Written(%d) = %d ≠ Dropped(%d) — the identity must hold on the panic path too (#146)",
			sentTrue, o.Written(), diff, o.Dropped())
	}
	if !o.Closed() {
		t.Error("Closed = false after a write panic, want true (a panicking write means the pipe is dead)")
	}
	if n := len(o.queue); n != 0 {
		t.Errorf("%d frame(s) left in the queue after Wait", n)
	}
	// 进程仍存活（panic 已被收编）且管道已关：后续 Send 拒收、不计账。
	if o.Send([]byte("late")) {
		t.Error("Send after the write panic must refuse the frame")
	}
}

// TestOutbox_DrainWritePanicCountsInFlight 回归（#146）的冲刷路径：Close 之后
// drain 里的 write panic 同样不得跳过记账——正在写的那一帧计入 Dropped。
func TestOutbox_DrainWritePanicCountsInFlight(t *testing.T) {
	var calls atomic.Int32
	taken := make(chan struct{})
	gate := make(chan struct{})
	o := NewOutbox(func([]byte) error {
		if calls.Add(1) == 1 {
			close(taken)
			<-gate
			return nil // 第一帧正常写出
		}
		panic("drain write panic (simulated)")
	}, OutboxOptions{QueueSize: 8})

	if !o.Send([]byte("a")) || !o.Send([]byte("b")) {
		t.Fatal("Send refused")
	}
	<-taken     // 写协程已取走第一帧（挂在 gate 上）
	o.Close()   // Close ⇒ 写协程走 drain 分支（先冲刷再退出）
	close(gate) // 放行：第一帧成功、第二帧 panic
	o.Wait()

	if got := o.Written(); got != 1 {
		t.Errorf("Written = %d, want 1", got)
	}
	if got := o.Dropped(); got != 1 {
		t.Errorf("Dropped = %d, want 1 (the in-flight frame whose write panicked during drain, #146)", got)
	}
	if diff := int64(2) - o.Written(); diff != o.Dropped() {
		t.Errorf("Send-true(2) − Written(%d) = %d ≠ Dropped(%d) — the identity must hold on the drain panic path too (#146)",
			o.Written(), diff, o.Dropped())
	}
}
