package streamx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSSESink_IDPassThroughForJSONPayloads 钉住 2026-09-06 修复:带 ID 的
// JSON 载荷必须走 SendRaw 透传 ID(Last-Event-ID 断线续传依赖),
// 原实现走 Send 把 ID 丢掉。
func TestSSESink_IDPassThroughForJSONPayloads(t *testing.T) {
	var gotID, gotType string
	var gotData []byte
	sink := NewSSESinkFromFuncs(
		func(eventType string, data any) error {
			t.Errorf("Send should not be called for ID-carrying events, got type=%s", eventType)
			return nil
		},
		func(id, eventType string, data []byte) error {
			gotID, gotType, gotData = id, eventType, data
			return nil
		},
		func() error { return nil },
		func() error { return nil },
	)

	err := sink.Emit(context.Background(), Event{
		ID:   "seq-42",
		Type: "progress",
		Data: map[string]any{"percent": 50},
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if gotID != "seq-42" {
		t.Errorf("id = %q, want seq-42 (ID 透传)", gotID)
	}
	if gotType != "progress" {
		t.Errorf("type = %q, want progress", gotType)
	}
	if !strings.Contains(string(gotData), "percent") {
		t.Errorf("data = %s, want JSON-encoded payload", gotData)
	}
}

// --- #144 回归：Emit 的 closed 检查与 stream 写入必须原子 ---

// gateMarshaler 在 json.Marshal 期间阻塞（MarshalJSON 是 Emit「closed 检查
// 通过后、stream 写入前」窗口里唯一可编排的暂停点），把 #144 的
// check-then-act 缝隙变成确定性可复现的交错。
type gateMarshaler struct {
	entered chan struct{}
	release chan struct{}
}

func (m *gateMarshaler) MarshalJSON() ([]byte, error) {
	close(m.entered)
	<-m.release
	return []byte(`"gated"`), nil
}

// streamEventLog 记录 stream 侧事件序（互斥保护）：「write」与「close」的
// 全序关系是 #144 的判据——write 不得落在 close 之后。
type streamEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *streamEventLog) add(ev string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *streamEventLog) index(ev string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.events {
		if e == ev {
			return i
		}
	}
	return -1
}

func (l *streamEventLog) count(ev string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e == ev {
			n++
		}
	}
	return n
}

func (l *streamEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// TestSSESink_CloseDoesNotInterleaveWithInFlightEmit 回归（#144，#117 同构
// 竞态换包复活）：Emit 的「closed 检查 + stream 写入」非原子时，通过检查
// 的 Emit 会在 Close 之后把事件写进**已关**的 stream——返回底层 stream
// 错误而非 ErrSinkClosed（契约「Close 后 Emit 返回 ErrSinkClosed」被击穿），
// Hub 把正常关闭当 sink 故障留痕。修复后两步纳入同一临界区（Emit 持
// RLock、Close 持 Lock，照 wsutil.Outbox #117 修法）：写入只发生在 stream
// 关闭之前，交错后的 Emit 一律 ErrSinkClosed。
//
// 编排（屏障/事件序，不靠 sleep 赌交错）：MarshalJSON 把 Emit 精确暂停在
// 「检查已过、写入未发」的缝隙里；此时并发 Close——修复前 Close 无临界区、
// 立即落「close」（等它落地再放行写，交错必现）；修复后 Close 挂在写锁上
// 等在途 Emit，等待窗内 close 不出现（空等即「Close 等在途 Emit」语义本身）。
func TestSSESink_CloseDoesNotInterleaveWithInFlightEmit(t *testing.T) {
	gm := &gateMarshaler{entered: make(chan struct{}), release: make(chan struct{})}
	var log streamEventLog
	var streamClosed atomic.Bool
	sink := NewSSESinkFromFuncs(
		func(string, any) error { log.add("send"); return nil },
		func(_, _ string, _ []byte) error {
			// 模拟 hertz stream 对「写已关连接」的反应：底层报错。
			if streamClosed.Load() {
				log.add("write")
				return errors.New("hertz: write to closed stream")
			}
			log.add("write")
			return nil
		},
		func() error { return nil },
		func() error {
			streamClosed.Store(true)
			log.add("close")
			return nil
		},
	)

	var wg sync.WaitGroup
	var emitErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		emitErr = sink.Emit(context.Background(), Event{ID: "seq-1", Type: "progress", Data: gm})
	}()
	<-gm.entered // 确定性：Emit 已过 closed 检查、停在写入前的缝隙

	closeCalling := make(chan struct{})
	closeDone := make(chan struct{})
	var closeErr error
	go func() {
		defer close(closeDone)
		close(closeCalling)     // 标记「即将 Close」；不等它返回——修复后 Close
		closeErr = sink.Close() // 会等在途 Emit（写锁），返回被推迟属预期语义
	}()
	<-closeCalling

	// 修复前：Close 无临界区、无任何阻塞，等待窗内必然落地「close」；
	// 修复后：Close 等在途写，窗内不出现——空等即修复语义本身，非 flaky。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && log.index("close") < 0 {
		time.Sleep(2 * time.Millisecond)
	}

	close(gm.release) // 放行写：交错窗口在此闭合
	wg.Wait()
	<-closeDone

	evs := log.snapshot()
	iClose, iWrite := log.index("close"), log.index("write")
	if iClose >= 0 && iWrite > iClose {
		t.Fatalf("事件序 %v：write 落在 close 之后——Emit 写进已关 stream（#144 check-then-act 竞态）", evs)
	}
	if emitErr != nil && !errors.Is(emitErr, ErrSinkClosed) {
		t.Fatalf("交错中的 Emit 返回底层流错误而非 nil/ErrSinkClosed：%v（#144 契约违背）", emitErr)
	}
	if err := errors.Join(emitErr, closeErr); err != nil {
		t.Fatalf("concurrent emit/close: %v", err)
	}

	// 契约后置（并发形态下同样成立）：Close 返回后再 Emit 一律
	// ErrSinkClosed、不再触碰 stream；stream.Close 只下发一次（幂等）。
	if err := sink.Emit(context.Background(), Event{Type: "after"}); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("post-close Emit err = %v, want ErrSinkClosed", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := log.count("close"); got != 1 {
		t.Fatalf("stream.Close 下发 %d 次, want 1（幂等）", got)
	}
	if got := log.count("write"); got != 1 {
		t.Fatalf("stream 写入 %d 次, want 1（post-close Emit 不得触碰 stream）", got)
	}
}
