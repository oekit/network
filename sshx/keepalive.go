package sshx

// keepalive.go 保持空闲连接不被掐断。
//
// 为什么需要它。一条池化的连接在调用之间是闲着的——一秒，也许两秒。但池
// 往往留它两分钟，而这条路径上任何东西（NAT 表、有空闲超时的防火墙）都可能
// 远在这之前就把空闲连接关掉。连接于是静默地死了，下一次调用拿到的是一条
// 死掉的 socket，而不是一条好用的。
//
// 修法是隔一会儿在连接上说句话。这个请求就是 OpenSSH 自己的 keepalive，
// 每个 SSH 服务端都认——不认就失败，而答不答无所谓：流量本身就是保温。
//
// 它**不**等回包。等回包意味着等一个回应，而一个既不应也不关的服务端会
// 让这个请求卡在这条连接的整个余生里。
//
// 它能看见什么、看不见什么。传输层的错误说明连接没了，此时连接被标记为
// 坏掉，下次调用会开一条新的而不是捡到一具尸体。但**被静默掐断**的连接
//（包被丢弃、没有 RST）在这里不会报错——写操作进了本地缓冲，对端根本没
// 听见。认出这一种是命令那一步的事，而命令那一步已经处理了：开不出通道
// 就说明命令没到机器上，可以换一条连接重跑。

import (
	"errors"
	"time"

	"github.com/oekit/concurrency/async"
)

// DefaultKeepAliveInterval 是戳连接的间隔。
const DefaultKeepAliveInterval = 30 * time.Second

// keepAliveRequest 是 OpenSSH 的全局 keepalive 请求名。
const keepAliveRequest = "keepalive@openssh.com"

// keepAliveMissesTolerated 是连续失败多少次之后才判连接已死。
//
// 一次是「不误判」的上限：一次写失败不能证明连接没了，而换掉一条好连接
// 的代价是一次握手。
const keepAliveMissesTolerated = 1

// ErrBusy 说连接当时正忙着，所以这一下没戳。
//
// 它不是失败：忙着说明连接不闲，不闲的连接不需要保温。
var ErrBusy = errors.New("the connection is busy")

// StartKeepAlive 定期戳连接，直到 done 被关掉。
//
// poke 发出这个请求并报告发生了什么：ErrBusy 表示连接正忙着（跳过这一
// 下），别的错误表示连接没了。onDead 在那时被调用——它是调用方自己的信号：
// 把连接标记为坏掉，下次调用开一条新的。
//
// interval 是参数，这样测试不必等半分钟才看到一次保活。零值即默认。
//
// 它在返回之前不等待：保活是后台的，done 关掉之后 goroutine 自己退出。
func StartKeepAlive(poke func() error, done <-chan struct{}, onDead func(), interval time.Duration) {
	if poke == nil || done == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultKeepAliveInterval
	}
	async.GoSafeLabeled("sshx-keepalive", func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		misses := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				err := poke()
				if err == nil {
					misses = 0
					continue
				}
				if errors.Is(err, ErrBusy) {
					continue
				}
				misses++
				if misses <= keepAliveMissesTolerated {
					continue
				}
				if onDead != nil {
					onDead()
				}
				return
			}
		}
	})
}
