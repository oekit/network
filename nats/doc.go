// Package nats 提供 NATS 消息队列客户端封装。
//
// 基于 nats-io/nats.go，为 ekit 微服务提供统一的 NATS 接入。
//
// # 核心特性
//
//   - 自动重连 + 重连回调
//   - Publish / Subscribe / QueueSubscribe
//   - Request / Reply（同步 RPC 模式）
//   - JSON 序列化/反序列化
//   - 优雅关闭（drain）
//
// # 使用方式
//
//	// 连接
//	client, err := nats.NewClient("nats://localhost:4222",
//	    nats.WithName("my-service"),
//	    nats.WithReconnectWait(2*time.Second),
//	)
//	defer client.Close()
//
//	// 发布
//	err := client.Publish(ctx, "order.created", []byte(`{"id":1}`))
//
//	// 订阅
//	sub, err := client.Subscribe(ctx, "order.created", func(msg *nats.Msg) {
//	    log.Printf("received: %s", string(msg.Data))
//	})
//	defer sub.Unsubscribe()
//
//	// Request/Reply
//	resp, err := client.Request(ctx, "user.get", []byte(`{"id":1}`), 5*time.Second)
package nats
