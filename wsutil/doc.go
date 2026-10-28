// Package wsutil 提供通用 WebSocket 基础设施。
//
// 两个原语，都**不依赖任何 websocket 库**：
//
//   - Broadcaster：基于房间的广播器，支持多房间、自动清理空房间、容错广播。
//     通过 Conn 接口解耦。
//   - Outbox：单条连接的**有界出站管道**——保证「只有一个写者」，并把「慢
//     消费者」从发送方的主路径上摘掉。
//
// 用法（广播）：
//
//	b := wsutil.NewBroadcaster()
//	defer b.Close()
//
//	// 客户端加入房间
//	b.Register("room:123", conn)
//
//	// 广播事件
//	b.BroadcastEvent("room:123", wsutil.Event{Type: "progress", Data: data})
//
//	// 查询房间状态
//	count := b.GetRoomCount("room:123")
//	rooms := b.GetRooms()
//
// 用法（单连接的出站管道）：
//
//	// websocket 实现不许并发写，所以每条连接得有一个独家写者。
//	ob := wsutil.NewOutbox(func(msg []byte) error {
//		return conn.WriteMessage(websocket.TextMessage, msg)
//	}, wsutil.OutboxOptions{QueueSize: 64})
//	defer ob.Close()
//
//	ob.Send(frame) // 永不阻塞；队列满了返回 false，处置由调用方决定
package wsutil
