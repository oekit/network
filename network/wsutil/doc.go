// Package wsutil 提供通用 WebSocket 基础设施。
//
// Broadcaster 是基于房间的广播器，支持多房间、自动清理空房间、容错广播。
// 通过 Conn 接口解耦，不依赖具体的 websocket 库实现。
//
// 用法:
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
package wsutil
