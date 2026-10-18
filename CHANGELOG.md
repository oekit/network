# Changelog — oekit/network

## [v0.5.4] — 2026-10-18

- fix(deps): tracing v0.2.2 地板对齐（零代码改动）

## [v0.5.3] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.5.2] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.1.1] — 2026-10-17

- fix(deps): 内部依赖地板 logx v0.2.0 → v0.5.0；go 维持 1.26.0——k8s.io/client-go 生态声明 go1.26，暂不做生态级回退

## [v0.5.1] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——concurrency v0.5.2、logx v0.5.0、tracing v0.2.1；go 维持 1.26.0（tracing/OTel 生态传导）

## [v0.5.0] — 2026-10-17

全新起点：历史与旧版本号已全部清空，自本版重新起算。

- **模块布局收平**：包从 `network/<name>` 双层目录提为仓库根一级
  （`conn/`、`mq/`、`nats/`、`streamx/`、`wsutil/`），import 路径为
  `github.com/oekit/network/<name>`，不再多一层 `network/network/`。
- **模块边界**：通用轻量网络客户端集——连接管理、消息队列抽象、NATS、
  流工具、WebSocket 广播。业务推送客户端不属于本模块，不在范围内。
- 包能力面：
  | 包 | 做什么 |
  |---|---|
  | `conn` | 连接管理 |
  | `mq` | 消息队列抽象 |
  | `nats` | NATS 客户端（嵌入式测试友好）+ JetStream |
  | `streamx` | 流工具（事件、Hub、SSE sink） |
  | `wsutil` | WebSocket 广播器（房间注册、背压安全） |
