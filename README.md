# ekit-network

> 网络客户端层——HTTP 客户端（服务发现/熔断/重试/指标）、WebSocket、NATS、消息队列抽象。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit)（`git.enjoye.top/enjoydream/ekit` v0.34.0）
按框架耦合分层拆分出的独立 Go module，属于 **yi-nology** ekit 子项目矩阵。

## 安装

```bash
go get github.com/yi-nology/ekit-network@v0.1.0
```

## 包清单

- `network/httpx/` — Hertz Client 高层封装：NNS 服务发现、熔断（gobreaker）、指数退避重试、Prometheus 弹性指标、OTel 透传、实例权重贯通
- `network/loadbalance/` — 加权负载均衡策略
- `network/wsutil/` — WebSocket 广播器（背压安全）
- `network/nats/`、`network/mq/` — NATS 客户端与 MQ 抽象
- `network/conn/`、`network/streamx/` — 连接管理与流工具
- `network/octopus/` — Octopus 推送客户端

## 依赖关系

子项目矩阵与依赖方向（自底向上）：

```
ekit-core                      纯工具层（零内部依赖）
  └─ ekit-observability        日志/追踪/指标门面
       └─ ekit-registry        注册发现 + 配置
            ├─ ekit-data       存储层
            ├─ ekit-network    网络客户端
            │    └─ ekit-nns   服务治理客户端
            │         └─ ekit-bootstrap  应用引导
            ├─ ekit-hertz      Hertz 生态
            ├─ ekit-kitex      Kitex 生态
            └─ ekit-workflow   流程编排
```

依赖：[ekit-core](https://github.com/yi-nology/ekit-core)、[ekit-observability](https://github.com/yi-nology/ekit-observability)、[ekit-registry](https://github.com/yi-nology/ekit-registry)。

## 与 ekit 单体仓库的关系

- 代码与 ekit v0.34.0 等价，仅 module 路径从 `git.enjoye.top/enjoydream/ekit/...` 改为 `github.com/yi-nology/ekit-network/...`
- 后续演进以各子仓库独立发版（SemVer）；ekit 单体仓库保留完整历史与架构文档（docs/PLATFORM_CONTRACT.md）
- 开发规范（命名/错误处理/并发安全/日志门面）沿用 ekit AGENTS.md 约定

## License

见 [LICENSE](LICENSE)
