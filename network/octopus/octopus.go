// Package octopus octopus 消息平台推送客户端（公司 IM 基建件，收编自
// argus internal/notify，v0.32.0）：POST /api/v2/send/quick，X-User-Id
// 机器账号头（octopus AuthMiddleware 按 header 自动建账号）+ 可选 Bearer。
// 单一固定上游、共享连接池、缺省 10s 超时。
//
// 非目标：不做重试与收件人路由（终态推送失败由调用方按 status/err 自行
// 记账降级——审查主链路不因 IM 推送阻断）；不做富文本卡片模板（markdown
// 内容由调用方渲染，业务文案不入本包）。
package octopus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client octopus 推送客户端（零值不可用；经 New 构建）。
type Client struct {
	BaseURL   string // 如 http://octopus.svc（不带尾斜杠）
	ChannelID int64  // octopus 渠道 ID（企微/飞书/钉钉机器人渠道）
	User      string // 机器账号名（X-User-Id 头，必填——octopus 建账号语义）
	Token     string // 可选 Bearer（octopus 前置网关需要时用）

	hc *http.Client
}

// New 构建客户端（timeout <=0 取 10s；hc 为 nil 时内置）。
func New(baseURL string, channelID int64, user, token string, timeout time.Duration, hc *http.Client) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if hc == nil {
		hc = &http.Client{Timeout: timeout}
	}
	return &Client{BaseURL: baseURL, ChannelID: channelID, User: user, Token: token, hc: hc}
}

// SendMarkdown 推送 markdown 消息；返回 HTTP 状态码（网络错误时为 0）。
// 非 2xx 由调用方按业务记账（本包不重试不吞错）。
func (c *Client) SendMarkdown(ctx context.Context, content string) (int, error) {
	return c.send(ctx, "markdown", content)
}

// send POST /api/v2/send/quick。
func (c *Client) send(ctx context.Context, msgType, content string) (int, error) {
	payload, err := json.Marshal(map[string]any{
		"channelId": c.ChannelID,
		"content":   content,
		"msgType":   msgType,
	})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.BaseURL, "/")+"/api/v2/send/quick", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// X-User-Id 是 octopus AuthMiddleware 的建账号语义，必带；空账号显式报错
	// 而非匿名发送（匿名推送在审计面上不可归因）。
	if c.User == "" {
		return 0, fmt.Errorf("octopus: 缺少机器账号（User 必填）")
	}
	req.Header.Set("X-User-Id", c.User)
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, nil
}
