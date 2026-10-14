package octopus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSendMarkdown(t *testing.T) {
	var gotPath, gotUser, gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotUser = r.Header.Get("X-User-Id")
		gotAuth = r.Header.Get("Authorization")
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, 42, "argus-bot", "tok-1", time.Second, nil)
	code, err := c.SendMarkdown(context.Background(), "**hello**")
	if err != nil || code != http.StatusOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if gotPath != "/api/v2/send/quick" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotUser != "argus-bot" {
		t.Fatalf("X-User-Id 必带（octopus 建账号语义）: %q", gotUser)
	}
	if gotAuth != "Bearer tok-1" {
		t.Fatalf("可选 Bearer: %q", gotAuth)
	}
	if !strings.Contains(gotBody, `"channelId":42`) || !strings.Contains(gotBody, `"msgType":"markdown"`) {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestSendRequiresUser(t *testing.T) {
	c := New("http://octopus.test", 1, "", "", time.Second, nil)
	if _, err := c.SendMarkdown(context.Background(), "x"); err == nil {
		t.Fatal("空机器账号应显式报错（匿名推送不可归因）")
	}
}

func TestSendStatusPassthrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, 1, "bot", "", time.Second, nil)
	code, err := c.SendMarkdown(context.Background(), "x")
	if err != nil || code != http.StatusForbidden {
		t.Fatalf("非 2xx 应透出状态码由调用方记账: code=%d err=%v", code, err)
	}
}
