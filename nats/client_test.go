package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestOptions(t *testing.T) {
	o := options{}
	WithName("test-svc")(&o)
	if o.name != "test-svc" {
		t.Errorf("expected name 'test-svc', got %q", o.name)
	}

	WithReconnectWait(5 * time.Second)(&o)
	if o.reconnectWait != 5*time.Second {
		t.Errorf("expected 5s, got %v", o.reconnectWait)
	}

	WithMaxReconnects(10)(&o)
	if o.maxReconnects != 10 {
		t.Errorf("expected 10, got %d", o.maxReconnects)
	}
}

func TestOptions_Defaults(t *testing.T) {
	o := options{
		reconnectWait: 2 * time.Second,
		maxReconnects: -1,
	}
	if o.reconnectWait != 2*time.Second {
		t.Errorf("expected 2s default, got %v", o.reconnectWait)
	}
	if o.maxReconnects != -1 {
		t.Errorf("expected -1 default, got %d", o.maxReconnects)
	}
}

func TestOptions_Callbacks(t *testing.T) {
	var disconnectCalled, reconnectCalled, closeCalled bool

	o := options{}
	WithDisconnectHandler(func(c *nats.Conn, err error) {
		disconnectCalled = true
	})(&o)
	WithReconnectHandler(func(c *nats.Conn) {
		reconnectCalled = true
	})(&o)
	WithCloseHandler(func(c *nats.Conn) {
		closeCalled = true
	})(&o)

	if o.onDisconnect == nil {
		t.Error("expected onDisconnect set")
	}
	if o.onReconnect == nil {
		t.Error("expected onReconnect set")
	}
	if o.onClose == nil {
		t.Error("expected onClose set")
	}

	o.onDisconnect(nil, nil)
	o.onReconnect(nil)
	o.onClose(nil)

	if !disconnectCalled {
		t.Error("expected disconnect called")
	}
	if !reconnectCalled {
		t.Error("expected reconnect called")
	}
	if !closeCalled {
		t.Error("expected close called")
	}
}

func TestNewClient_BadURL(t *testing.T) {
	_, err := NewClient("nats://invalid:99999")
	if err == nil {
		t.Error("expected error for invalid URL")
	}
}
