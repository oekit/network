package nats

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// unsubscribeOnCleanup schedules sub.Unsubscribe to run at test cleanup.
// The error is intentionally swallowed: by the time cleanup runs, the
// connection is being torn down and any Unsubscribe error is meaningless
// (the test has already passed or failed for a different reason).
func unsubscribeOnCleanup(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

// stopOnCleanup schedules cc.Stop to run at test cleanup. Stop is a
// best-effort shutdown call; no error to swallow.
func stopOnCleanup(t *testing.T, cc jetstream.ConsumeContext) {
	t.Helper()
	t.Cleanup(func() { cc.Stop() })
}
