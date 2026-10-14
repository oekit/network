package nats

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
)

// newJetStreamTestClient starts an embedded NATS server with JetStream
// enabled on a random port. The server is torn down via t.Cleanup.
func newJetStreamTestClient(t *testing.T) *Client {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir() // required when JetStream is enabled
	srv := natsserver.RunServer(&opts)
	t.Cleanup(func() { srv.Shutdown() })

	c, err := NewClient(srv.ClientURL(),
		WithName("ekit-nats-js-test"),
		WithMaxReconnects(0),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestJetStream_NewJetStream_NotNil covers the NewJetStream constructor.
// A bare call should return a non-nil client; we don't open a stream here
// to keep this test cheap.
func TestJetStream_NewJetStream_NotNil(t *testing.T) {
	c := newJetStreamTestClient(t)

	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	if js == nil {
		t.Fatal("NewJetStream returned nil without error")
	}
}

// TestJetStream_CreateStream_ThenIdempotent covers the CreateStream
// happy path AND the "stream already exists" idempotent return. The
// second call must not return an error — production callers rely on
// this to safely call CreateStream at boot.
func TestJetStream_CreateStream_ThenIdempotent(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "TEST_JS_STREAM_1"
	if createErr := js.CreateStream(ctx, streamName, "test.js.>"); createErr != nil {
		t.Fatalf("CreateStream (first): %v", createErr)
	}
	// Second call must be a no-op (idempotent).
	if createErr := js.CreateStream(ctx, streamName, "test.js.>"); createErr != nil {
		t.Errorf("CreateStream (second, idempotent) returned error: %v", createErr)
	}

	// Stream lookup must succeed and reflect the configured subjects.
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("Stream lookup: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info: %v", err)
	}
	if info.Config.Name != streamName {
		t.Errorf("expected stream name %q, got %q", streamName, info.Config.Name)
	}
}

// TestJetStream_PublishAndConsume_EndToEnd covers the publish → consume
// round trip through the push-mode Consume API. Exercises CreateStream,
// CreateOrUpdateConsumer, Consume callback, and ack-on-success path.
func TestJetStream_PublishAndConsume_EndToEnd(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "TEST_JS_STREAM_2"
	if createErr := js.CreateStream(ctx, streamName, "test.js.publish.>"); createErr != nil {
		t.Fatalf("CreateStream: %v", createErr)
	}

	ack, err := js.Publish(ctx, "test.js.publish.one", []byte("hello"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ack == nil || ack.Sequence == 0 {
		t.Errorf("expected non-zero PubAck sequence, got %+v", ack)
	}

	// Consume in a goroutine; stop the consumer when the message arrives.
	got := make(chan string, 1)
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()

	cc, err := js.Consume(consumeCtx, streamName, "consumer-1", func(msg jetstream.Msg) {
		select {
		case got <- string(msg.Data()):
		default:
		}
		_ = msg.Ack()
		stop()
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	stopOnCleanup(t, cc)

	select {
	case s := <-got:
		if s != "hello" {
			t.Errorf("expected %q, got %q", "hello", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for JetStream Consume callback")
	}
}

// TestJetStream_Fetch_EndToEnd uses the simpler Fetch API (pull mode,
// batch read) to verify that messages we publish land in the stream and
// can be retrieved. Covers Publish / Fetch / CreateOrUpdateConsumer.
func TestJetStream_Fetch_EndToEnd(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "TEST_JS_STREAM_3"
	if createErr := js.CreateStream(ctx, streamName, "test.js.fetch.>"); createErr != nil {
		t.Fatalf("CreateStream: %v", createErr)
	}

	for i := 1; i <= 3; i++ {
		if _, pubErr := js.Publish(ctx, "test.js.fetch.one", []byte("m")); pubErr != nil {
			t.Fatalf("Publish %d: %v", i, pubErr)
		}
	}

	batch, err := js.Fetch(ctx, streamName, "consumer-1", 10,
		jetstream.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	count := 0
	for msg := range batch.Messages() {
		count++
		_ = msg
	}
	if err := batch.Error(); err != nil {
		t.Errorf("batch.Error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 messages from Fetch, got %d", count)
	}
}

// TestJetStream_DeleteStream covers the DeleteStream happy path. After
// delete, Stream lookup must fail.
func TestJetStream_DeleteStream(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "TEST_JS_STREAM_4"
	if createErr := js.CreateStream(ctx, streamName, "test.js.del.>"); createErr != nil {
		t.Fatalf("CreateStream: %v", createErr)
	}
	if err := js.DeleteStream(ctx, streamName); err != nil {
		t.Errorf("DeleteStream: %v", err)
	}
	if _, err := js.Stream(ctx, streamName); err == nil {
		t.Error("expected Stream lookup to fail after DeleteStream, got nil")
	}
}

// TestJetStream_PublishJSON_MarshalError covers the unmarshalable value
// short-circuit in PublishJSON. Must return an error before touching
// the network.
func TestJetStream_PublishJSON_MarshalError(t *testing.T) {
	c := newJetStreamTestClient(t)
	js, err := c.NewJetStream()
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	_, err = js.PublishJSON(context.Background(), "test.js.bad", make(chan int))
	if err == nil {
		t.Fatal("expected marshal error for chan value, got nil")
	}
}
