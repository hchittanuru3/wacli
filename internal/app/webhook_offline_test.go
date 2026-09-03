package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"github.com/openclaw/wacli/internal/wa"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// collectWebhookEvents drives the real sync event handler and records every
// event it enqueues for delivery.
type webhookEventRecorder struct {
	mu     sync.Mutex
	events []syncWebhookEvent
}

func (r *webhookEventRecorder) enqueue(evt syncWebhookEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
}

func (r *webhookEventRecorder) offlineFlags() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	flags := make([]bool, 0, len(r.events))
	for _, evt := range r.events {
		flags = append(flags, evt.Offline)
	}
	return flags
}

func offlineTestApp(t *testing.T, rec *webhookEventRecorder) (*App, *fakeWA) {
	t.Helper()
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	a.opts.Events = out.NewEventWriter(io.Discard, true)

	var messagesStored atomic.Int64
	var lastEvent atomic.Int64
	a.addSyncEventHandler(
		context.Background(),
		SyncOptions{},
		&messagesStored,
		&lastEvent,
		make(chan struct{}, 1),
		make(chan struct{}, 1),
		make(chan staleReconnectRequest, 1),
		func(string, string) {},
		rec.enqueue,
		nil,
		&syncPresence{},
		nil,
	)
	return a, f
}

func offlineTestMessage(id string) *events.Message {
	chat := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            id,
			Timestamp:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		},
		Message: &waProto.Message{Conversation: proto.String("hello")},
	}
}

// A reconnect after downtime replays everything the device missed as ordinary
// live message events. Without a marker a consumer answers a backlog as though
// it were current traffic, so the count the server announces bounds the window.
func TestOfflineBacklogMarksReplayedMessages(t *testing.T) {
	rec := &webhookEventRecorder{}
	_, f := offlineTestApp(t, rec)

	f.emit(&events.OfflineSyncPreview{Total: 3, Messages: 2})
	f.emit(offlineTestMessage("replayed-1"))
	f.emit(offlineTestMessage("replayed-2"))
	f.emit(&events.OfflineSyncCompleted{Count: 3})
	f.emit(offlineTestMessage("live-1"))

	got := rec.offlineFlags()
	want := []bool{true, true, false}
	if len(got) != len(want) {
		t.Fatalf("enqueued %d webhook events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offline flags = %v, want %v", got, want)
		}
	}
}

// The window closes on the announced count alone, so a replay cut short by a
// dropped connection (no OfflineSyncCompleted) cannot mark live traffic forever.
func TestOfflineBacklogWindowIsBoundedByTheAnnouncedCount(t *testing.T) {
	rec := &webhookEventRecorder{}
	_, f := offlineTestApp(t, rec)

	f.emit(&events.OfflineSyncPreview{Total: 1, Messages: 1})
	f.emit(offlineTestMessage("replayed-1"))
	// No OfflineSyncCompleted: the socket dropped mid-replay.
	f.emit(offlineTestMessage("live-1"))

	got := rec.offlineFlags()
	want := []bool{true, false}
	if len(got) != len(want) {
		t.Fatalf("enqueued %d webhook events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offline flags = %v, want %v", got, want)
		}
	}
}

// OfflineSyncCompleted closes a window the server over-counted, so the very next
// live message is not mislabelled.
func TestOfflineSyncCompletedClosesTheWindowEarly(t *testing.T) {
	rec := &webhookEventRecorder{}
	_, f := offlineTestApp(t, rec)

	f.emit(&events.OfflineSyncPreview{Total: 5, Messages: 5})
	f.emit(offlineTestMessage("replayed-1"))
	f.emit(&events.OfflineSyncCompleted{Count: 5})
	f.emit(offlineTestMessage("live-1"))

	got := rec.offlineFlags()
	want := []bool{true, false}
	if len(got) != len(want) {
		t.Fatalf("enqueued %d webhook events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offline flags = %v, want %v", got, want)
		}
	}
}

// Offline is omitempty: a live message keeps the exact payload shape consumers
// written before this flag already parse.
func TestOfflinePayloadFieldIsOmittedWhenLive(t *testing.T) {
	a := newTestApp(t)
	ctx := context.Background()
	pm := offlineTestMessage("m-1")

	live, err := json.Marshal(a.newSyncWebhookPayload(ctx, wa.ParseLiveMessage(pm), false))
	if err != nil {
		t.Fatalf("marshal live payload: %v", err)
	}
	if strings.Contains(string(live), "Offline") {
		t.Fatalf("live payload gained an Offline key: %s", live)
	}

	replayed, err := json.Marshal(a.newSyncWebhookPayload(ctx, wa.ParseLiveMessage(pm), true))
	if err != nil {
		t.Fatalf("marshal replayed payload: %v", err)
	}
	if !strings.Contains(string(replayed), `"Offline":true`) {
		t.Fatalf("replayed payload missing Offline: %s", replayed)
	}
}

// The lifecycle events are the half a consumer that does not use --webhook can
// act on: they name the replay, and how much of it is still coming.
func TestOfflineSyncEmitsLifecycleEvents(t *testing.T) {
	var eventsOut bytes.Buffer
	rec := &webhookEventRecorder{}
	a, f := offlineTestApp(t, rec)
	a.opts.Events = out.NewEventWriter(&eventsOut, true)

	f.emit(&events.OfflineSyncPreview{Total: 7, Messages: 4, Receipts: 2, Notifications: 1})
	f.emit(&events.OfflineSyncCompleted{Count: 7})

	log := eventsOut.String()
	for _, want := range []string{
		`"event":"offline_sync_preview"`,
		`"messages":4`,
		`"receipts":2`,
		`"total":7`,
		`"event":"offline_sync_completed"`,
		`"count":7`,
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("event log missing %s: %s", want, log)
		}
	}
}
