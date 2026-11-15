package telemetry

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type blockedWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w blockedWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(p), nil
}

func TestDiagnosticQueueDropsWhileOutputBlocked(t *testing.T) {
	w := blockedWriter{make(chan struct{}, 1), make(chan struct{})}
	e := NewEvents(slog.New(slog.NewJSONHandler(w, nil)), 1, "proxy")
	defer e.Close()
	defer close(w.release)
	id := e.RequestID(1)
	e.Emit(time.Now(), Event{RequestID: id})
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer never started")
	}
	for range queueSize + 10 {
		e.Emit(time.Now(), Event{RequestID: id})
	}
	if got := e.Dropped(); got != 10 {
		t.Fatalf("dropped %d, want 10", got)
	}
}

func TestDiagnosticSamplingAndSummary(t *testing.T) {
	if NewEvents(slog.Default(), 0, "proxy") != nil {
		t.Fatal("disabled diagnostics constructed a worker")
	}
	var b bytes.Buffer
	e := NewEvents(slog.New(slog.NewJSONHandler(&b, nil)), 2, "proxy")
	id := e.RequestID(2)
	if e.RequestID(1) != "" || !ValidRequestID(id) || ValidRequestID(id+" sensitive") || ValidRequestID(strings.Repeat("A", 26)+"-+1") {
		t.Fatal("unexpected sample or accepted correlation ID")
	}
	e.Emit(time.Unix(1, 0), Event{Kind: "test", RequestID: id})
	e.Close()
	decoder := json.NewDecoder(&b)
	for _, msg := range []string{"sbr diagnostic event", "sbr diagnostic summary"} {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil || record["msg"] != msg || record["diagnostic_schema"] != float64(SchemaVersion) {
			t.Fatalf("record = %v, error %v, want %s", record, err, msg)
		}
		if msg == "sbr diagnostic summary" && (record["written"] != float64(1) || record["dropped"] != float64(0)) {
			t.Fatalf("unexpected summary %v", record)
		}
	}
}
