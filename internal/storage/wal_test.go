package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALStorePersistenceRecoveryAndCRCValidation(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewWALStore(tmpDir, 100, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWALStore failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		rec := TraceRecord{
			ID:              fmt.Sprintf("tr_%03d", i),
			Timestamp:       time.Now().UTC(),
			Mode:            ModeRecord,
			Method:          "POST",
			Path:            "/v1/payments/authorize",
			Fingerprint:     fmt.Sprintf("fp_payment_%d", i%3),
			StatusCode:      200,
			LatencyMs:       float64(i * 12),
			UpstreamMs:      float64(i*12) - 0.2,
			ProxyOverheadMs: 0.2,
			RequestBodyHash: "abc123",
			ResponseHash:    fmt.Sprintf("resp_hash_%d", i),
		}
		if err := store.Enqueue(rec); err != nil {
			t.Fatalf("Enqueue(%d) failed: %v", i, err)
		}
	}

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Inject a corrupted line into traces.wal to verify CRC32 corruption resilience on recovery
	walFile := filepath.Join(tmpDir, "traces.wal")
	f, err := os.OpenFile(walFile, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal failed: %v", err)
	}
	_, _ = f.WriteString(`{"crc32":12345678,"record":{"id":"tr_corrupt","method":"GET","path":"/tampered"}}` + "\n")
	_ = f.Close()

	// Re-open store and verify recovery of the 10 valid records and rejection of the 1 corrupted record
	recovered, err := NewWALStore(tmpDir, 100, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer recovered.Close()

	metrics := recovered.Metrics()
	if metrics.TotalRequests != 10 {
		t.Fatalf("expected 10 valid recovered records, got %d", metrics.TotalRequests)
	}
	if metrics.WALCorruptSkipped != 1 {
		t.Fatalf("expected 1 corrupt WAL record skipped, got %d", metrics.WALCorruptSkipped)
	}

	golden, err := recovered.FindGoldenByFingerprint("fp_payment_1")
	if err != nil {
		t.Fatalf("FindGoldenByFingerprint failed: %v", err)
	}
	// Latest record with i%3 == 1 is i=10 ("tr_010")
	if golden.ID != "tr_010" {
		t.Fatalf("expected latest golden record tr_010, got %s", golden.ID)
	}
}

func TestWALStoreQueryAndMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewWALStore(tmpDir, 50, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWALStore failed: %v", err)
	}
	defer store.Close()

	_ = store.Enqueue(TraceRecord{
		ID: "tr_1", Mode: ModeRecord, Method: "GET", Path: "/v1/models",
		StatusCode: 200, LatencyMs: 15.5, ProxyOverheadMs: 0.15,
	})
	_ = store.Enqueue(TraceRecord{
		ID: "tr_2", Mode: ModeReplay, Method: "POST", Path: "/v1/chat/completions",
		StatusCode: 200, LatencyMs: 0.8, ProxyOverheadMs: 0.12, DiffBreaking: 1,
	})
	_ = store.Enqueue(TraceRecord{
		ID: "tr_3", Mode: ModeRecord, Method: "POST", Path: "/v1/chat/completions",
		StatusCode: 503, LatencyMs: 240.0, ProxyOverheadMs: 0.25, ChaosInjected: "http_status:503",
	})

	chaosOnly := store.Query(QueryFilter{OnlyChaos: true})
	if len(chaosOnly) != 1 || chaosOnly[0].ID != "tr_3" {
		t.Fatalf("expected 1 chaos trace tr_3, got %+v", chaosOnly)
	}

	driftOnly := store.Query(QueryFilter{OnlyDrift: true})
	if len(driftOnly) != 1 || driftOnly[0].ID != "tr_2" {
		t.Fatalf("expected 1 drift trace tr_2, got %+v", driftOnly)
	}

	m := store.Metrics()
	if m.TotalRequests != 3 || m.ReplayHits != 1 || m.ChaosFaults != 1 || m.DriftDetected != 1 {
		t.Fatalf("unexpected metrics counters: %+v", m)
	}
	if m.P95LatencyMs != 240.0 {
		t.Fatalf("expected P95=240.0, got %f", m.P95LatencyMs)
	}
}
