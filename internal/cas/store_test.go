package cas

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestStorePutGetAndDeduplication(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir, 4)
	if err != nil {
		t.Fatalf("failed to create CAS store: %v", err)
	}

	payload := []byte(`{"id":"evt_99281","object":"payment_intent","amount":4999,"currency":"usd","status":"succeeded"}`)
	hash1, err := store.Put(payload)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if !store.Has(hash1) {
		t.Fatalf("expected store.Has(%s) to be true", hash1)
	}

	// Second put of identical payload should trigger deduplication
	hash2, err := store.Put(payload)
	if err != nil {
		t.Fatalf("second Put failed: %v", err)
	}
	if hash1 != hash2 {
		t.Fatalf("expected identical hashes, got %s vs %s", hash1, hash2)
	}

	stats := store.Stats()
	if stats.TotalPuts != 2 {
		t.Errorf("expected TotalPuts=2, got %d", stats.TotalPuts)
	}
	if stats.DeduplicatedHits != 1 {
		t.Errorf("expected DeduplicatedHits=1, got %d", stats.DeduplicatedHits)
	}

	retrieved, err := store.Get(hash1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(payload, retrieved) {
		t.Fatalf("retrieved payload mismatch: got %q", string(retrieved))
	}
}

func TestStoreLRUEvictionAndDiskFallback(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir, 2) // tiny cache size of 2
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	hashes := make([]string, 5)
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf(`{"index":%d,"message":"payload_block_%d"}`, i, i))
		h, err := store.Put(data)
		if err != nil {
			t.Fatalf("Put(%d) failed: %v", i, err)
		}
		hashes[i] = h
	}

	// Hashes 0, 1, 2 were evicted from the 2-entry LRU cache and must load cleanly from disk
	for i := 0; i < 5; i++ {
		got, err := store.Get(hashes[i])
		if err != nil {
			t.Fatalf("Get(%d) after LRU eviction failed: %v", i, err)
		}
		expected := fmt.Sprintf(`{"index":%d,"message":"payload_block_%d"}`, i, i)
		if string(got) != expected {
			t.Fatalf("expected %s, got %s", expected, string(got))
		}
	}

	if store.Stats().CacheMisses == 0 {
		t.Errorf("expected cache misses after LRU eviction, got 0")
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir, 32)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				// Half shared payloads, half unique payloads
				payload := []byte(fmt.Sprintf(`{"shared_bucket":%d,"seq":%d}`, j%5, id%2))
				h, err := store.Put(payload)
				if err != nil {
					t.Errorf("concurrent Put failed: %v", err)
					return
				}
				out, err := store.Get(h)
				if err != nil {
					t.Errorf("concurrent Get failed: %v", err)
					return
				}
				if !bytes.Equal(payload, out) {
					t.Errorf("concurrent payload mismatch")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestStoreErrors(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewStore(tmpDir, 8)

	if _, err := store.Get("not-a-valid-sha256"); !errors.Is(err, ErrInvalidHash) {
		t.Fatalf("expected ErrInvalidHash, got %v", err)
	}

	missingHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := store.Get(missingHash); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("expected ErrBlobNotFound, got %v", err)
	}
}
