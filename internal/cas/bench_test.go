package cas

import (
	"fmt"
	"testing"
)

func BenchmarkCASPutDeduplicated(b *testing.B) {
	tmpDir := b.TempDir()
	store, err := NewStore(tmpDir, 512)
	if err != nil {
		b.Fatalf("NewStore failed: %v", err)
	}
	payload := []byte(`{"id":"chatcmpl_benchmark","model":"claude-3-7-sonnet","choices":[{"index":0,"message":{"role":"assistant","content":"deterministic benchmark response payload"}}]}`)
	_, _ = store.Put(payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Put(payload)
	}
}

func BenchmarkCASGetHotLRU(b *testing.B) {
	tmpDir := b.TempDir()
	store, err := NewStore(tmpDir, 512)
	if err != nil {
		b.Fatalf("NewStore failed: %v", err)
	}
	hashes := make([]string, 32)
	for i := 0; i < 32; i++ {
		h, _ := store.Put([]byte(fmt.Sprintf(`{"slot":%d,"payload":"hot_lru_cache_benchmark_entry"}`, i)))
		hashes[i] = h
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(hashes[i&31])
	}
}
