package cas

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	// ErrBlobNotFound is returned when a requested SHA-256 digest does not exist in the store.
	ErrBlobNotFound = errors.New("cas: blob not found")
	// ErrInvalidHash is returned when a digest string is not a valid 64-character hex SHA-256 hash.
	ErrInvalidHash = errors.New("cas: invalid sha256 digest")
)

// Stats holds runtime telemetry for the Content-Addressable Storage engine.
type Stats struct {
	TotalPuts        uint64  `json:"total_puts"`
	DeduplicatedHits uint64  `json:"deduplicated_hits"`
	CacheHits        uint64  `json:"cache_hits"`
	CacheMisses      uint64  `json:"cache_misses"`
	LogicalBytes     uint64  `json:"logical_bytes"`
	PhysicalBytes    uint64  `json:"physical_bytes"`
	DedupRatio       float64 `json:"dedup_ratio"`
}

type cacheEntry struct {
	hash string
	data []byte
}

// Store implements a thread-safe, content-addressable blob storage engine backed by
// two-level directory sharding (e.g., objects/ab/cd/abcdef...), gzip compression,
// atomic write-and-rename durability, and an in-memory LRU hot cache.
type Store struct {
	baseDir     string
	maxCacheLen int

	mu        sync.RWMutex
	lruList   *list.List
	lruMap    map[string]*list.Element
	knownSize map[string]int64

	totalPuts        atomic.Uint64
	deduplicatedHits atomic.Uint64
	cacheHits        atomic.Uint64
	cacheMisses      atomic.Uint64
	logicalBytes     atomic.Uint64
	physicalBytes    atomic.Uint64
}

// NewStore initializes a Content-Addressable Store rooted at baseDir.
// maxCacheEntries controls the maximum number of uncompressed blobs kept in the hot LRU cache.
func NewStore(baseDir string, maxCacheEntries int) (*Store, error) {
	if maxCacheEntries <= 0 {
		maxCacheEntries = 512
	}
	objectsDir := filepath.Join(baseDir, "objects")
	if err := os.MkdirAll(objectsDir, 0o755); err != nil {
		return nil, fmt.Errorf("cas: failed to create objects directory: %w", err)
	}

	s := &Store{
		baseDir:     baseDir,
		maxCacheLen: maxCacheEntries,
		lruList:     list.New(),
		lruMap:      make(map[string]*list.Element, maxCacheEntries),
		knownSize:   make(map[string]int64),
	}

	if err := s.scanExistingBlobs(); err != nil {
		return nil, err
	}
	return s, nil
}

// ComputeHash returns the lowercase hex-encoded SHA-256 digest of data.
func ComputeHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Put stores a payload in the content-addressable store and returns its SHA-256 hex digest.
// Identical payloads are automatically deduplicated without redundant disk writes.
func (s *Store) Put(data []byte) (string, error) {
	hash := ComputeHash(data)
	s.totalPuts.Add(1)
	s.logicalBytes.Add(uint64(len(data)))

	if len(data) == 0 {
		s.deduplicatedHits.Add(1)
		return hash, nil
	}

	s.mu.RLock()
	_, existsInIndex := s.knownSize[hash]
	s.mu.RUnlock()

	blobPath := s.blobPath(hash)
	if existsInIndex {
		s.deduplicatedHits.Add(1)
		s.promoteCache(hash, data)
		return hash, nil
	}

	if info, err := os.Stat(blobPath); err == nil && !info.IsDir() {
		s.mu.Lock()
		if _, ok := s.knownSize[hash]; !ok {
			s.knownSize[hash] = info.Size()
			s.physicalBytes.Add(uint64(info.Size()))
		}
		s.mu.Unlock()
		s.deduplicatedHits.Add(1)
		s.promoteCache(hash, data)
		return hash, nil
	}

	compressed, err := compressGzip(data)
	if err != nil {
		return "", fmt.Errorf("cas: gzip compression failed: %w", err)
	}

	shardDir := filepath.Dir(blobPath)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return "", fmt.Errorf("cas: failed to create shard dir: %w", err)
	}

	// Write to temporary file in the same directory and atomically rename to prevent torn reads.
	tmpFile, err := os.CreateTemp(shardDir, ".tmp-"+hash[:12]+"-*")
	if err != nil {
		return "", fmt.Errorf("cas: failed to create temp file: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.Write(compressed); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("cas: failed to write temp blob: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("cas: failed to close temp blob: %w", err)
	}

	if err := os.Rename(tmpName, blobPath); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("cas: atomic rename failed: %w", err)
	}

	s.mu.Lock()
	if _, alreadyRecorded := s.knownSize[hash]; !alreadyRecorded {
		s.knownSize[hash] = int64(len(compressed))
		s.physicalBytes.Add(uint64(len(compressed)))
	} else {
		s.deduplicatedHits.Add(1)
	}
	s.mu.Unlock()

	s.promoteCache(hash, data)
	return hash, nil
}

// Get retrieves and decompresses a payload by its SHA-256 hex digest.
func (s *Store) Get(hash string) ([]byte, error) {
	if !isValidHash(hash) {
		return nil, ErrInvalidHash
	}

	// Empty payload SHA-256 fast path
	if hash == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		return []byte{}, nil
	}

	if cached, ok := s.getFromCache(hash); ok {
		s.cacheHits.Add(1)
		out := make([]byte, len(cached))
		copy(out, cached)
		return out, nil
	}
	s.cacheMisses.Add(1)

	blobPath := s.blobPath(hash)
	raw, err := os.ReadFile(blobPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("cas: failed to read blob %s: %w", hash, err)
	}

	decompressed, err := decompressGzip(raw)
	if err != nil {
		return nil, fmt.Errorf("cas: corrupt blob %s: %w", hash, err)
	}

	// Verify content integrity on read
	if ComputeHash(decompressed) != hash {
		return nil, fmt.Errorf("cas: sha256 integrity mismatch for blob %s", hash)
	}

	s.promoteCache(hash, decompressed)
	out := make([]byte, len(decompressed))
	copy(out, decompressed)
	return out, nil
}

// Has checks whether a blob with the given SHA-256 digest exists in the store.
func (s *Store) Has(hash string) bool {
	if !isValidHash(hash) {
		return false
	}
	if hash == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		return true
	}
	s.mu.RLock()
	_, ok := s.knownSize[hash]
	s.mu.RUnlock()
	if ok {
		return true
	}
	_, err := os.Stat(s.blobPath(hash))
	return err == nil
}

// Stats returns a snapshot of storage efficiency, deduplication, and cache hit metrics.
func (s *Store) Stats() Stats {
	logical := s.logicalBytes.Load()
	physical := s.physicalBytes.Load()
	ratio := 1.0
	if physical > 0 && logical > 0 {
		ratio = float64(logical) / float64(physical)
	}
	return Stats{
		TotalPuts:        s.totalPuts.Load(),
		DeduplicatedHits: s.deduplicatedHits.Load(),
		CacheHits:        s.cacheHits.Load(),
		CacheMisses:      s.cacheMisses.Load(),
		LogicalBytes:     logical,
		PhysicalBytes:    physical,
		DedupRatio:       ratio,
	}
}

func (s *Store) blobPath(hash string) string {
	return filepath.Join(s.baseDir, "objects", hash[0:2], hash[2:4], hash)
}

func (s *Store) getFromCache(hash string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.lruMap[hash]; ok {
		s.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).data, true
	}
	return nil, false
}

func (s *Store) promoteCache(hash string, data []byte) {
	copied := make([]byte, len(data))
	copy(copied, data)

	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.lruMap[hash]; ok {
		s.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).data = copied
		return
	}

	entry := &cacheEntry{hash: hash, data: copied}
	elem := s.lruList.PushFront(entry)
	s.lruMap[hash] = elem

	if s.lruList.Len() > s.maxCacheLen {
		oldest := s.lruList.Back()
		if oldest != nil {
			s.lruList.Remove(oldest)
			evicted := oldest.Value.(*cacheEntry)
			delete(s.lruMap, evicted.hash)
		}
	}
}

func (s *Store) scanExistingBlobs() error {
	objectsRoot := filepath.Join(s.baseDir, "objects")
	return filepath.WalkDir(objectsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !isValidHash(name) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		s.knownSize[name] = info.Size()
		s.physicalBytes.Add(uint64(info.Size()))
		return nil
	})
}

func isValidHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(data); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressGzip(data []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
