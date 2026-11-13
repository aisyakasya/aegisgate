package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTraceNotFound is returned when no trace matches the requested ID or fingerprint.
var ErrTraceNotFound = errors.New("storage: trace record not found")

// ProxyMode represents the operational mode in which a trace was captured.
type ProxyMode string

const (
	ModeRecord      ProxyMode = "RECORD"
	ModeReplay      ProxyMode = "REPLAY"
	ModeShadowDiff  ProxyMode = "SHADOW_DIFF"
	ModePassthrough ProxyMode = "PASSTHROUGH"
)

// TraceRecord represents a single intercepted HTTP/API transaction persisted in the WAL.
type TraceRecord struct {
	ID              string              `json:"id"`
	Timestamp       time.Time           `json:"timestamp"`
	Mode            ProxyMode           `json:"mode"`
	Method          string              `json:"method"`
	Path            string              `json:"path"`
	Query           string              `json:"query,omitempty"`
	Fingerprint     string              `json:"fingerprint"`
	StatusCode      int                 `json:"status_code"`
	LatencyMs       float64             `json:"latency_ms"`
	UpstreamMs      float64             `json:"upstream_ms"`
	ProxyOverheadMs float64             `json:"proxy_overhead_ms"`
	RequestHeaders  map[string][]string `json:"request_headers,omitempty"`
	ResponseHeaders map[string][]string `json:"response_headers,omitempty"`
	RequestBodyHash string              `json:"request_body_hash"`
	RequestSize     int                 `json:"request_size"`
	ResponseHash    string              `json:"response_hash"`
	ResponseSize    int                 `json:"response_size"`
	ChaosInjected   string              `json:"chaos_injected,omitempty"`
	CircuitState    string              `json:"circuit_state,omitempty"`
	RateLimited     bool                `json:"rate_limited,omitempty"`
	DiffBreaking    int                 `json:"diff_breaking,omitempty"`
	DiffNonBreaking int                 `json:"diff_non_breaking,omitempty"`
	ClientIP        string              `json:"client_ip,omitempty"`
}

// walEnvelope wraps a TraceRecord with a CRC32 IEEE checksum for corruption detection during recovery.
type walEnvelope struct {
	CRC32  uint32      `json:"crc32"`
	Record TraceRecord `json:"record"`
}

// QueryFilter defines filtering criteria for querying traces.
type QueryFilter struct {
	Method     string
	PathPrefix string
	Mode       string
	StatusMin  int
	StatusMax  int
	OnlyChaos  bool
	OnlyDrift  bool
	Limit      int
	Offset     int
}

// LatencyMetrics holds summary latency percentiles and throughput counters.
type LatencyMetrics struct {
	TotalRequests     int     `json:"total_requests"`
	ReplayHits        int     `json:"replay_hits"`
	ChaosFaults       int     `json:"chaos_faults"`
	RateLimitedCount  int     `json:"rate_limited_count"`
	DriftDetected     int     `json:"drift_detected"`
	ErrorRatePct      float64 `json:"error_rate_pct"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	AvgProxyOverhead  float64 `json:"avg_proxy_overhead_ms"`
	WALFlushes        uint64  `json:"wal_flushes"`
	WALCorruptSkipped uint64  `json:"wal_corrupt_skipped"`
}

// WALStore provides an append-only Write-Ahead Log with CRC32 checksums,
// asynchronous ring-buffer batch flushing, and O(1) fingerprint lookup for deterministic replay.
type WALStore struct {
	walPath    string
	maxInMem   int
	batchSize  int
	flushEvery time.Duration

	mu              sync.RWMutex
	records         []TraceRecord
	byID            map[string]int
	byFingerprint   map[string]int // maps canonical request fingerprint -> latest recorded index
	walFile         *os.File
	walWriter       *bufio.Writer
	ingestCh        chan TraceRecord
	flushReqCh      chan chan error
	stopCh          chan struct{}
	wg              sync.WaitGroup
	closed          atomic.Bool
	walFlushes      atomic.Uint64
	corruptSkipped  atomic.Uint64
	droppedOverflow atomic.Uint64
}

// NewWALStore opens or creates a Write-Ahead Log at dataDir/traces.wal, recovers all valid
// historical records, rebuilds in-memory secondary indexes, and starts the async batch writer.
func NewWALStore(dataDir string, maxInMemory int, flushInterval time.Duration) (*WALStore, error) {
	if maxInMemory <= 0 {
		maxInMemory = 10000
	}
	if flushInterval <= 0 {
		flushInterval = 20 * time.Millisecond
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: failed to create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "traces.wal")
	s := &WALStore{
		walPath:       walPath,
		maxInMem:      maxInMemory,
		batchSize:     128,
		flushEvery:    flushInterval,
		records:       make([]TraceRecord, 0, 1024),
		byID:          make(map[string]int, 1024),
		byFingerprint: make(map[string]int, 512),
		ingestCh:      make(chan TraceRecord, 4096),
		flushReqCh:    make(chan chan error, 16),
		stopCh:        make(chan struct{}),
	}

	if err := s.recoverFromWAL(); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to open WAL file for append: %w", err)
	}
	s.walFile = f
	s.walWriter = bufio.NewWriterSize(f, 64*1024)

	s.wg.Add(1)
	go s.asyncFlushLoop()

	return s, nil
}

// Enqueue schedules a TraceRecord for asynchronous persistence and immediate in-memory indexing.
// Indexing happens synchronously so subsequent deterministic replays see the trace immediately,
// while disk I/O is batched asynchronously in the background.
func (s *WALStore) Enqueue(rec TraceRecord) error {
	if s.closed.Load() {
		return errors.New("storage: WAL store is closed")
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}

	s.indexRecord(rec)

	select {
	case s.ingestCh <- rec:
		return nil
	default:
		s.droppedOverflow.Add(1)
		return errors.New("storage: async WAL ring buffer full")
	}
}

// Flush blocks until all currently queued trace records have been written and flushed to the OS page cache.
func (s *WALStore) Flush() error {
	if s.closed.Load() {
		return nil
	}
	done := make(chan error, 1)
	s.flushReqCh <- done
	return <-done
}

// Close flushes all pending records and closes the underlying WAL file handle.
func (s *WALStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopCh)
	s.wg.Wait()
	if s.walWriter != nil {
		_ = s.walWriter.Flush()
	}
	if s.walFile != nil {
		return s.walFile.Close()
	}
	return nil
}

// GetByID retrieves a trace record by its unique ID.
func (s *WALStore) GetByID(id string) (TraceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.byID[id]
	if !ok || idx < 0 || idx >= len(s.records) {
		return TraceRecord{}, ErrTraceNotFound
	}
	return s.records[idx], nil
}

// FindGoldenByFingerprint performs an O(1) lookup for the most recent healthy recorded trace
// matching the canonical request fingerprint. Used by the Deterministic Replay and Shadow Diff engines.
func (s *WALStore) FindGoldenByFingerprint(fingerprint string) (TraceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.byFingerprint[fingerprint]
	if !ok || idx < 0 || idx >= len(s.records) {
		return TraceRecord{}, ErrTraceNotFound
	}
	return s.records[idx], nil
}

// Query returns traces in reverse chronological order (newest first) matching the provided filter.
func (s *WALStore) Query(f QueryFilter) []TraceRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	out := make([]TraceRecord, 0, limit)
	skipped := 0

	for i := len(s.records) - 1; i >= 0; i-- {
		rec := s.records[i]
		if f.Method != "" && !strings.EqualFold(rec.Method, f.Method) {
			continue
		}
		if f.PathPrefix != "" && !strings.HasPrefix(rec.Path, f.PathPrefix) {
			continue
		}
		if f.Mode != "" && !strings.EqualFold(string(rec.Mode), f.Mode) {
			continue
		}
		if f.StatusMin > 0 && rec.StatusCode < f.StatusMin {
			continue
		}
		if f.StatusMax > 0 && rec.StatusCode > f.StatusMax {
			continue
		}
		if f.OnlyChaos && rec.ChaosInjected == "" {
			continue
		}
		if f.OnlyDrift && rec.DiffBreaking == 0 && rec.DiffNonBreaking == 0 {
			continue
		}

		if skipped < f.Offset {
			skipped++
			continue
		}
		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// Metrics computes real-time latency percentiles (p50, p95, p99) and operational counters.
func (s *WALStore) Metrics() LatencyMetrics {
	s.mu.RLock()
	n := len(s.records)
	if n == 0 {
		s.mu.RUnlock()
		return LatencyMetrics{
			WALFlushes:        s.walFlushes.Load(),
			WALCorruptSkipped: s.corruptSkipped.Load(),
		}
	}

	latencies := make([]float64, 0, n)
	var totalOverhead float64
	var replayHits, chaosFaults, rateLimited, driftDetected, errCount int

	for i := 0; i < n; i++ {
		rec := s.records[i]
		latencies = append(latencies, rec.LatencyMs)
		totalOverhead += rec.ProxyOverheadMs
		if rec.Mode == ModeReplay {
			replayHits++
		}
		if rec.ChaosInjected != "" {
			chaosFaults++
		}
		if rec.RateLimited {
			rateLimited++
		}
		if rec.DiffBreaking > 0 || rec.DiffNonBreaking > 0 {
			driftDetected++
		}
		if rec.StatusCode >= 500 {
			errCount++
		}
	}
	s.mu.RUnlock()

	sort.Float64s(latencies)

	return LatencyMetrics{
		TotalRequests:     n,
		ReplayHits:        replayHits,
		ChaosFaults:       chaosFaults,
		RateLimitedCount:  rateLimited,
		DriftDetected:     driftDetected,
		ErrorRatePct:      math.Round((float64(errCount)/float64(n))*10000) / 100,
		P50LatencyMs:      percentile(latencies, 0.50),
		P95LatencyMs:      percentile(latencies, 0.95),
		P99LatencyMs:      percentile(latencies, 0.99),
		AvgProxyOverhead:  math.Round((totalOverhead/float64(n))*1000) / 1000,
		WALFlushes:        s.walFlushes.Load(),
		WALCorruptSkipped: s.corruptSkipped.Load(),
	}
}

func (s *WALStore) indexRecord(rec TraceRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.records) >= s.maxInMem {
		// Evict oldest 10% and compact indexes
		evictCount := s.maxInMem / 10
		if evictCount < 1 {
			evictCount = 1
		}
		s.records = append([]TraceRecord(nil), s.records[evictCount:]...)
		s.rebuildIndexesLocked()
	}

	idx := len(s.records)
	s.records = append(s.records, rec)
	s.byID[rec.ID] = idx

	// Index golden records (RECORD or PASSTHROUGH with 2xx/3xx status and no injected chaos)
	if (rec.Mode == ModeRecord || rec.Mode == ModePassthrough) && rec.ChaosInjected == "" && rec.StatusCode >= 200 && rec.StatusCode < 400 && rec.Fingerprint != "" {
		s.byFingerprint[rec.Fingerprint] = idx
	}
}

func (s *WALStore) rebuildIndexesLocked() {
	s.byID = make(map[string]int, len(s.records))
	s.byFingerprint = make(map[string]int, len(s.records)/2)
	for i, rec := range s.records {
		s.byID[rec.ID] = i
		if (rec.Mode == ModeRecord || rec.Mode == ModePassthrough) && rec.ChaosInjected == "" && rec.StatusCode >= 200 && rec.StatusCode < 400 && rec.Fingerprint != "" {
			s.byFingerprint[rec.Fingerprint] = i
		}
	}
}

func (s *WALStore) recoverFromWAL() error {
	f, err := os.Open(s.walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("storage: failed to open existing WAL: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 128*1024)
	scanner.Buffer(buf, 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var env walEnvelope
		if err := json.Unmarshal(line, &env); err != nil {
			s.corruptSkipped.Add(1)
			continue
		}
		payloadBytes, err := json.Marshal(env.Record)
		if err != nil {
			s.corruptSkipped.Add(1)
			continue
		}
		if crc32.ChecksumIEEE(payloadBytes) != env.CRC32 {
			s.corruptSkipped.Add(1)
			continue
		}
		s.indexRecord(env.Record)
	}
	return scanner.Err()
}

func (s *WALStore) asyncFlushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()

	batch := make([]TraceRecord, 0, s.batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		for _, rec := range batch {
			recBytes, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			env := walEnvelope{
				CRC32:  crc32.ChecksumIEEE(recBytes),
				Record: rec,
			}
			line, err := json.Marshal(env)
			if err != nil {
				continue
			}
			_, _ = s.walWriter.Write(line)
			_ = s.walWriter.WriteByte('\n')
		}
		batch = batch[:0]
		s.walFlushes.Add(1)
		return s.walWriter.Flush()
	}

	for {
		select {
		case rec := <-s.ingestCh:
			batch = append(batch, rec)
			if len(batch) >= s.batchSize {
				_ = flushBatch()
			}
		case <-ticker.C:
			_ = flushBatch()
		case done := <-s.flushReqCh:
			for {
				select {
				case rec := <-s.ingestCh:
					batch = append(batch, rec)
				default:
					goto drained
				}
			}
		drained:
			done <- flushBatch()
		case <-s.stopCh:
			for {
				select {
				case rec := <-s.ingestCh:
					batch = append(batch, rec)
				default:
					_ = flushBatch()
					return
				}
			}
		}
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return math.Round(sorted[idx]*100) / 100
}
