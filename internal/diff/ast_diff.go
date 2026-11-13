package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
)

// Severity classifies whether a structural or semantic change breaks downstream API consumers.
type Severity string

const (
	SeverityBreaking    Severity = "BREAKING"
	SeverityNonBreaking Severity = "NON_BREAKING"
	SeverityInfo        Severity = "INFO"
)

// ChangeKind describes the specific AST mutation detected between baseline and candidate responses.
type ChangeKind string

const (
	KindStatusChanged     ChangeKind = "STATUS_CODE_CHANGED"
	KindFieldRemoved      ChangeKind = "FIELD_REMOVED"
	KindFieldAdded        ChangeKind = "FIELD_ADDED"
	KindTypeMismatch      ChangeKind = "TYPE_MISMATCH"
	KindValueChanged      ChangeKind = "VALUE_CHANGED"
	KindArrayLengthChange ChangeKind = "ARRAY_LENGTH_CHANGED"
	KindLatencyRegression ChangeKind = "LATENCY_REGRESSION"
)

// ChangeEntry represents a single node-level difference in an API response contract.
type ChangeEntry struct {
	Path      string     `json:"path"`
	Kind      ChangeKind `json:"kind"`
	Severity  Severity   `json:"severity"`
	Baseline  string     `json:"baseline,omitempty"`
	Candidate string     `json:"candidate,omitempty"`
	Message   string     `json:"message"`
}

// Report summarizes all structural, semantic, and latency differences between two traces.
type Report struct {
	BaselineTraceID  string        `json:"baseline_trace_id"`
	CandidateTraceID string        `json:"candidate_trace_id"`
	Compatible       bool          `json:"compatible"`
	BreakingCount    int           `json:"breaking_count"`
	NonBreakingCount int           `json:"non_breaking_count"`
	IgnoredEphemeral int           `json:"ignored_ephemeral"`
	Changes          []ChangeEntry `json:"changes"`
}

// Options configures how the AST analyzer treats volatile fields and latency thresholds.
type Options struct {
	IgnoreKeys          map[string]bool
	IgnoreDynamicValues bool
	LatencyRegressionMs float64
}

var (
	uuidRegex    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	iso8601Regex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)
)

// DefaultOptions returns production defaults that automatically ignore common volatile trace/timestamp fields.
func DefaultOptions() Options {
	return Options{
		IgnoreKeys: map[string]bool{
			"request_id": true,
			"trace_id":   true,
			"span_id":    true,
			"timestamp":  true,
			"created_at": true,
			"updated_at": true,
			"nonce":      true,
		},
		IgnoreDynamicValues: true,
		LatencyRegressionMs: 100.0,
	}
}

// Analyzer performs recursive AST comparison of HTTP statuses, JSON payloads, and latency profiles.
type Analyzer struct {
	opts Options
}

// NewAnalyzer creates a new AST drift analyzer with the given options.
func NewAnalyzer(opts Options) *Analyzer {
	if opts.IgnoreKeys == nil {
		opts = DefaultOptions()
	}
	return &Analyzer{opts: opts}
}

// Compare evaluates a baseline (golden) response against a candidate response and returns a structured Report.
func (a *Analyzer) Compare(
	baselineID, candidateID string,
	baseStatus, candStatus int,
	baseBody, candBody []byte,
	baseLatencyMs, candLatencyMs float64,
) Report {
	report := Report{
		BaselineTraceID:  baselineID,
		CandidateTraceID: candidateID,
		Compatible:       true,
		Changes:          make([]ChangeEntry, 0),
	}

	// 1. Compare HTTP status codes
	if baseStatus != candStatus {
		sev := SeverityNonBreaking
		if (baseStatus/100) != (candStatus/100) || candStatus >= 400 {
			sev = SeverityBreaking
		}
		report.addChange(ChangeEntry{
			Path:      "$.http.status_code",
			Kind:      KindStatusChanged,
			Severity:  sev,
			Baseline:  fmt.Sprintf("%d", baseStatus),
			Candidate: fmt.Sprintf("%d", candStatus),
			Message:   fmt.Sprintf("HTTP status changed from %d to %d", baseStatus, candStatus),
		})
	}

	// 2. Compare latency drift if threshold configured
	if a.opts.LatencyRegressionMs > 0 && baseLatencyMs > 0 && candLatencyMs > baseLatencyMs {
		delta := candLatencyMs - baseLatencyMs
		if delta >= a.opts.LatencyRegressionMs && candLatencyMs >= baseLatencyMs*1.5 {
			report.addChange(ChangeEntry{
				Path:      "$.telemetry.latency_ms",
				Kind:      KindLatencyRegression,
				Severity:  SeverityNonBreaking,
				Baseline:  fmt.Sprintf("%.2fms", baseLatencyMs),
				Candidate: fmt.Sprintf("%.2fms", candLatencyMs),
				Message:   fmt.Sprintf("Latency regressed by +%.2fms (%.1fx slower)", delta, candLatencyMs/baseLatencyMs),
			})
		}
	}

	// 3. Parse JSON ASTs
	trimmedBase := bytes.TrimSpace(baseBody)
	trimmedCand := bytes.TrimSpace(candBody)

	if len(trimmedBase) == 0 && len(trimmedCand) == 0 {
		return report
	}

	var baseNode, candNode any
	errBase := json.Unmarshal(trimmedBase, &baseNode)
	errCand := json.Unmarshal(trimmedCand, &candNode)

	if errBase != nil || errCand != nil {
		// Fallback to raw byte comparison for non-JSON bodies
		if !bytes.Equal(trimmedBase, trimmedCand) {
			report.addChange(ChangeEntry{
				Path:      "$.body",
				Kind:      KindValueChanged,
				Severity:  SeverityBreaking,
				Baseline:  truncatePreview(string(trimmedBase), 64),
				Candidate: truncatePreview(string(trimmedCand), 64),
				Message:   "Raw non-JSON response payload diverged",
			})
		}
		return report
	}

	a.diffNodes("$", "", baseNode, candNode, &report)
	return report
}

func (a *Analyzer) diffNodes(path, fieldKey string, base, cand any, report *Report) {
	if fieldKey != "" && a.opts.IgnoreKeys[strings.ToLower(fieldKey)] {
		// Still verify the type didn't break (e.g. string -> null), but ignore value changes
		if jsonTypeOf(base) == jsonTypeOf(cand) {
			report.IgnoredEphemeral++
			return
		}
	}

	baseType := jsonTypeOf(base)
	candType := jsonTypeOf(cand)

	if baseType != candType {
		report.addChange(ChangeEntry{
			Path:      path,
			Kind:      KindTypeMismatch,
			Severity:  SeverityBreaking,
			Baseline:  baseType,
			Candidate: candType,
			Message:   fmt.Sprintf("Contract type changed from %s to %s at %s", baseType, candType, path),
		})
		return
	}

	switch bVal := base.(type) {
	case map[string]any:
		cVal := cand.(map[string]any)
		keysMap := make(map[string]struct{}, len(bVal)+len(cVal))
		for k := range bVal {
			keysMap[k] = struct{}{}
		}
		for k := range cVal {
			keysMap[k] = struct{}{}
		}
		sortedKeys := make([]string, 0, len(keysMap))
		for k := range keysMap {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)

		for _, k := range sortedKeys {
			childPath := path + "." + k
			bChild, bHas := bVal[k]
			cChild, cHas := cVal[k]

			if bHas && !cHas {
				report.addChange(ChangeEntry{
					Path:      childPath,
					Kind:      KindFieldRemoved,
					Severity:  SeverityBreaking,
					Baseline:  formatValuePreview(bChild),
					Candidate: "<missing>",
					Message:   fmt.Sprintf("Required field %q removed from object at %s", k, path),
				})
				continue
			}
			if !bHas && cHas {
				report.addChange(ChangeEntry{
					Path:      childPath,
					Kind:      KindFieldAdded,
					Severity:  SeverityNonBreaking,
					Baseline:  "<missing>",
					Candidate: formatValuePreview(cChild),
					Message:   fmt.Sprintf("New field %q added to object at %s", k, path),
				})
				continue
			}
			a.diffNodes(childPath, k, bChild, cChild, report)
		}

	case []any:
		cVal := cand.([]any)
		if len(bVal) != len(cVal) {
			report.addChange(ChangeEntry{
				Path:      path,
				Kind:      KindArrayLengthChange,
				Severity:  SeverityNonBreaking,
				Baseline:  fmt.Sprintf("len=%d", len(bVal)),
				Candidate: fmt.Sprintf("len=%d", len(cVal)),
				Message:   fmt.Sprintf("Array length changed from %d to %d at %s", len(bVal), len(cVal), path),
			})
		}
		minLen := len(bVal)
		if len(cVal) < minLen {
			minLen = len(cVal)
		}
		for i := 0; i < minLen; i++ {
			elemPath := fmt.Sprintf("%s[%d]", path, i)
			a.diffNodes(elemPath, "", bVal[i], cVal[i], report)
		}

	case string:
		cStr := cand.(string)
		if bVal != cStr {
			if a.opts.IgnoreDynamicValues && isDynamicString(bVal) && isDynamicString(cStr) {
				report.IgnoredEphemeral++
				return
			}
			report.addChange(ChangeEntry{
				Path:      path,
				Kind:      KindValueChanged,
				Severity:  SeverityNonBreaking,
				Baseline:  truncatePreview(bVal, 48),
				Candidate: truncatePreview(cStr, 48),
				Message:   fmt.Sprintf("String value changed at %s", path),
			})
		}

	case float64:
		cNum := cand.(float64)
		if math.Abs(bVal-cNum) > 1e-9 {
			report.addChange(ChangeEntry{
				Path:      path,
				Kind:      KindValueChanged,
				Severity:  SeverityNonBreaking,
				Baseline:  fmt.Sprintf("%v", bVal),
				Candidate: fmt.Sprintf("%v", cNum),
				Message:   fmt.Sprintf("Numeric value changed from %v to %v at %s", bVal, cNum, path),
			})
		}

	case bool:
		cBool := cand.(bool)
		if bVal != cBool {
			report.addChange(ChangeEntry{
				Path:      path,
				Kind:      KindValueChanged,
				Severity:  SeverityNonBreaking,
				Baseline:  fmt.Sprintf("%v", bVal),
				Candidate: fmt.Sprintf("%v", cBool),
				Message:   fmt.Sprintf("Boolean flag flipped from %v to %v at %s", bVal, cBool, path),
			})
		}
	}
}

func (r *Report) addChange(c ChangeEntry) {
	r.Changes = append(r.Changes, c)
	if c.Severity == SeverityBreaking {
		r.BreakingCount++
		r.Compatible = false
	} else {
		r.NonBreakingCount++
	}
}

func jsonTypeOf(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func isDynamicString(s string) bool {
	return uuidRegex.MatchString(s) || iso8601Regex.MatchString(s)
}

func formatValuePreview(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return truncatePreview(string(b), 48)
}

func truncatePreview(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
