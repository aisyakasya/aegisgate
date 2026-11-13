package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"sort"
	"strings"
)

var volatileBodyFields = map[string]bool{
	"request_id": true,
	"trace_id":   true,
	"timestamp":  true,
	"nonce":      true,
}

// ComputeFingerprint generates a deterministic canonical SHA-256 fingerprint for an HTTP request.
// It normalizes method, path, sorted query parameters, and canonicalized JSON keys so semantically
// identical API/LLM requests resolve to the same replay key regardless of JSON key ordering.
func ComputeFingerprint(method, rawPath, rawQuery string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(strings.ToUpper(strings.TrimSpace(method))))
	h.Write([]byte("|"))
	h.Write([]byte(normalizePath(rawPath)))
	h.Write([]byte("|"))
	h.Write([]byte(canonicalQuery(rawQuery)))
	h.Write([]byte("|"))
	h.Write(canonicalBody(body))

	sum := h.Sum(nil)
	return "fp_" + hex.EncodeToString(sum[:12])
}

func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		return strings.TrimSuffix(p, "/")
	}
	return p
}

func canonicalQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		return rawQuery
	}
	// Strip common cache-buster query params
	delete(vals, "_")
	delete(vals, "_t")
	delete(vals, "nonce")
	return vals.Encode() // url.Values.Encode() sorts keys lexicographically
}

func canonicalBody(body []byte) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil
	}
	var parsed any
	if err := json.Unmarshal(trimmed, &parsed); err != nil {
		return trimmed
	}
	cleaned := stripVolatileFields(parsed)
	// json.Marshal sorts map keys deterministically in Go's standard library
	canonical, err := json.Marshal(cleaned)
	if err != nil {
		return trimmed
	}
	return canonical
}

func stripVolatileFields(node any) any {
	switch v := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		keys := make([]string, 0, len(v))
		for k := range v {
			if volatileBodyFields[strings.ToLower(k)] {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out[k] = stripVolatileFields(v[k])
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = stripVolatileFields(v[i])
		}
		return out
	default:
		return v
	}
}
