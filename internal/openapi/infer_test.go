package openapi

import (
	"testing"
	"time"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/storage"
)

func TestInferSpecFromTraces(t *testing.T) {
	tmpDir := t.TempDir()
	casStore, err := cas.NewStore(tmpDir, 64)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	reqBody := []byte(`{"model":"gpt-4o","temperature":0.7,"messages":[{"role":"user","content":"hello"}]}`)
	respBody := []byte(`{"id":"chatcmpl_1","object":"chat.completion","created":1700000000,"choices":[{"index":0,"message":{"role":"assistant","content":"Hi there!"}}]}`)

	reqHash, _ := casStore.Put(reqBody)
	respHash, _ := casStore.Put(respBody)

	traces := []storage.TraceRecord{
		{
			ID:              "tr_001",
			Timestamp:       time.Now().UTC(),
			Method:          "POST",
			Path:            "/v1/chat/completions",
			StatusCode:      200,
			RequestBodyHash: reqHash,
			RequestSize:     len(reqBody),
			ResponseHash:    respHash,
			ResponseSize:    len(respBody),
		},
	}

	spec, err := InferSpecFromTraces(traces, casStore, "Test LLM Gateway", "1.0.0")
	if err != nil {
		t.Fatalf("InferSpecFromTraces failed: %v", err)
	}

	if spec.OpenAPI != "3.1.0" {
		t.Errorf("expected OpenAPI 3.1.0, got %s", spec.OpenAPI)
	}

	pathObj, exists := spec.Paths["/v1/chat/completions"]
	if !exists {
		t.Fatalf("expected /v1/chat/completions path in spec")
	}

	op, hasPost := pathObj["post"]
	if !hasPost {
		t.Fatalf("expected POST operation")
	}

	if op.RequestBody == nil {
		t.Fatalf("expected requestBody definition")
	}
	reqSchema := op.RequestBody.Content["application/json"].Schema
	if reqSchema.Type != "object" {
		t.Errorf("expected request body type object, got %s", reqSchema.Type)
	}
	if reqSchema.Properties["model"].Type != "string" {
		t.Errorf("expected model property to be string, got %s", reqSchema.Properties["model"].Type)
	}

	resp200, has200 := op.Responses["200"]
	if !has200 {
		t.Fatalf("expected 200 response definition")
	}
	respSchema := resp200.Content["application/json"].Schema
	if respSchema.Type != "object" {
		t.Errorf("expected response body type object, got %s", respSchema.Type)
	}
}
