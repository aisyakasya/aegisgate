package openapi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/storage"
)

// SchemaNode represents a JSON Schema / OpenAPI 3.1 type definition.
type SchemaNode struct {
	Type        string                 `json:"type,omitempty"`
	Format      string                 `json:"format,omitempty"`
	Properties  map[string]*SchemaNode `json:"properties,omitempty"`
	Items       *SchemaNode            `json:"items,omitempty"`
	Required    []string               `json:"required,omitempty"`
	Description string                 `json:"description,omitempty"`
}

// ResponseDef describes an HTTP status code response specification.
type ResponseDef struct {
	Description string                 `json:"description"`
	Content     map[string]MediaObjDef `json:"content,omitempty"`
}

// MediaObjDef describes the media type payload structure.
type MediaObjDef struct {
	Schema *SchemaNode `json:"schema,omitempty"`
}

// RequestBodyDef describes an HTTP request body specification.
type RequestBodyDef struct {
	Required bool                   `json:"required"`
	Content  map[string]MediaObjDef `json:"content,omitempty"`
}

// OperationDef represents a single HTTP method operation on a route.
type OperationDef struct {
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	RequestBody *RequestBodyDef        `json:"requestBody,omitempty"`
	Responses   map[string]ResponseDef `json:"responses"`
}

// Spec represents a complete OpenAPI 3.1 document.
type Spec struct {
	OpenAPI string                             `json:"openapi"`
	Info    map[string]string                  `json:"info"`
	Paths   map[string]map[string]OperationDef `json:"paths"`
}

// InferSpecFromTraces iterates over recorded WAL traces, retrieves payloads from CAS,
// and synthesizes a structured OpenAPI 3.1 specification document.
func InferSpecFromTraces(traces []storage.TraceRecord, casStore *cas.Store, title, version string) (*Spec, error) {
	if title == "" {
		title = "AegisGate Inferred API"
	}
	if version == "" {
		version = "1.0.0"
	}

	spec := &Spec{
		OpenAPI: "3.1.0",
		Info: map[string]string{
			"title":       title,
			"version":     version,
			"description": "Synthesized from live recorded traffic via AegisGate Write-Ahead Log",
		},
		Paths: make(map[string]map[string]OperationDef),
	}

	// Sort traces to process deterministic baselines first
	for _, tr := range traces {
		if tr.Path == "" || tr.StatusCode == 0 {
			continue
		}

		pathKey := tr.Path
		methodKey := strings.ToLower(tr.Method)

		if _, exists := spec.Paths[pathKey]; !exists {
			spec.Paths[pathKey] = make(map[string]OperationDef)
		}

		op, exists := spec.Paths[pathKey][methodKey]
		if !exists {
			op = OperationDef{
				Summary:   fmt.Sprintf("%s %s", strings.ToUpper(methodKey), pathKey),
				Responses: make(map[string]ResponseDef),
			}
		}

		// Infer Request Body Schema if present
		if tr.RequestBodyHash != "" && tr.RequestSize > 2 {
			if reqBytes, err := casStore.Get(tr.RequestBodyHash); err == nil && len(reqBytes) > 0 {
				var reqJSON any
				if err := json.Unmarshal(reqBytes, &reqJSON); err == nil {
					schema := inferNodeSchema(reqJSON)
					op.RequestBody = &RequestBodyDef{
						Required: true,
						Content: map[string]MediaObjDef{
							"application/json": {Schema: schema},
						},
					}
				}
			}
		}

		// Infer Response Body Schema
		statusStr := fmt.Sprintf("%d", tr.StatusCode)
		respDef, hasStatus := op.Responses[statusStr]
		if !hasStatus {
			respDef = ResponseDef{
				Description: fmt.Sprintf("HTTP %d Response", tr.StatusCode),
				Content:     make(map[string]MediaObjDef),
			}
		}

		if tr.ResponseHash != "" && tr.ResponseSize > 2 {
			if respBytes, err := casStore.Get(tr.ResponseHash); err == nil && len(respBytes) > 0 {
				var respJSON any
				if err := json.Unmarshal(respBytes, &respJSON); err == nil {
					schema := inferNodeSchema(respJSON)
					respDef.Content["application/json"] = MediaObjDef{Schema: schema}
				}
			}
		}

		op.Responses[statusStr] = respDef
		spec.Paths[pathKey][methodKey] = op
	}

	return spec, nil
}

func inferNodeSchema(v any) *SchemaNode {
	if v == nil {
		return &SchemaNode{Type: "null"}
	}

	switch val := v.(type) {
	case bool:
		return &SchemaNode{Type: "boolean"}
	case float64:
		if val == float64(int64(val)) {
			return &SchemaNode{Type: "integer"}
		}
		return &SchemaNode{Type: "number"}
	case string:
		node := &SchemaNode{Type: "string"}
		if strings.Contains(val, "T") && strings.HasSuffix(val, "Z") {
			node.Format = "date-time"
		}
		return node
	case []any:
		node := &SchemaNode{Type: "array"}
		if len(val) > 0 {
			node.Items = inferNodeSchema(val[0])
		}
		return node
	case map[string]any:
		node := &SchemaNode{
			Type:       "object",
			Properties: make(map[string]*SchemaNode, len(val)),
			Required:   make([]string, 0, len(val)),
		}
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			node.Properties[k] = inferNodeSchema(val[k])
			node.Required = append(node.Required, k)
		}
		return node
	default:
		return &SchemaNode{Type: "string"}
	}
}
