package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

func load(t *testing.T) *Set {
	t.Helper()
	s, err := Load()
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	return s
}

func TestAllSchemasCompile(t *testing.T) {
	s := load(t)
	for _, name := range []string{
		"common.schema.json", "envelope.schema.json", "entity-upsert.schema.json",
		"relationship-upsert.schema.json", "relationship-removed.schema.json",
		"metric-batch.schema.json", "lifecycle-changed.schema.json",
		"snapshot-completed.schema.json", "heartbeat.schema.json",
		"ingest-response.schema.json", "enroll-request.schema.json",
		"enroll-response.schema.json",
	} {
		if s.Schema(name) == nil {
			t.Errorf("schema %s did not compile", name)
		}
	}
}

const validEntityUpsert = `{
  "specversion": "1.0",
  "id": "0192f3a4-7c1e-7b52-9d3a-5c8e1f2a4b60",
  "source": "urn:agent:6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
  "type": "agent.entity.upsert.v1",
  "subject": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
  "time": "2026-11-04T20:00:00Z",
  "datacontenttype": "application/json",
  "agentid": "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
  "agentseq": 1001,
  "agentrun": "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
  "environment": "production",
  "data": {
    "snapshot_id": "0192f3a4-8000-7000-8000-000000000001",
    "entities": [
      {
        "id": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
        "kind": "host",
        "name": "web-01",
        "attributes": {"os": "linux"},
        "observed_at": "2026-11-04T20:00:00Z"
      }
    ]
  }
}`

func TestValidEntityUpsert(t *testing.T) {
	if err := load(t).ValidateEnvelope([]byte(validEntityUpsert)); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestEnvelopeRejectsBadInput(t *testing.T) {
	s := load(t)
	tests := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"bad id", func(m map[string]any) { m["id"] = "not-a-uuid" }},
		{"agentseq zero", func(m map[string]any) { m["agentseq"] = 0 }},
		{"unknown field", func(m map[string]any) { m["extra"] = true }},
		{"empty environment", func(m map[string]any) { m["environment"] = "" }},
		{"bad type", func(m map[string]any) { m["type"] = "agent.unknown.v1" }},
		{"missing data", func(m map[string]any) { delete(m, "data") }},
		{"entity missing attributes", func(m map[string]any) {
			data := m["data"].(map[string]any)
			list := data["entities"].([]any)
			delete(list[0].(map[string]any), "attributes")
		}},
		{"metric gauge with temporality", func(m map[string]any) {
			m["type"] = "agent.metric.batch.v1"
			m["data"] = map[string]any{
				"observed_at": "2026-11-04T20:00:00Z",
				"readings": []any{map[string]any{
					"entity_id": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
					"metrics": []any{map[string]any{
						"name": "host.cpu.utilization", "kind": "gauge",
						"value": 0.5, "unit": "ratio", "temporality": "cumulative",
					}},
				}},
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal([]byte(validEntityUpsert), &m); err != nil {
				t.Fatal(err)
			}
			tt.mut(m)
			raw, _ := json.Marshal(m)
			if err := s.ValidateEnvelope(raw); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCounterRequiresTemporality(t *testing.T) {
	s := load(t)
	good := map[string]any{
		"specversion":     "1.0",
		"id":              "0192f3a4-7c1e-7b52-9d3a-5c8e1f2a4b60",
		"source":          "urn:agent:6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		"type":            "agent.metric.batch.v1",
		"subject":         "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
		"time":            "2026-11-04T20:00:05Z",
		"datacontenttype": "application/json",
		"agentid":         "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		"agentseq":        2,
		"agentrun":        "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		"environment":     "production",
		"data": map[string]any{
			"observed_at": "2026-11-04T20:00:05Z",
			"readings": []any{map[string]any{
				"entity_id": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
				"metrics": []any{map[string]any{
					"name": "host.disk.io.read.bytes", "kind": "counter",
					"value": 42, "unit": "By", "temporality": "cumulative",
				}},
			}},
		},
	}
	raw, _ := json.Marshal(good)
	if err := s.ValidateEnvelope(raw); err != nil {
		t.Fatalf("valid counter rejected: %v", err)
	}
	data := good["data"].(map[string]any)
	metric := data["readings"].([]any)[0].(map[string]any)["metrics"].([]any)[0].(map[string]any)
	delete(metric, "temporality")
	raw, _ = json.Marshal(good)
	err := s.ValidateEnvelope(raw)
	if err == nil || !strings.Contains(err.Error(), "temporality") {
		t.Fatalf("counter without temporality must fail, got %v", err)
	}
}

func TestEnrollAndIngestSchemas(t *testing.T) {
	s := load(t)
	req := `{"agent_id":"6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55","host_id":"3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03","hostname":"web-01","agent_version":"1.0.0","os":"linux","architecture":"amd64"}`
	if err := s.ValidateValue([]byte(req), "enroll-request.schema.json"); err != nil {
		t.Fatalf("valid enroll request rejected: %v", err)
	}
	resp := `{"agent_id":"6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55","credential":"abcdefghijklmnopqrstuvwxyz012345"}`
	if err := s.ValidateValue([]byte(resp), "enroll-response.schema.json"); err != nil {
		t.Fatalf("valid enroll response rejected: %v", err)
	}
	bad := `{"status":"accepted","accepted_count":1,"rejected_count":0,"rejected":[{"id":"0192f3a4-7c1e-7b52-9d3a-5c8e1f2a4b60","code":"nope","retryable":false}]}`
	if err := s.ValidateValue([]byte(bad), "ingest-response.schema.json"); err == nil {
		t.Fatal("invalid reject code must fail")
	}
}
