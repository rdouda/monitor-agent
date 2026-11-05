// Package schema loads and validates the normative v1 JSON Schemas.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"sort"

	"agent/schemas"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// BaseURL is the logical location used to resolve the relative references
// between the schema files. It is never fetched.
const BaseURL = "https://agent.local/schemas/v1/"

// Set holds the compiled schemas used by the agent and the mock server.
type Set struct {
	Envelope        *jsonschema.Schema
	EnrollRequest   *jsonschema.Schema
	EnrollResponse  *jsonschema.Schema
	IngestResponse  *jsonschema.Schema
	compiledSchemas map[string]*jsonschema.Schema
}

// Load compiles every schema embedded in the binary.
func Load() (*Set, error) {
	return loadFS(schemas.FS, "v1")
}

// LoadDir compiles schemas from a directory on disk. It is used by the mock
// server to validate against a working copy of the schemas.
func LoadDir(dir string) (*Set, error) {
	return loadFS(os.DirFS(dir), ".")
}

func loadFS(fsys fs.FS, dir string) (*Set, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read schema dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()

	for _, name := range names {
		raw, err := fs.ReadFile(fsys, dir+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		if err := compiler.AddResource(BaseURL+name, doc); err != nil {
			return nil, fmt.Errorf("add %s: %w", name, err)
		}
	}

	set := &Set{compiledSchemas: map[string]*jsonschema.Schema{}}
	for _, name := range names {
		sch, err := compiler.Compile(BaseURL + name)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", name, err)
		}
		set.compiledSchemas[name] = sch
	}

	set.Envelope = set.compiledSchemas["envelope.schema.json"]
	set.EnrollRequest = set.compiledSchemas["enroll-request.schema.json"]
	set.EnrollResponse = set.compiledSchemas["enroll-response.schema.json"]
	set.IngestResponse = set.compiledSchemas["ingest-response.schema.json"]
	if set.Envelope == nil || set.EnrollRequest == nil || set.EnrollResponse == nil || set.IngestResponse == nil {
		return nil, fmt.Errorf("one or more required schemas are missing")
	}
	return set, nil
}

// ValidateEnvelope validates raw JSON of one CloudEvent against the envelope
// and the type-specific data schema.
func (s *Set) ValidateEnvelope(raw []byte) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return s.Envelope.Validate(v)
}

// ValidateValue validates an already-decoded JSON value.
func (s *Set) ValidateValue(raw []byte, name string) error {
	sch, ok := s.compiledSchemas[name]
	if !ok {
		return fmt.Errorf("unknown schema %q", name)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return sch.Validate(v)
}

// Schema returns a compiled schema by file name, or nil.
func (s *Set) Schema(name string) *jsonschema.Schema {
	return s.compiledSchemas[name]
}
