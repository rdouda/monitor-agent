// Package schemas embeds the normative JSON Schemas for the v1 wire format.
package schemas

import "embed"

// FS contains schemas/v1/*.schema.json.
//
//go:embed v1/*.schema.json
var FS embed.FS
