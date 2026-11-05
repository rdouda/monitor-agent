package modules

import (
	"testing"

	"agent/internal/config"
	"agent/internal/core"
)

func TestBuildHonorsModuleToggles(t *testing.T) {
	cfg := config.Default()
	deps := core.Deps{Config: cfg}

	mods, err := Build(deps)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"host", "process", "containers", "network"}
	if got := moduleNames(mods); !equal(got, want) {
		t.Fatalf("modules = %v, want %v", got, want)
	}

	cfg.Modules.Process.Enabled = false
	cfg.Modules.Containers.Enabled = false
	cfg.Modules.Network.Enabled = false
	mods, err = Build(deps)
	if err != nil {
		t.Fatal(err)
	}
	if got := moduleNames(mods); !equal(got, []string{"host"}) {
		t.Fatalf("modules = %v, want host only", got)
	}

	cfg.Modules.Containers.Enabled = true
	mods, err = Build(deps)
	if err != nil {
		t.Fatal(err)
	}
	if got := moduleNames(mods); !equal(got, []string{"host", "containers"}) {
		t.Fatalf("modules = %v", got)
	}
}

func moduleNames(mods []core.Module) []string {
	out := make([]string, 0, len(mods))
	for _, m := range mods {
		out = append(out, m.Name())
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
