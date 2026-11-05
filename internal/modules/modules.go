// Package modules builds the enabled collector modules.
package modules

import (
	"fmt"

	"agent/internal/core"
	"agent/internal/modules/containers"
	"agent/internal/modules/host"
	"agent/internal/modules/network"
	"agent/internal/modules/process"
)

// Build returns the modules enabled by cfg. The host module is always
// present.
func Build(d core.Deps) ([]core.Module, error) {
	mods := []core.Module{host.New()}
	if d.Config.Modules.Process.Enabled {
		mods = append(mods, process.New())
	}
	if d.Config.Modules.Containers.Enabled {
		mods = append(mods, containers.New())
	}
	if d.Config.Modules.Network.Enabled {
		mods = append(mods, network.New())
	}
	return mods, nil
}

// BuildSelected returns the enabled modules in requested order. An empty
// names slice means all enabled modules. Unknown or disabled modules are an
// error.
func BuildSelected(d core.Deps, names []string) ([]core.Module, error) {
	all, err := Build(d)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return all, nil
	}
	byName := make(map[string]core.Module, len(all))
	for _, mod := range all {
		byName[mod.Name()] = mod
	}
	out := make([]core.Module, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		mod, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("module %q is not enabled", name)
		}
		out = append(out, mod)
	}
	return out, nil
}
