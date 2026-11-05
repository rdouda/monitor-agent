package containers

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"agent/internal/core"
	"agent/internal/modules/containers/docker"
	"agent/internal/modules/containers/model"
	"agent/internal/modules/containers/podman"
)

// candidate is one possible runtime socket.
type candidate struct {
	path       string
	hint       string
	ownerUID   int
	configured bool
}

// discoverCandidates merges configured endpoints with well-known paths under
// the configured hostfs root. Symlinks are resolved and duplicates removed so
// podman-docker symlinks are probed only once.
func discoverCandidates(d core.Deps) []candidate {
	cfg := d.Config.Modules.Containers
	hostfs := d.Paths.HostfsRoot

	var raw []candidate
	for _, rt := range cfg.Runtimes {
		p := rt.Endpoint
		switch {
		case strings.HasPrefix(p, "unix://"):
			p = strings.TrimPrefix(p, "unix://")
		case strings.Contains(p, "://"):
			continue // only unix sockets are supported
		}
		if !filepath.IsAbs(p) {
			continue
		}
		raw = append(raw, candidate{path: p, hint: rt.Type, configured: true})
	}

	if cfg.Autodiscovery {
		wellKnown := []candidate{
			{path: "/var/run/docker.sock", hint: "docker"},
			{path: "/run/docker.sock", hint: "docker"},
			{path: "/run/podman/podman.sock", hint: "podman"},
		}
		for _, c := range wellKnown {
			c.path = hostPath(hostfs, c.path)
			raw = append(raw, c)
		}
		for _, pattern := range []string{"/run/user/*/podman/podman.sock", "/run/user/*/docker.sock"} {
			matches, _ := filepath.Glob(hostPath(hostfs, pattern))
			sort.Strings(matches)
			for _, p := range matches {
				hint := "docker"
				if strings.Contains(p, "podman") {
					hint = "podman"
				}
				raw = append(raw, candidate{path: p, hint: hint})
			}
		}
		if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
			for _, p := range []string{
				filepath.Join(xdg, "podman", "podman.sock"),
				filepath.Join(xdg, "docker.sock"),
			} {
				hint := "docker"
				if strings.Contains(p, "podman") {
					hint = "podman"
				}
				raw = append(raw, candidate{path: hostPath(hostfs, p), hint: hint})
			}
		}
	}

	seen := map[string]bool{}
	out := make([]candidate, 0, len(raw))
	for _, c := range raw {
		resolved, err := filepath.EvalSymlinks(c.path)
		if err != nil {
			continue
		}
		if seen[resolved] {
			continue
		}
		info, err := os.Stat(resolved)
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			continue
		}
		uid := 0
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			uid = int(st.Uid)
		}
		seen[resolved] = true
		c.path = resolved
		c.ownerUID = uid
		if c.hint == "" {
			c.hint = hintFromPath(resolved)
		}
		out = append(out, c)
	}
	return out
}

func setStatsConcurrency(a model.RuntimeAdapter, n int) {
	type setter interface{ SetStatsConcurrency(int) }
	if s, ok := a.(setter); ok && n > 0 {
		s.SetStatsConcurrency(n)
	}
}

func hintFromPath(p string) string {
	if strings.Contains(strings.ToLower(p), "podman") {
		return "podman"
	}
	return "docker"
}

// detectEngine probes a socket and returns the matching adapter. Both
// adapters are tried because the runtime identity must come from the API
// response, not from the socket path.
func detectEngine(ctx context.Context, c candidate, statsConcurrency int) (model.RuntimeAdapter, model.Engine, error) {
	adapters := []model.RuntimeAdapter{docker.New(c.path), podman.New(c.path)}
	if c.hint == "podman" {
		adapters[0], adapters[1] = adapters[1], adapters[0]
	}
	var lastErr error
	for _, a := range adapters {
		engine, err := a.Probe(ctx)
		if err == nil {
			engine.OwnerUID = c.ownerUID
			engine.Status = "available"
			setStatsConcurrency(a, statsConcurrency)
			return a, engine, nil
		}
		if errors.Is(err, model.ErrWrongRuntime) {
			continue
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no container runtime answered")
	}
	return nil, model.Engine{}, lastErr
}
