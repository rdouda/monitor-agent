package docker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/internal/modules/containers/model"
)

func serveUnix(t *testing.T, handler http.Handler) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "docker.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(path)
	})
	return path
}

type fakeDocker struct {
	mu           sync.Mutex
	state        string
	inspectCalls int
	inspectFail  string
}

func (f *fakeDocker) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		path := r.URL.Path
		switch {
		case path == "/version":
			fmt.Fprint(w, `{"Version":"26.1.0","ApiVersion":"1.45","Components":[{"Name":"Engine","Version":"26.1.0"}]}`)
		case path == "/v1.45/info":
			fmt.Fprint(w, `{"Driver":"overlay2","CgroupVersion":"2","SecurityOptions":["name=rootless"]}`)
		case path == "/v1.45/containers/json":
			f.mu.Lock()
			state := f.state
			f.mu.Unlock()
			fmt.Fprintf(w, `[{"Id":"web1","Names":["/web"],"Image":"nginx:1.25","ImageID":"sha256:abc123","State":%q,"Status":"Up 2 hours"},{"Id":"db1","Names":["/db"],"State":"exited","Status":"Exited (0)"}]`, state)
		case strings.HasPrefix(path, "/v1.45/containers/web1/json"):
			f.mu.Lock()
			f.inspectCalls++
			fail := f.inspectFail
			state := f.state
			f.mu.Unlock()
			if fail != "" {
				http.Error(w, fail, http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, `{
			  "Id":"web1","Name":"/web","Created":"2026-11-04T18:00:00.123456789Z",
			  "State":{"Status":%q,"Running":true,"Pid":4821,"ExitCode":0,
			    "StartedAt":"2026-11-04T18:00:01.5Z","FinishedAt":"0001-01-01T00:00:00Z",
			    "Health":{"Status":"healthy"}},
			  "Image":"sha256:abc123",
			  "Config":{"Image":"nginx:1.25","Labels":{"com.docker.compose.service":"web","secret":"no"}},
			  "HostConfig":{"RestartPolicy":{"Name":"unless-stopped"}},
			  "NetworkSettings":{"Ports":{"80/tcp":[{"HostIp":"0.0.0.0","HostPort":"8080"}]},
			    "Networks":{"bridge":{"NetworkID":"net1","IPAddress":"172.17.0.2","Gateway":"172.17.0.1","Aliases":["web"]}}},
			  "Mounts":[{"Type":"volume","Name":"www","Destination":"/usr/share/nginx/html","ReadOnly":false}],
			  "RestartCount":3
			}`, state)
		case strings.HasPrefix(path, "/v1.45/containers/db1/json"):
			fmt.Fprint(w, `{"Id":"db1","Name":"/db","Created":"2026-11-03T10:00:00Z","State":{"Status":"exited","ExitCode":1,"FinishedAt":"2026-11-04T10:00:00Z"},"Image":"sha256:def456","Config":{"Image":"postgres:16","Labels":{}},"HostConfig":{"RestartPolicy":{"Name":"always"}},"Mounts":[]}`)
		case strings.HasPrefix(path, "/v1.45/containers/web1/stats"):
			fmt.Fprint(w, `{"cpu_stats":{"cpu_usage":{"total_usage":1000000000}},
			  "memory_stats":{"usage":1000,"limit":2000,"stats":{"total_inactive_file":100}},
			  "networks":{"eth0":{"rx_bytes":10,"tx_bytes":20},"eth1":{"rx_bytes":5,"tx_bytes":6}},
			  "blkio_stats":{"io_service_bytes_recursive":[{"op":"read","value":30},{"op":"write","value":40}]},
			  "pids_stats":{"current":5}}`)
		case path == "/v1.45/images/json":
			fmt.Fprint(w, `[{"Id":"sha256:abc123","RepoTags":["nginx:1.25"],"RepoDigests":["nginx@sha256:aaa"],"Created":1762290000,"Size":1234,"Architecture":"amd64","OS":"linux"}]`)
		case path == "/v1.45/networks":
			fmt.Fprint(w, `[{"Id":"net1","Name":"bridge","Driver":"bridge","Scope":"local","EnableIPv6":false,"Internal":false,"IPAM":{"Config":[{"Subnet":"172.17.0.0/16","Gateway":"172.17.0.1"}]},"Options":{}}]`)
		case path == "/v1.45/volumes":
			fmt.Fprint(w, `{"Volumes":[{"Name":"www","Driver":"local","Scope":"local","Mountpoint":"/var/lib/docker/volumes/www/_data","CreatedAt":"2026-11-01T00:00:00Z"}]}`)
		case path == "/v1.45/events":
			w.Header().Set("Content-Type", "application/json")
			flusher, _ := w.(http.Flusher)
			fmt.Fprint(w, `{"Type":"container","Action":"die","Actor":{"ID":"web1","Attributes":{"exitCode":"137"}},"time":1762290000,"timeNano":1762290000123456789}`+"\n")
			fmt.Fprint(w, `{"Type":"container","Action":"health_status: unhealthy","Actor":{"ID":"web1"},"time":1762290001}`+"\n")
			if flusher != nil {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	})
}

func newFake(t *testing.T) (*fakeDocker, *Adapter, string) {
	t.Helper()
	fake := &fakeDocker{state: "running"}
	path := serveUnix(t, fake.handler())
	return fake, New(path), path
}

func TestProbeIdentifiesDocker(t *testing.T) {
	_, a, path := newFake(t)
	e, err := a.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if e.Runtime != "docker" || e.APIVersion != "1.45" || e.StorageDriver != "overlay2" || e.CgroupVersion != 2 || !e.Rootless {
		t.Fatalf("engine = %+v", e)
	}
	if e.Endpoint != path {
		t.Fatalf("endpoint = %q, want %q", e.Endpoint, path)
	}
}

func TestListContainersMappingAndInspectCache(t *testing.T) {
	fake, a, _ := newFake(t)
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	list, err := a.ListContainers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("containers = %d", len(list))
	}
	var web, db model.Container
	for _, c := range list {
		switch c.ID {
		case "web1":
			web = c
		case "db1":
			db = c
		}
	}
	if web.Name != "web" || web.State != "running" || web.MainPID != 4821 || web.Health != "healthy" || web.RestartCount != 3 {
		t.Fatalf("web = %+v", web)
	}
	if web.ImageID != "abc123" || web.RestartPolicy != "unless-stopped" {
		t.Fatalf("web image/policy = %+v", web)
	}
	if len(web.Ports) != 1 || web.Ports[0].ContainerPort != 80 || web.Ports[0].HostPort != 8080 {
		t.Fatalf("web ports = %+v", web.Ports)
	}
	if len(web.Networks) != 1 || web.Networks[0].NetworkID != "net1" || web.Networks[0].IPAddress != "172.17.0.2" {
		t.Fatalf("web networks = %+v", web.Networks)
	}
	if len(web.Mounts) != 1 || web.Mounts[0].VolumeName != "www" {
		t.Fatalf("web mounts = %+v", web.Mounts)
	}
	if db.State != "exited" || db.ExitCode != 1 || db.MainPID != 0 {
		t.Fatalf("db = %+v", db)
	}

	fake.mu.Lock()
	calls := fake.inspectCalls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("inspect calls = %d, want 1", calls)
	}
	// Same list state within TTL reuses inspect data.
	if _, err := a.ListContainers(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	calls = fake.inspectCalls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("inspect calls after cached list = %d, want 1", calls)
	}
	// A state change invalidates the cache.
	fake.mu.Lock()
	fake.state = "exited"
	fake.mu.Unlock()
	if _, err := a.ListContainers(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	calls = fake.inspectCalls
	fake.mu.Unlock()
	if calls != 2 {
		t.Fatalf("inspect calls after state change = %d, want 2", calls)
	}
}

func TestPartialInspectFailure(t *testing.T) {
	fake, a, _ := newFake(t)
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.inspectFail = "boom"
	fake.mu.Unlock()
	list, err := a.ListContainers(context.Background())
	if err == nil {
		t.Fatal("expected an error for the failed inspect")
	}
	ids := map[string]bool{}
	for _, c := range list {
		ids[c.ID] = true
	}
	if !ids["db1"] || ids["web1"] {
		t.Fatalf("partial list = %+v", ids)
	}
}

func TestStats(t *testing.T) {
	_, a, _ := newFake(t)
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err := a.Stats(context.Background(), []string{"web1"})
	if err != nil {
		t.Fatal(err)
	}
	s := stats["web1"]
	if s.CPUUsageNs != 1e9 || s.MemoryUsage != 900 || s.MemoryLimit != 2000 {
		t.Fatalf("stats = %+v", s)
	}
	if s.NetworkRxBytes != 15 || s.NetworkTxBytes != 26 {
		t.Fatalf("network = %+v", s)
	}
	if s.BlockReadBytes != 30 || s.BlockWriteBytes != 40 || s.Pids != 5 {
		t.Fatalf("blkio/pids = %+v", s)
	}
}

func TestImagesNetworksVolumes(t *testing.T) {
	_, a, _ := newFake(t)
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	images, err := a.ListImages(context.Background())
	if err != nil || len(images) != 1 || images[0].ID != "abc123" || images[0].CreatedAt.IsZero() {
		t.Fatalf("images = %+v, %v", images, err)
	}
	networks, err := a.ListNetworks(context.Background())
	if err != nil || len(networks) != 1 || networks[0].BridgeInterface != "docker0" {
		t.Fatalf("networks = %+v, %v", networks, err)
	}
	volumes, err := a.ListVolumes(context.Background())
	if err != nil || len(volumes) != 1 || volumes[0].Name != "www" {
		t.Fatalf("volumes = %+v, %v", volumes, err)
	}
	if _, err := a.ListPods(context.Background()); !errors.Is(err, model.ErrNotSupported) {
		t.Fatalf("pods err = %v", err)
	}
}

func TestEvents(t *testing.T) {
	_, a, _ := newFake(t)
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, errs := a.Events(ctx, time.Now().Add(-time.Minute))
	var got []model.RuntimeEvent
	for ev := range ch {
		got = append(got, ev)
	}
	if len(got) != 2 {
		t.Fatalf("events = %+v", got)
	}
	if got[0].Action != "die" || got[0].ActorID != "web1" || got[0].ExitCode == nil || *got[0].ExitCode != 137 {
		t.Fatalf("die event = %+v", got[0])
	}
	if got[1].Action != "health_status" || got[1].Health != "unhealthy" {
		t.Fatalf("health event = %+v", got[1])
	}
	for range errs {
	}
}
