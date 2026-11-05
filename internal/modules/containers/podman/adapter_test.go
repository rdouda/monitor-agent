package podman

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent/internal/modules/containers/docker"
	"agent/internal/modules/containers/model"
)

func serveUnix(t *testing.T, handler http.Handler) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "podman.sock")
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

func fakeHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		switch {
		case r.URL.Path == "/version":
			fmt.Fprint(w, `{"Version":"4.9.4","ApiVersion":"4.9.4","Components":[{"Name":"Podman Engine","Version":"4.9.4"}]}`)
		case r.URL.Path == "/libpod/info":
			fmt.Fprint(w, `{"host":{"security":{"rootless":true},"cgroupVersion":"v2"},"store":{"graphDriverName":"overlay"}}`)
		case r.URL.Path == "/libpod/containers/json":
			fmt.Fprint(w, `[{"Id":"c1","Names":["api"],"State":"running"}]`)
		case strings.HasPrefix(r.URL.Path, "/libpod/containers/c1/json"):
			fmt.Fprint(w, `{"Id":"c1","Name":"api","Created":"2026-11-04T19:00:00Z",
			  "State":{"Status":"running","Running":true,"Pid":777,"ExitCode":0,
			    "StartedAt":"2026-11-04T19:00:01Z","FinishedAt":"0001-01-01T00:00:00Z","Health":{"Status":"healthy"}},
			  "Image":"sha256:img1","ImageName":"quay.io/acme/api:1.0","Config":{"Image":"quay.io/acme/api:1.0","Labels":{"io.podman.compose.project":"acme"}},
			  "HostConfig":{"RestartPolicy":{"Name":"always"}},
			  "NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"","HostPort":"18080"}]},
			    "Networks":{"podman":{"NetworkID":"net9","IPAddress":"10.88.0.2","Gateway":"10.88.0.1"}}},
			  "Mounts":[{"Type":"volume","Name":"data","Destination":"/data"}],
			  "Pod":"pod1","RestartCount":2}`)
		case strings.HasPrefix(r.URL.Path, "/libpod/containers/c1/stats"):
			fmt.Fprint(w, `{"ContainerID":"c1","CPUNano":2000000000,"MemUsage":500,"MemLimit":1000,
			  "NetInput":7,"NetOutput":8,"BlockInput":9,"BlockOutput":10,"Pids":4}`)
		case r.URL.Path == "/libpod/images/json":
			fmt.Fprint(w, `[{"Id":"sha256:img1","RepoTags":["quay.io/acme/api:1.0"],"RepoDigests":["quay.io/acme/api@sha256:abc"],"Created":"2026-11-01T00:00:00Z","Size":555}]`)
		case r.URL.Path == "/libpod/networks/json":
			fmt.Fprint(w, `[{"id":"net9","name":"podman","driver":"bridge","network_interface":"podman0","ipv6_enabled":true,"internal":false,"subnets":[{"subnet":"10.88.0.0/16","gateway":"10.88.0.1"}]}]`)
		case r.URL.Path == "/libpod/volumes/json":
			fmt.Fprint(w, `[{"Name":"data","Driver":"local","Mountpoint":"/var/lib/containers/storage/volumes/data/_data","CreatedAt":"2026-11-02T00:00:00Z"}]`)
		case r.URL.Path == "/libpod/pods/json":
			fmt.Fprint(w, `[{"Id":"pod1","Name":"acme","Status":"Running","Created":"2026-11-04T18:00:00Z","InfraId":"infra1","Containers":[{"Id":"c1","Names":["api"]}]}]`)
		case r.URL.Path == "/libpod/events":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"Type":"container","Status":"start","ID":"c1","Name":"api","Time":"2026-11-04T20:00:00.123Z"}`+"\n")
			fmt.Fprint(w, `{"Type":"container","Status":"die","ID":"c1","Attributes":{"exitCode":"0"},"Time":"2026-11-04T20:01:00Z"}`+"\n")
		default:
			http.NotFound(w, r)
		}
	})
}

func TestProbeIdentifiesPodman(t *testing.T) {
	path := serveUnix(t, fakeHandler())
	a := New(path)
	e, err := a.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if e.Runtime != "podman" || e.StorageDriver != "overlay" || e.CgroupVersion != 2 || !e.Rootless {
		t.Fatalf("engine = %+v", e)
	}
	// A Docker adapter must reject a Podman endpoint.
	if _, err := docker.New(path).Probe(context.Background()); !errors.Is(err, model.ErrWrongRuntime) {
		t.Fatalf("docker probe on podman = %v", err)
	}
}

func TestPodmanInventory(t *testing.T) {
	a := New(serveUnix(t, fakeHandler()))
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	list, err := a.ListContainers(context.Background())
	if err != nil || len(list) != 1 {
		t.Fatalf("containers = %+v, %v", list, err)
	}
	c := list[0]
	if c.Name != "api" || c.MainPID != 777 || c.PodID != "pod1" || c.ImageID != "img1" || c.RestartCount != 2 {
		t.Fatalf("container = %+v", c)
	}
	if c.Image != "quay.io/acme/api:1.0" || c.Networks[0].NetworkID != "net9" || c.Mounts[0].VolumeName != "data" {
		t.Fatalf("container details = %+v", c)
	}
	images, err := a.ListImages(context.Background())
	if err != nil || len(images) != 1 || images[0].ID != "img1" || images[0].CreatedAt.IsZero() {
		t.Fatalf("images = %+v, %v", images, err)
	}
	networks, err := a.ListNetworks(context.Background())
	if err != nil || len(networks) != 1 || networks[0].BridgeInterface != "podman0" || !networks[0].IPv6 {
		t.Fatalf("networks = %+v, %v", networks, err)
	}
	volumes, err := a.ListVolumes(context.Background())
	if err != nil || len(volumes) != 1 || volumes[0].Name != "data" {
		t.Fatalf("volumes = %+v, %v", volumes, err)
	}
	pods, err := a.ListPods(context.Background())
	if err != nil || len(pods) != 1 || pods[0].ID != "pod1" || pods[0].State != "running" || len(pods[0].ContainerIDs) != 1 {
		t.Fatalf("pods = %+v, %v", pods, err)
	}
	stats, err := a.Stats(context.Background(), []string{"c1"})
	if err != nil {
		t.Fatal(err)
	}
	s := stats["c1"]
	if s.CPUUsageNs != 2000000000 || s.MemoryUsage != 500 || s.MemoryLimit != 1000 || s.Pids != 4 || s.BlockReadBytes != 9 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestPodmanEvents(t *testing.T) {
	a := New(serveUnix(t, fakeHandler()))
	if _, err := a.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, _ := a.Events(ctx, time.Now().Add(-time.Minute))
	var got []model.RuntimeEvent
	for ev := range ch {
		got = append(got, ev)
	}
	if len(got) != 2 || got[0].Action != "start" || got[1].Action != "die" {
		t.Fatalf("events = %+v", got)
	}
	if got[1].ExitCode == nil || *got[1].ExitCode != 0 {
		t.Fatalf("exit code = %+v", got[1])
	}
}

func TestVersionedLibpodAPINegotiation(t *testing.T) {
	var sawVersioned bool
	path := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			fmt.Fprint(w, `{"Version":"5.8.9","ApiVersion":"1.44","Components":[{"Name":"Podman Engine","Version":"5.8.9","Details":{"APIVersion":"5.8.9"}}]}`)
		case "/v5.8.9/libpod/info":
			fmt.Fprint(w, `{"host":{"security":{"rootless":true},"cgroupVersion":"v2"},"store":{"graphDriverName":"overlay"}}`)
		case "/v5.8.9/libpod/containers/json":
			sawVersioned = true
			fmt.Fprint(w, `[{"Id":"c1","Names":["api"],"State":"exited"}]`)
		case "/v5.8.9/libpod/containers/c1/json":
			fmt.Fprint(w, `{"Id":"c1","Name":"api","State":{"Status":"exited","ExitCode":0},"Image":"sha256:img1","Config":{"Image":"acme/api:1.0"},"HostConfig":{},"Mounts":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	a := New(path)
	e, err := a.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if e.Version != "5.8.9" || e.Rootless != true {
		t.Fatalf("engine = %+v", e)
	}
	list, err := a.ListContainers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !sawVersioned || len(list) != 1 || list[0].ID != "c1" {
		t.Fatalf("versioned list = %+v, saw=%v", list, sawVersioned)
	}
}
