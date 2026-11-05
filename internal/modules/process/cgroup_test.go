package process

import (
	"strings"
	"testing"
)

func TestParseCgroup(t *testing.T) {
	id := strings.Repeat("a", 64)
	cases := []struct {
		name string
		in   string
		path string
		id   string
		rt   string
	}{
		{
			name: "cgroup v2 docker path",
			in:   "0::/docker/" + id + "\n",
			path: "/docker/" + id,
			id:   id,
			rt:   "docker",
		},
		{
			name: "cgroup v1 systemd docker scope",
			in:   "12:pids:/system.slice/docker-" + id + ".scope\n1:name=systemd:/system.slice/docker-" + id + ".scope\n",
			path: "/system.slice/docker-" + id + ".scope",
			id:   id,
			rt:   "docker",
		},
		{
			name: "podman scope",
			in:   "0::/user.slice/user-1000.slice/user@1000.service/user.slice/libpod-" + id + ".scope\n",
			path: "/user.slice/user-1000.slice/user@1000.service/user.slice/libpod-" + id + ".scope",
			id:   id,
			rt:   "podman",
		},
		{
			name: "podman path",
			in:   "0::/libpod/" + id + "\n",
			path: "/libpod/" + id,
			id:   id,
			rt:   "podman",
		},
		{
			name: "containerd",
			in:   "0::/system.slice/cri-containerd-" + id + ".scope\n",
			path: "/system.slice/cri-containerd-" + id + ".scope",
			id:   id,
			rt:   "containerd",
		},
		{
			name: "cri-o",
			in:   "0::/system.slice/crio-" + id + ".scope\n",
			path: "/system.slice/crio-" + id + ".scope",
			id:   id,
			rt:   "cri-o",
		},
		{
			name: "plain service",
			in:   "0::/system.slice/nginx.service\n",
			path: "/system.slice/nginx.service",
		},
		{
			name: "v1 cpu preference",
			in:   "2:cpu:/docker/" + id + "\n1:name=systemd:/system.slice\n",
			path: "/system.slice",
			id:   id,
			rt:   "docker",
		},
		{
			name: "empty",
			in:   "\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseCgroup(c.in)
			if got.Path != c.path || got.ContainerID != c.id || got.RuntimeHint != c.rt {
				t.Fatalf("parseCgroup = %+v, want path=%q id=%q runtime=%q", got, c.path, c.id, c.rt)
			}
		})
	}
}

func TestContainerHintRequiresFullID(t *testing.T) {
	if id, rt := containerHint("/docker/abc"); id != "" || rt != "" {
		t.Fatalf("short id matched: %q %q", id, rt)
	}
}
