package process

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"agent/internal/config"
)

func TestParseStat(t *testing.T) {
	line := "100 (nginx) S 1 100 100 0 -1 4194560 200 0 0 0 25 10 0 0 20 0 8 0 500 22345678 900 18446744073709551615 1 1 0 0\n"
	st, err := parseStat([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if st.PID != 100 || st.Comm != "nginx" || st.State != 'S' || st.PPID != 1 {
		t.Fatalf("stat: %+v", st)
	}
	if st.Flags != 4194560 || st.UTime != 25 || st.STime != 10 || st.NumThreads != 8 {
		t.Fatalf("stat: %+v", st)
	}
	if st.StartTime != 500 || st.VSize != 22345678 || st.RSS != 900 {
		t.Fatalf("stat: %+v", st)
	}
	if st.Flags&pfKThread != 0 {
		t.Fatal("not a kernel thread")
	}
}

func TestParseStatParenthesesAndSpaces(t *testing.T) {
	line := "42 (a process (with) parens) R 7 1 1 0 -1 0 0 0 0 0 1 2 0 0 20 0 3 0 77 1024 2 0 0\n"
	st, err := parseStat([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if st.PID != 42 || st.Comm != "a process (with) parens" || st.PPID != 7 {
		t.Fatalf("stat: %+v", st)
	}
	if st.UTime != 1 || st.STime != 2 || st.StartTime != 77 || st.RSS != 2 {
		t.Fatalf("stat: %+v", st)
	}
}

func TestParseStatKernelThread(t *testing.T) {
	line := "300 (kworker/0:0) I 2 0 0 0 -1 2097152 0 0 0 0 1 1 0 0 20 0 1 0 800 0 0\n"
	st, err := parseStat([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if st.Flags&pfKThread == 0 {
		t.Fatalf("flags = %d, want PF_KTHREAD", st.Flags)
	}
}

func TestParseStatInvalid(t *testing.T) {
	cases := []string{
		"",
		"1",
		"1 systemd",
		"1 (systemd",
		"1 (systemd) S 0",
		"1 (systemd) SS 0 1 1 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 1 1 1",
		"1 (systemd) S 0 1 1 0 -1 nope 0 0 0 0 1 1 0 0 20 0 1 0 1 1 1",
	}
	for _, in := range cases {
		if _, err := parseStat([]byte(in)); err == nil {
			t.Errorf("parseStat(%q) succeeded, want error", in)
		}
	}
}

func FuzzParseStat(f *testing.F) {
	f.Add([]byte("1 (systemd) S 0 1 1 0 -1 4194560 100 0 0 0 10 5 0 0 20 0 4 0 100 12345678 678\n"))
	f.Add([]byte("42 (a process (with) parens) R 7 1 1 0 -1 0 0 0 0 0 1 2 0 0 20 0 3 0 77 1024 2 0 0\n"))
	f.Add([]byte("2 (x)y) S 1 2 2 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 9 10 11 0 0\n"))
	f.Add([]byte("300 (kworker/0:0) I 2 0 0 0 -1 2097152 0 0 0 0 1 1 0 0 20 0 1 0 800 0 0\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseStat(data)
	})
}

func TestParseStatus(t *testing.T) {
	in := "Name:\tnginx\nUid:\t33\t33\t33\t33\nGid:\t44\t44\t44\t44\n" +
		"VmSize:\t  22345 kB\nVmRSS:\t   9000 kB\nThreads:\t8\n"
	st, err := parseStatus(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if !st.HasUID || st.UID != 33 || !st.HasGID || st.GID != 44 {
		t.Fatalf("ids: %+v", st)
	}
	if !st.HasVmSize || st.VmSize != 22345*1024 || !st.HasVmRSS || st.VmRSS != 9000*1024 {
		t.Fatalf("memory: %+v", st)
	}
	if !st.HasThreads || st.Threads != 8 {
		t.Fatalf("threads: %+v", st)
	}
}

func TestParseStatusMissingFields(t *testing.T) {
	st, err := parseStatus(strings.NewReader("Name:\tx\n"))
	if err != nil {
		t.Fatal(err)
	}
	if st.HasUID || st.HasGID || st.HasVmSize || st.HasVmRSS || st.HasThreads {
		t.Fatalf("unexpected fields: %+v", st)
	}
}

func TestStateName(t *testing.T) {
	cases := map[byte]string{
		'R': "running",
		'S': "sleeping",
		'D': "disk_sleep",
		'T': "stopped",
		't': "stopped",
		'Z': "zombie",
		'I': "idle",
	}
	for state, want := range cases {
		got, ok := stateName(state)
		if !ok || got != want {
			t.Errorf("stateName(%q) = %q, %v; want %q", state, got, ok, want)
		}
	}
	if _, ok := stateName('X'); ok {
		t.Fatal("unknown state must be omitted")
	}
}

func TestNameAllowed(t *testing.T) {
	cases := []struct {
		name    string
		include []string
		exclude []string
		want    bool
	}{
		{"nginx", nil, nil, true},
		{"nginx", []string{"ngin*"}, nil, true},
		{"nginx", []string{"postgres*"}, nil, false},
		{"nginx", nil, []string{"nginx"}, false},
		{"nginx", []string{"nginx"}, []string{"nginx"}, false},
	}
	for _, c := range cases {
		if got := nameAllowed(c.name, c.include, c.exclude); got != c.want {
			t.Errorf("nameAllowed(%q, %v, %v) = %v, want %v", c.name, c.include, c.exclude, got, c.want)
		}
	}
}

func TestReadExecutableDeleted(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "400")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/usr/bin/deleted-proc (deleted)", filepath.Join(dir, "exe")); err != nil {
		t.Fatal(err)
	}
	exe, deleted := readExecutable(root, 400)
	if exe != "/usr/bin/deleted-proc" || !deleted {
		t.Fatalf("exe=%q deleted=%v", exe, deleted)
	}
}

func compileRedact(t *testing.T) []*regexp.Regexp {
	t.Helper()
	var out []*regexp.Regexp
	for _, pat := range config.Default().Modules.Process.RedactPatterns {
		re, err := regexp.Compile(pat)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, re)
	}
	return out
}

func TestRedactCommandLine(t *testing.T) {
	patterns := compileRedact(t)
	in := "app --password=hunter2 --token abc --keep=ok"
	got := redactCommandLine(in, patterns)
	if strings.Contains(got, "hunter2") || strings.Contains(got, "abc") {
		t.Fatalf("redaction failed: %q", got)
	}
	if !strings.Contains(got, "--keep=ok") {
		t.Fatalf("non-secret argument removed: %q", got)
	}
}
