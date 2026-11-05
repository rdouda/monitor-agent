package process

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// userHZ is the kernel's USER_HZ on Linux. It is fixed by the kernel ABI for
// /proc/[pid]/stat.
const userHZ = 100

// pfKThread is PF_KTHREAD from linux/sched.h.
const pfKThread = 0x00200000

type procStat struct {
	PID        int
	Comm       string
	State      byte
	PPID       int
	Flags      uint64
	UTime      uint64
	STime      uint64
	NumThreads uint64
	StartTime  uint64
	VSize      uint64
	RSS        uint64
}

// parseStat parses /proc/[pid]/stat. The command name may contain spaces and
// parentheses, so the fields are split after the last ')'.
func parseStat(data []byte) (procStat, error) {
	first := bytes.IndexByte(data, ' ')
	if first <= 0 {
		return procStat{}, errors.New("stat: missing pid")
	}
	pid, err := strconv.Atoi(string(data[:first]))
	if err != nil || pid <= 0 {
		return procStat{}, fmt.Errorf("stat: invalid pid %q", data[:first])
	}
	open := bytes.IndexByte(data[first:], '(')
	if open < 0 {
		return procStat{}, errors.New("stat: missing command name")
	}
	open += first
	closeIdx := bytes.LastIndexByte(data, ')')
	if closeIdx <= open {
		return procStat{}, errors.New("stat: unterminated command name")
	}
	comm := string(data[open+1 : closeIdx])

	fields := strings.Fields(string(data[closeIdx+1:]))
	// state, ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt,
	// majflt, cmajflt, utime, stime, cutime, cstime, priority, nice,
	// num_threads, itrealvalue, starttime, vsize, rss
	if len(fields) < 22 {
		return procStat{}, fmt.Errorf("stat: only %d fields after command name", len(fields))
	}
	var st procStat
	st.PID = pid
	st.Comm = comm
	if len(fields[0]) != 1 {
		return procStat{}, fmt.Errorf("stat: invalid state %q", fields[0])
	}
	st.State = fields[0][0]

	if st.PPID, err = parseIntField(fields[1], "ppid"); err != nil {
		return procStat{}, err
	}
	if st.Flags, err = parseUintField(fields[6], "flags"); err != nil {
		return procStat{}, err
	}
	if st.UTime, err = parseUintField(fields[11], "utime"); err != nil {
		return procStat{}, err
	}
	if st.STime, err = parseUintField(fields[12], "stime"); err != nil {
		return procStat{}, err
	}
	if st.NumThreads, err = parseUintField(fields[17], "num_threads"); err != nil {
		return procStat{}, err
	}
	if st.StartTime, err = parseUintField(fields[19], "starttime"); err != nil {
		return procStat{}, err
	}
	if st.VSize, err = parseUintField(fields[20], "vsize"); err != nil {
		return procStat{}, err
	}
	rss, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil {
		return procStat{}, fmt.Errorf("stat: invalid rss %q", fields[21])
	}
	if rss > 0 {
		st.RSS = uint64(rss)
	}
	return st, nil
}

func parseIntField(s, name string) (int, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("stat: invalid %s %q", name, s)
	}
	return int(v), nil
}

func parseUintField(s, name string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("stat: invalid %s %q", name, s)
	}
	return v, nil
}

func readStat(procfsRoot string, pid int) (procStat, error) {
	p := filepath.Join(procfsRoot, strconv.Itoa(pid), "stat")
	data, err := os.ReadFile(p)
	if err != nil {
		return procStat{}, fmt.Errorf("pid %d stat: %w", pid, err)
	}
	return parseStat(data)
}

// listPIDs enumerates numeric process directories, sorted ascending.
func listPIDs(procfsRoot string) ([]int, error) {
	entries, err := os.ReadDir(procfsRoot)
	if err != nil {
		return nil, fmt.Errorf("procfs: %w", err)
	}
	pids := make([]int, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids, nil
}

// skipProcessError reports whether err is the normal race with a process that
// exited while it was being read.
func skipProcessError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// procStatus is the subset of /proc/[pid]/status the agent reports.
type procStatus struct {
	UID        int
	HasUID     bool
	GID        int
	HasGID     bool
	VmSize     uint64
	HasVmSize  bool
	VmRSS      uint64
	HasVmRSS   bool
	Threads    int
	HasThreads bool
}

func readStatus(procfsRoot string, pid int) (procStatus, error) {
	p := filepath.Join(procfsRoot, strconv.Itoa(pid), "status")
	f, err := os.Open(p)
	if err != nil {
		return procStatus{}, fmt.Errorf("pid %d status: %w", pid, err)
	}
	defer f.Close()
	st, err := parseStatus(f)
	if err != nil {
		return st, fmt.Errorf("pid %d status: %w", pid, err)
	}
	return st, nil
}

func parseStatus(r io.Reader) (procStatus, error) {
	var out procStatus
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := s.Text()
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		switch key {
		case "Uid":
			if v, err := strconv.Atoi(fields[0]); err == nil {
				out.UID = v
				out.HasUID = true
			}
		case "Gid":
			if v, err := strconv.Atoi(fields[0]); err == nil {
				out.GID = v
				out.HasGID = true
			}
		case "VmSize":
			if v, err := parseKB(fields[0]); err == nil {
				out.VmSize = v
				out.HasVmSize = true
			}
		case "VmRSS":
			if v, err := parseKB(fields[0]); err == nil {
				out.VmRSS = v
				out.HasVmRSS = true
			}
		case "Threads":
			if v, err := strconv.Atoi(fields[0]); err == nil {
				out.Threads = v
				out.HasThreads = true
			}
		}
	}
	if err := s.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func parseKB(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return v * 1024, nil
}

// stateName maps a /proc stat state character to the wire vocabulary.
func stateName(state byte) (string, bool) {
	switch state {
	case 'R':
		return "running", true
	case 'S':
		return "sleeping", true
	case 'D':
		return "disk_sleep", true
	case 'T', 't':
		return "stopped", true
	case 'Z':
		return "zombie", true
	case 'I':
		return "idle", true
	default:
		return "", false
	}
}

// nameAllowed applies the include/exclude globs from the configuration.
func nameAllowed(name string, include, exclude []string) bool {
	if len(include) > 0 {
		matched := false
		for _, glob := range include {
			if ok, _ := path.Match(glob, name); ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, glob := range exclude {
		if ok, _ := path.Match(glob, name); ok {
			return false
		}
	}
	return true
}

func readExecutable(procfsRoot string, pid int) (string, bool) {
	target, err := os.Readlink(filepath.Join(procfsRoot, strconv.Itoa(pid), "exe"))
	if err != nil {
		return "", false
	}
	const deleted = " (deleted)"
	if strings.HasSuffix(target, deleted) {
		return strings.TrimSuffix(target, deleted), true
	}
	return target, false
}

func readCommandLine(procfsRoot string, pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join(procfsRoot, strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return "", err
	}
	parts := strings.Split(string(data), "\x00")
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return strings.Join(parts, " "), nil
}

func redactCommandLine(s string, patterns []*regexp.Regexp) string {
	for _, re := range patterns {
		s = re.ReplaceAllString(s, "[REDACTED]")
	}
	return s
}

func readUserNames(hostfsRoot string) map[int]string {
	out := map[int]string{}
	f, err := os.Open(filepath.Join(hostfsRoot, "etc/passwd"))
	if err != nil {
		return out
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Split(s.Text(), ":")
		if len(fields) < 3 {
			continue
		}
		uid, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		if fields[0] != "" {
			out[uid] = fields[0]
		}
	}
	return out
}

func readBootInfo(procfsRoot string) (time.Time, string) {
	var bootTime time.Time
	if f, err := os.Open(filepath.Join(procfsRoot, "stat")); err == nil {
		s := bufio.NewScanner(f)
		for s.Scan() {
			fields := strings.Fields(s.Text())
			if len(fields) == 2 && fields[0] == "btime" {
				if sec, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					bootTime = time.Unix(sec, 0).UTC()
				}
				break
			}
		}
		_ = f.Close()
	}
	bootID := ""
	if data, err := os.ReadFile(filepath.Join(procfsRoot, "sys/kernel/random/boot_id")); err == nil {
		bootID = strings.TrimSpace(string(data))
	}
	return bootTime, bootID
}

func readLogicalCores(procfsRoot string) int {
	f, err := os.Open(filepath.Join(procfsRoot, "cpuinfo"))
	if err != nil {
		return 1
	}
	defer f.Close()
	count := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		key, _, ok := strings.Cut(s.Text(), ":")
		if ok && strings.TrimSpace(key) == "processor" {
			count++
		}
	}
	if count < 1 {
		return 1
	}
	return count
}

func readFDCount(procfsRoot string, pid int) (int, bool) {
	entries, err := os.ReadDir(filepath.Join(procfsRoot, strconv.Itoa(pid), "fd"))
	if err != nil {
		return 0, false
	}
	return len(entries), true
}
