package queue

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"agent/internal/events"
)

// TestSpoolKillHelper is re-executed as a child process by
// TestSpoolSurvivesSIGKILL. It writes and flushes events, optionally resolves
// a prefix, and then kills itself without a clean close.
func TestSpoolKillHelper(t *testing.T) {
	if os.Getenv("SPOOL_KILL_HELPER") != "1" {
		t.Skip("helper process only")
	}
	dir := os.Getenv("SPOOL_KILL_DIR")
	s, err := Open(Options{
		Directory:     dir,
		MaxBytes:      1 << 20,
		SegmentBytes:  1 << 20,
		FlushInterval: time.Hour,
		Logger:        testLogger(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := s.Send(makeEvent(t, events.TypeEntityUpsert, i, 10)); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if n := os.Getenv("SPOOL_KILL_RESOLVE"); n != "" {
		seq, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			t.Fatalf("parse resolve: %v", err)
		}
		if err := s.Resolve([]Receipt{{Priority: events.PriorityP1, Seq: seq}}); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func TestSpoolSurvivesSIGKILL(t *testing.T) {
	tests := []struct {
		name    string
		resolve string
		want    []uint64
		depth   int64
	}{
		{name: "queued events", want: []uint64{1, 2, 3, 4, 5}, depth: 5},
		{name: "acknowledged prefix", resolve: "2", want: []uint64{3, 4, 5}, depth: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "spool")
			cmd := exec.Command(os.Args[0], "-test.run=TestSpoolKillHelper", "-test.v")
			cmd.Env = append(os.Environ(),
				"SPOOL_KILL_HELPER=1",
				"SPOOL_KILL_DIR="+dir,
				"SPOOL_KILL_RESOLVE="+tc.resolve,
			)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("helper exited cleanly, expected SIGKILL; output:\n%s", out)
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("helper failed: %v\n%s", err, out)
			}
			ws, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
				t.Fatalf("helper was not killed by SIGKILL: %v\n%s", err, out)
			}

			s, err := Open(Options{
				Directory:     dir,
				MaxBytes:      1 << 20,
				SegmentBytes:  1 << 20,
				FlushInterval: time.Hour,
				Logger:        testLogger(),
			})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer s.Close()
			if got := s.QueueSnapshot().DepthEvents; got != tc.depth {
				t.Fatalf("depth after kill = %d, want %d", got, tc.depth)
			}
			batch, err := s.ReadBatch(10, 1<<20)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if got := seqs(batch); !equalSeqs(got, tc.want) {
				t.Fatalf("events after kill = %v, want %v", got, tc.want)
			}
		})
	}
}
