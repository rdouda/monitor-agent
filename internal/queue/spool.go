// Package queue implements the bounded, priority-aware disk spool.
package queue

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent/internal/events"
)

const (
	classP0 = "p0"
	classP1 = "p1"
	classP2 = "p2"

	cursorFile   = "cursor"
	segmentGlob  = "seg-"
	groupCommit  = 1 << 20 // force a flush when 1 MiB is buffered
	readOverhead = 64      // JSON array separators and framing
)

// Options configures a Spool.
type Options struct {
	Directory     string
	MaxBytes      int64
	SegmentBytes  int64
	FlushInterval time.Duration
	Logger        *slog.Logger
	Clock         func() time.Time
}

// Spool is a disk queue with three FIFO priority classes.
type Spool struct {
	dir           string
	maxBytes      int64
	segmentBytes  int64
	flushInterval time.Duration
	log           *slog.Logger
	clock         func() time.Time

	mu       sync.Mutex
	classes  [3]*spoolClass
	pending  int64
	disk     int64
	dropped  [3]int64
	rejected int64
	rateLim  int64

	appended int64
	resolved int64

	stop   chan struct{}
	done   chan struct{}
	closed bool
}

type spoolClass struct {
	prio       events.Priority
	dir        string
	cursorPath string
	cursor     uint64
	segments   []segment
	pending    []pendingEvent
	pendingSz  int64

	current      *os.File
	currentPath  string
	currentSize  int64
	currentFirst uint64
	currentLast  uint64
	currentCount int
}

type pendingEvent struct {
	seq uint64
	raw []byte
}

type segment struct {
	path    string
	first   uint64
	last    uint64
	count   int
	size    int64
	modTime time.Time
}

// QueuedEvent is one event read from the spool.
type QueuedEvent struct {
	Raw      []byte
	Envelope events.Envelope
	Priority events.Priority
	Seq      uint64
}

// Receipt resolves one event. Rejected marks a non-retryable rejection.
type Receipt struct {
	Priority events.Priority
	Seq      uint64
	Rejected bool
}

// Open prepares the spool, replays cursors and starts the flush goroutine.
func Open(opts Options) (*Spool, error) {
	if opts.Directory == "" {
		return nil, errors.New("spool: directory is required")
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 100 << 20
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 4 << 20
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	s := &Spool{
		dir:           opts.Directory,
		maxBytes:      opts.MaxBytes,
		segmentBytes:  opts.SegmentBytes,
		flushInterval: opts.FlushInterval,
		log:           opts.Logger,
		clock:         opts.Clock,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	for i, name := range []string{classP0, classP1, classP2} {
		dir := filepath.Join(opts.Directory, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create spool dir: %w", err)
		}
		c := &spoolClass{
			prio:       events.Priority(i),
			dir:        dir,
			cursorPath: filepath.Join(dir, cursorFile),
		}
		if err := s.loadClass(c); err != nil {
			return nil, err
		}
		s.classes[i] = c
	}
	go s.flushLoop(s.stop)
	return s, nil
}

func (s *Spool) loadClass(c *spoolClass) error {
	if b, err := os.ReadFile(c.cursorPath); err == nil {
		n, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", c.cursorPath, perr)
		}
		c.cursor = n
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), segmentGlob) {
			continue
		}
		seg, err := scanSegment(filepath.Join(c.dir, e.Name()))
		if err != nil {
			s.log.Warn("discarding unreadable spool segment", "path", e.Name(), "error", err)
			_ = os.Remove(filepath.Join(c.dir, e.Name()))
			continue
		}
		c.segments = append(c.segments, seg)
		s.appended += int64(seg.count)
		s.resolved += int64(countResolved(seg, c.cursor))
		s.disk += seg.size
	}
	sort.Slice(c.segments, func(i, j int) bool { return c.segments[i].first < c.segments[j].first })

	kept := c.segments[:0]
	for _, seg := range c.segments {
		if seg.last <= c.cursor && c.cursor > 0 {
			s.disk -= seg.size
			_ = os.Remove(seg.path)
			continue
		}
		kept = append(kept, seg)
	}
	c.segments = kept
	return nil
}

func scanSegment(path string) (segment, error) {
	info, err := os.Stat(path)
	if err != nil {
		return segment{}, err
	}
	base := filepath.Base(path)
	name := strings.TrimSuffix(strings.TrimPrefix(base, segmentGlob), ".jsonl")
	first, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return segment{}, fmt.Errorf("bad segment name %s", base)
	}
	seg := segment{path: path, first: first, size: info.Size(), modTime: info.ModTime()}
	f, err := os.Open(path)
	if err != nil {
		return segment{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8<<20)
	for sc.Scan() {
		var head struct {
			AgentSeq uint64 `json:"agentseq"`
		}
		if err := json.Unmarshal(sc.Bytes(), &head); err != nil {
			continue
		}
		seg.count++
		if head.AgentSeq > seg.last {
			seg.last = head.AgentSeq
		}
	}
	if err := sc.Err(); err != nil {
		return segment{}, err
	}
	if seg.last == 0 {
		return segment{}, errors.New("empty segment")
	}
	return seg, nil
}

func countResolved(seg segment, cursor uint64) int {
	if cursor == 0 {
		return 0
	}
	if seg.last <= cursor {
		return seg.count
	}
	if seg.first > cursor {
		return 0
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return 0
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8<<20)
	for sc.Scan() {
		var head struct {
			AgentSeq uint64 `json:"agentseq"`
		}
		if json.Unmarshal(sc.Bytes(), &head) == nil && head.AgentSeq <= cursor {
			n++
		}
	}
	return n
}

// Send writes one event to the spool. Heartbeats are rejected because they
// must never be spooled.
func (s *Spool) Send(env *events.Envelope) error {
	prio := events.PriorityOf(env.Type)
	if prio == events.PriorityNone {
		return errors.New("heartbeats must not be written to the spool")
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	line := append(raw, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("spool is closed")
	}
	c := s.classes[int(prio)]
	c.pending = append(c.pending, pendingEvent{seq: env.AgentSeq, raw: line})
	c.pendingSz += int64(len(line))
	s.pending += int64(len(line))
	s.appended++

	if s.pending >= groupCommit {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	s.enforceLimitLocked()
	return nil
}

func (s *Spool) flushLoop(stop <-chan struct{}) {
	defer close(s.done)
	t := time.NewTicker(s.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.mu.Lock()
			err := s.flushLocked()
			s.mu.Unlock()
			if err != nil {
				s.log.Error("spool flush failed", "error", err)
			}
		}
	}
}

// Flush writes buffered events and fsyncs all open segments.
func (s *Spool) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *Spool) flushLocked() error {
	var errs []error
	for _, c := range s.classes {
		if len(c.pending) == 0 && c.current == nil {
			continue
		}
		if err := s.flushClassLocked(c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Spool) flushClassLocked(c *spoolClass) error {
	for _, pe := range c.pending {
		if c.current == nil || (c.currentSize > 0 && c.currentSize+int64(len(pe.raw)) > s.segmentBytes) {
			if err := s.rotateLocked(c, pe.seq); err != nil {
				return err
			}
		}
		if _, err := c.current.Write(pe.raw); err != nil {
			return fmt.Errorf("spool write: %w", err)
		}
		c.currentSize += int64(len(pe.raw))
		c.currentCount++
		c.currentLast = pe.seq
		c.pendingSz -= int64(len(pe.raw))
		s.pending -= int64(len(pe.raw))
		s.disk += int64(len(pe.raw))
		s.refreshCurrentLocked(c)
	}
	c.pending = c.pending[:0]
	if c.current != nil {
		if err := c.current.Sync(); err != nil {
			return fmt.Errorf("spool fsync: %w", err)
		}
		s.refreshCurrentLocked(c)
	}
	return nil
}

// refreshCurrentLocked keeps the metadata of the open segment in sync with
// what has actually been written. Eviction and restart accounting rely on
// this metadata being exact for every segment, not only the last one.
func (s *Spool) refreshCurrentLocked(c *spoolClass) {
	if c.current == nil {
		return
	}
	for i := len(c.segments) - 1; i >= 0; i-- {
		if c.segments[i].path != c.currentPath {
			continue
		}
		c.segments[i].last = c.currentLast
		c.segments[i].count = c.currentCount
		c.segments[i].size = c.currentSize
		c.segments[i].modTime = s.clock()
		return
	}
}

func (s *Spool) rotateLocked(c *spoolClass, firstSeq uint64) error {
	if c.current != nil {
		if err := c.current.Sync(); err != nil {
			return err
		}
		if err := c.current.Close(); err != nil {
			return err
		}
		c.current = nil
	}
	name := fmt.Sprintf("%s%020d.jsonl", segmentGlob, firstSeq)
	path := filepath.Join(c.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	c.current = f
	c.currentPath = path
	c.currentSize = 0
	c.currentFirst = firstSeq
	c.currentLast = firstSeq
	c.currentCount = 0
	c.segments = append(c.segments, segment{
		path: path, first: firstSeq, last: firstSeq, count: 0, modTime: s.clock(),
	})
	return nil
}

// ReadBatch returns up to maxEvents / maxBytes events, draining P0 then P1
// then P2. It does not advance cursors.
func (s *Spool) ReadBatch(maxEvents, maxBytes int) ([]QueuedEvent, error) {
	if maxEvents < 1 {
		maxEvents = 1
	}
	if maxBytes < 1024 {
		maxBytes = 1024
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make events that were just appended visible to the reader instead of
	// waiting for the next group commit.
	if err := s.flushLocked(); err != nil {
		return nil, err
	}

	var out []QueuedEvent
	bytes := 0
	for _, c := range s.classes {
		for _, seg := range c.segments {
			if seg.last <= c.cursor {
				continue
			}
			eventsFromSeg, err := readSegmentEvents(seg, c.cursor)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return nil, err
			}
			for _, qe := range eventsFromSeg {
				sz := len(qe.Raw) + readOverhead
				if len(out) > 0 && (len(out) >= maxEvents || bytes+sz > maxBytes) {
					return out, nil
				}
				qe.Priority = c.prio
				out = append(out, qe)
				bytes += sz
			}
		}
	}
	return out, nil
}

func readSegmentEvents(seg segment, cursor uint64) ([]QueuedEvent, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []QueuedEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		var env events.Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			continue
		}
		if env.AgentSeq <= cursor {
			continue
		}
		out = append(out, QueuedEvent{
			Raw:      append([]byte(nil), line...),
			Envelope: env,
			Seq:      env.AgentSeq,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Resolve acknowledges events in order. A non-retryable rejected event is
// resolved and counted separately.
func (s *Spool) Resolve(receipts []Receipt) error {
	if len(receipts) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	maxSeq := map[events.Priority]uint64{}
	rejected := 0
	for _, r := range receipts {
		if r.Priority == events.PriorityNone || int(r.Priority) < 0 || int(r.Priority) >= len(s.classes) {
			continue
		}
		if r.Seq <= s.classes[int(r.Priority)].cursor {
			// Already acknowledged; never count the same event twice.
			continue
		}
		if r.Seq > maxSeq[r.Priority] {
			maxSeq[r.Priority] = r.Seq
		}
		s.resolved++
		if r.Rejected {
			rejected++
		}
	}
	s.rejected += int64(rejected)

	var errs []error
	for prio, seq := range maxSeq {
		c := s.classes[int(prio)]
		if seq <= c.cursor {
			continue
		}
		c.cursor = seq
		if err := atomicWrite(c.cursorPath, []byte(fmt.Sprintf("%d\n", seq))); err != nil {
			errs = append(errs, err)
		}
		kept := c.segments[:0]
		for _, seg := range c.segments {
			if seg.last <= seq {
				s.disk -= seg.size
				_ = os.Remove(seg.path)
				if c.currentPath == seg.path {
					_ = c.current.Close()
					c.current = nil
					c.currentPath = ""
					c.currentSize = 0
				}
				continue
			}
			kept = append(kept, seg)
		}
		c.segments = kept
	}
	return errors.Join(errs...)
}

// RecordRateLimited increments the counter reported in the heartbeat.
func (s *Spool) RecordRateLimited(n int64) {
	s.mu.Lock()
	s.rateLim += n
	s.mu.Unlock()
}

// QueueSnapshot implements core.QueueStats.
func (s *Spool) QueueSnapshot() QueueSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	depth := s.appended - s.resolved
	if depth < 0 {
		depth = 0
	}
	q := QueueSnapshot{
		DepthEvents:      depth,
		SizeBytes:        s.disk + s.pending,
		Dropped:          s.dropped,
		RejectedTotal:    s.rejected,
		RateLimitedTotal: s.rateLim,
	}
	oldest := s.oldestSegmentLocked()
	if !oldest.IsZero() {
		age := int64(s.clock().Sub(oldest).Seconds())
		if age < 0 {
			age = 0
		}
		q.OldestEventAgeSeconds = &age
	}
	return q
}

func (s *Spool) oldestSegmentLocked() time.Time {
	var oldest time.Time
	for _, c := range s.classes {
		for _, seg := range c.segments {
			if seg.last <= c.cursor {
				continue
			}
			if oldest.IsZero() || seg.modTime.Before(oldest) {
				oldest = seg.modTime
			}
			break
		}
	}
	return oldest
}

func (s *Spool) enforceLimitLocked() {
	for s.disk+s.pending > s.maxBytes {
		if !s.dropOldestLocked() {
			return
		}
	}
}

func (s *Spool) dropOldestLocked() bool {
	if err := s.flushLocked(); err != nil {
		s.log.Error("flush before eviction failed", "error", err)
	}
	for _, prio := range []int{2, 1, 0} {
		c := s.classes[prio]
		if len(c.segments) == 0 {
			continue
		}
		seg := c.segments[0]
		if c.currentPath == seg.path {
			_ = c.current.Sync()
			_ = c.current.Close()
			c.current = nil
			c.currentPath = ""
			c.currentSize = 0
		}
		unresolved := seg.count - countResolved(seg, c.cursor)
		_ = os.Remove(seg.path)
		c.segments = c.segments[1:]
		s.disk -= seg.size
		s.dropped[prio] += int64(unresolved)
		s.resolved += int64(unresolved)
		s.log.Warn("spool full: dropped oldest segment", "priority", c.prio.String(), "events", unresolved, "path", filepath.Base(seg.path))
		return true
	}
	return false
}

// Close flushes and stops the spool.
func (s *Spool) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stop)
	err := s.flushLocked()
	for _, c := range s.classes {
		if c.current != nil {
			_ = c.current.Close()
			c.current = nil
		}
	}
	s.mu.Unlock()
	<-s.done
	return err
}

// Stats exposes counters for tests and logging.
type Stats struct {
	Appended  int64
	Resolved  int64
	Dropped   [3]int64
	Rejected  int64
	DiskBytes int64
}

// Stats returns a copy of the counters.
func (s *Spool) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Appended: s.appended, Resolved: s.resolved, Dropped: s.dropped, Rejected: s.rejected, DiskBytes: s.disk}
}

func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cursor-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// QueueSnapshot mirrors core.QueueSnapshot without importing core.
type QueueSnapshot struct {
	DepthEvents           int64
	SizeBytes             int64
	OldestEventAgeSeconds *int64
	Dropped               [3]int64
	RejectedTotal         int64
	RateLimitedTotal      int64
}
