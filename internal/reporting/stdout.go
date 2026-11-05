package reporting

import (
	"encoding/json"
	"io"
	"sync"

	"agent/internal/events"
)

// StdoutSink prints one CloudEvent per line. It is used by `agent once` and
// development runs.
type StdoutSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdoutSink creates a sink writing to w.
func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{w: w}
}

// Send implements core.Sink.
func (s *StdoutSink) Send(env *events.Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.w.Write(b)
	return err
}

// Close implements core.Sink.
func (s *StdoutSink) Close() error { return nil }
