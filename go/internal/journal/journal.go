package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"enragent/internal/protocol"
	"enragent/internal/state"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Kind      string              `json:"kind"`
	At        time.Time           `json:"at"`
	Spec      *protocol.RunSpec   `json:"spec,omitempty"`
	RunID     string              `json:"run_id,omitempty"`
	AttemptID string              `json:"attempt_id,omitempty"`
	From      state.Phase         `json:"from,omitempty"`
	To        state.Phase         `json:"to,omitempty"`
	Event     *protocol.RunEvent  `json:"event,omitempty"`
	Result    *protocol.RunResult `json:"result,omitempty"`
}

type envelope struct {
	Record json.RawMessage `json:"record"`
	CRC32C uint32          `json:"crc32c"`
}

type Store struct {
	mu     sync.Mutex
	dir    string
	runs   *os.File
	events map[string]*os.File
	seen   map[string]bool
	closed bool
}

type Recovery struct {
	Specs      map[string]protocol.RunSpec
	Phases     map[string]state.Phase
	AttemptIDs map[string]string
	Results    map[string]protocol.RunResult
	Incomplete bool
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o700); err != nil {
		return nil, err
	}
	recovery, err := Replay(dir)
	if err != nil {
		return nil, err
	}
	if recovery.Incomplete {
		return nil, fmt.Errorf("journal in %s has incomplete evidence; inspect before resuming", dir)
	}
	runs, err := os.OpenFile(filepath.Join(dir, "runs.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	store := &Store{dir: dir, runs: runs, events: map[string]*os.File{}, seen: map[string]bool{}}
	entries, err := os.ReadDir(filepath.Join(dir, "events"))
	if err != nil {
		runs.Close()
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		records, incomplete, err := readRecords(filepath.Join(dir, "events", entry.Name()))
		if err != nil || incomplete {
			runs.Close()
			return nil, fmt.Errorf("event journal %s is corrupt or incomplete: %v", entry.Name(), err)
		}
		for _, record := range records {
			if record.Event != nil {
				store.seen[record.Event.EventID] = true
			}
		}
	}
	return store, nil
}

func (s *Store) AppendSpec(spec protocol.RunSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendRun(Record{Kind: "spec", At: time.Now().UTC(), Spec: &spec, RunID: spec.RunID})
}

func (s *Store) AppendTransition(runID, attemptID string, from, to state.Phase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendRun(Record{Kind: "transition", At: time.Now().UTC(), RunID: runID, AttemptID: attemptID, From: from, To: to})
}

func (s *Store) AppendEvent(event protocol.RunEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[event.EventID] {
		return nil
	}
	file, err := s.eventFile(event.RunID)
	if err != nil {
		return err
	}
	if err := appendRecord(file, Record{Kind: "event", At: time.Now().UTC(), RunID: event.RunID, AttemptID: event.AttemptID, Event: &event}); err != nil {
		return err
	}
	s.seen[event.EventID] = true
	return nil
}

func (s *Store) AppendResult(result protocol.RunResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := s.eventFile(result.RunID)
	if err != nil {
		return err
	}
	return appendRecord(file, Record{Kind: "result", At: time.Now().UTC(), RunID: result.RunID, AttemptID: result.AttemptID, Result: &result})
}

func (s *Store) appendRun(record Record) error {
	if s.closed {
		return fmt.Errorf("journal is closed")
	}
	return appendRecord(s.runs, record)
}

func (s *Store) eventFile(runID string) (*os.File, error) {
	if s.closed {
		return nil, fmt.Errorf("journal is closed")
	}
	if !strings.HasPrefix(runID, "run_") || strings.ContainsAny(runID, `/\\.`) {
		return nil, fmt.Errorf("invalid run ID %q", runID)
	}
	if file := s.events[runID]; file != nil {
		return file, nil
	}
	file, err := os.OpenFile(filepath.Join(s.dir, "events", runID+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	s.events[runID] = file
	return file, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var first error
	if err := s.runs.Close(); err != nil {
		first = err
	}
	for _, file := range s.events {
		if err := file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func appendRecord(file *os.File, record Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line, err := json.Marshal(envelope{Record: data, CRC32C: crc32.Checksum(data, crcTable)})
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := file.Write(line); err != nil {
		return err
	}
	return file.Sync()
}

func Replay(dir string) (Recovery, error) {
	recovery := Recovery{Specs: map[string]protocol.RunSpec{}, Phases: map[string]state.Phase{}, AttemptIDs: map[string]string{}, Results: map[string]protocol.RunResult{}}
	path := filepath.Join(dir, "runs.jsonl")
	records, incomplete, err := readRecords(path)
	if os.IsNotExist(err) {
		records, incomplete, err = nil, false, nil
	}
	if err != nil {
		return recovery, err
	}
	recovery.Incomplete = incomplete
	for _, record := range records {
		switch record.Kind {
		case "spec":
			if record.Spec == nil {
				recovery.Incomplete = true
				continue
			}
			if _, exists := recovery.Specs[record.RunID]; exists {
				recovery.Incomplete = true
				continue
			}
			recovery.Specs[record.RunID] = *record.Spec
			recovery.Phases[record.RunID] = state.Planned
		case "transition":
			if _, ok := recovery.Specs[record.RunID]; !ok || recovery.Phases[record.RunID] != record.From {
				recovery.Incomplete = true
				continue
			}
			recovery.Phases[record.RunID] = record.To
			recovery.AttemptIDs[record.RunID] = record.AttemptID
		default:
			recovery.Incomplete = true
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "events"))
	if os.IsNotExist(err) {
		return recovery, nil
	}
	if err != nil {
		return recovery, err
	}
	seenEvents := map[string]bool{}
	sequences := map[string]uint64{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		runID := strings.TrimSuffix(entry.Name(), ".jsonl")
		if _, ok := recovery.Specs[runID]; !ok {
			recovery.Incomplete = true
			continue
		}
		eventRecords, eventIncomplete, err := readRecords(filepath.Join(dir, "events", entry.Name()))
		if err != nil {
			return recovery, err
		}
		recovery.Incomplete = recovery.Incomplete || eventIncomplete
		for _, record := range eventRecords {
			if record.RunID != runID {
				recovery.Incomplete = true
				continue
			}
			switch record.Kind {
			case "event":
				if record.Event == nil || record.Event.EventID == "" || seenEvents[record.Event.EventID] {
					recovery.Incomplete = true
					continue
				}
				if record.Event.RunID != runID || record.Event.ExperimentID != recovery.Specs[runID].ExperimentID || record.Event.SchemaVersion != protocol.SchemaVersion || record.Event.Producer == "" {
					recovery.Incomplete = true
					continue
				}
				sequenceKey := runID + "/" + record.Event.AttemptID + "/" + record.Event.Producer
				if record.Event.ProducerSequence != sequences[sequenceKey]+1 {
					recovery.Incomplete = true
				}
				sequences[sequenceKey] = record.Event.ProducerSequence
				seenEvents[record.Event.EventID] = true
			case "result":
				if record.Result == nil {
					recovery.Incomplete = true
					continue
				}
				if record.Result.RunID != runID || record.Result.AttemptID != record.AttemptID || record.Result.SchemaVersion != protocol.SchemaVersion {
					recovery.Incomplete = true
					continue
				}
				if _, exists := recovery.Results[runID]; exists {
					recovery.Incomplete = true
					continue
				}
				recovery.Results[runID] = *record.Result
			default:
				recovery.Incomplete = true
			}
		}
	}
	for runID, phase := range recovery.Phases {
		if phase == state.Terminal {
			if _, ok := recovery.Results[runID]; !ok {
				recovery.Incomplete = true
			}
		} else if phase != state.Running {
			if _, ok := recovery.Results[runID]; ok {
				recovery.Incomplete = true
			}
		}
	}
	return recovery, nil
}

func readRecords(path string) ([]Record, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var records []Record
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return records, len(line) != 0, nil
		}
		if err != nil {
			return records, true, err
		}
		var wrapped envelope
		if err := json.Unmarshal(bytes.TrimSpace(line), &wrapped); err != nil {
			return records, true, nil
		}
		if crc32.Checksum(wrapped.Record, crcTable) != wrapped.CRC32C {
			return records, true, nil
		}
		var record Record
		if err := json.Unmarshal(wrapped.Record, &record); err != nil {
			return records, true, nil
		}
		records = append(records, record)
	}
}
