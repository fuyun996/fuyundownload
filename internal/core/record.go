package core

import "path/filepath"

// Record retains task identity, options and display history across restarts.
type Record struct {
	Options  Options
	Snapshot Snapshot
}

func (t *Task) Record() Record { o := t.optsSnapshot(); o.Budget = nil; return Record{o, t.Snapshot()} }
func Restore(r Record, b *ConnBudget) *Task {
	r.Options.Budget = b
	if r.Snapshot.Path != "" {
		r.Options.Dir = r.Snapshot.Dir
		r.Options.FileName = filepath.Base(r.Snapshot.Path)
	}
	t := NewTask(r.Options)
	t.id = r.Snapshot.ID
	t.createdAt = r.Snapshot.CreatedAt
	t.completedAt = r.Snapshot.CompletedAt
	t.filePath = r.Snapshot.Path
	t.errMsg = r.Snapshot.Error
	t.state = r.Snapshot.State
	if t.state == StatusDownloading || t.state == StatusProbing || t.state == StatusPausing {
		t.state = StatusPaused
	}
	if st, err := LoadState(StatePath(t.filePath)); err == nil {
		t.cachedState = st
	} else if t.state == StatusCompleted {
		t.info = &ProbeResult{Size: r.Snapshot.TotalSize}
		t.table = NewSingleSegmentTable(r.Snapshot.TotalSize)
		for _, s := range t.table.segments {
			s.Done = s.Size()
			s.complete = true
		}
		t.table.done = r.Snapshot.TotalSize
	}
	return t
}
