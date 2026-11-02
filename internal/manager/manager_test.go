package manager

import (
	"fuyun/internal/core"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryAndShutdown(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	s := DefaultSettings()
	s.DownloadDir = t.TempDir()
	m := New(s)
	m.mu.Lock()
	m.stopping = true
	m.mu.Unlock()
	task := core.NewTask(core.Options{URL: "https://example.com/file", Dir: s.DownloadDir, Priority: 5, RateLimit: 1024})
	id := task.ID()
	m.mu.Lock()
	m.tasks = append(m.tasks, task)
	m.mu.Unlock()
	m.PauseAll()
	m.WaitIdle(time.Second)
	restored := New(s)
	defer func() { restored.PauseAll(); restored.WaitIdle(time.Second) }()
	list := restored.List()
	if len(list) != 1 || list[0].ID != id || list[0].State != core.StatusPaused || list[0].Priority != 5 || list[0].RateLimit != 1024 {
		t.Fatalf("bad restore: %+v", list)
	}
	s.DownloadDir = filepath.Join(t.TempDir(), "new")
	s.MaxGlobalConns = 1
	if err := restored.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}
	if restored.List()[0].Dir == s.DownloadDir {
		t.Fatal("existing task moved")
	}
	if restored.budget.TryAcquire(10) != 1 {
		t.Fatal("budget did not update")
	}
}
