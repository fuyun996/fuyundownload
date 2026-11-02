package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fuyun/internal/core"
)

// Settings 是可持久化的全局设置。
type Settings struct {
	DownloadDir     string            `json:"download_dir"`
	MaxConnsPerTask int               `json:"max_conns_per_task"`
	MaxTasks        int               `json:"max_tasks"`
	MaxGlobalConns  int               `json:"max_global_conns"`
	UserAgent       string            `json:"user_agent"`
	Headers         map[string]string `json:"headers"`
	Insecure        bool              `json:"insecure"`
	Port            int               `json:"port"`
	NoBrowserOpen   bool              `json:"no_browser_open"`
}

func clampInt(n, lo, hi int) int {
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

// DefaultSettings 返回默认设置。
func DefaultSettings() Settings {
	home, err := os.UserHomeDir()
	dl := "."
	if err == nil && home != "" {
		cand := filepath.Join(home, "Downloads")
		if fi, err := os.Stat(cand); err == nil && fi.IsDir() {
			dl = cand
		}
	}
	return Settings{
		DownloadDir:     dl,
		MaxConnsPerTask: 32,
		MaxTasks:        3,
		MaxGlobalConns:  64,
		UserAgent:       "fuyun/" + core.Version,
		Headers:         map[string]string{},
		Port:            8791,
	}
}

// Normalize 校验并修正设置取值范围。
func (s *Settings) Normalize() {
	s.MaxConnsPerTask = clampInt(s.MaxConnsPerTask, 1, 64)
	s.MaxTasks = clampInt(s.MaxTasks, 1, 20)
	s.MaxGlobalConns = clampInt(s.MaxGlobalConns, 1, 512)
	if s.Port < 0 || s.Port > 65535 {
		s.Port = 0
	}
	if s.UserAgent == "" {
		s.UserAgent = "fuyun/" + core.Version
	}
	if s.Headers == nil {
		s.Headers = map[string]string{}
	}
}

// SettingsPath 返回设置文件路径（~/.fuyun/config.json）。
func SettingsPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "fuyun-config.json"
	}
	return filepath.Join(home, ".fuyun", "config.json")
}

// LoadSettings 读取设置文件，缺失字段以默认值补充。
func LoadSettings() Settings {
	s := DefaultSettings()
	data, err := os.ReadFile(SettingsPath())
	if err == nil {
		var loaded Settings
		if json.Unmarshal(data, &loaded) == nil {
			if loaded.DownloadDir != "" {
				s.DownloadDir = loaded.DownloadDir
			}
			if loaded.MaxConnsPerTask > 0 {
				s.MaxConnsPerTask = loaded.MaxConnsPerTask
			}
			if loaded.MaxTasks > 0 {
				s.MaxTasks = loaded.MaxTasks
			}
			if loaded.MaxGlobalConns > 0 {
				s.MaxGlobalConns = loaded.MaxGlobalConns
			}
			if loaded.UserAgent != "" {
				s.UserAgent = loaded.UserAgent
			}
			if loaded.Headers != nil {
				s.Headers = loaded.Headers
			}
			s.Insecure = loaded.Insecure
			s.Port = loaded.Port
			s.NoBrowserOpen = loaded.NoBrowserOpen
		}
	}
	s.Normalize()
	return s
}

// SaveSettings 原子写入设置文件。
func SaveSettings(s Settings) error {
	s.Normalize()
	p := SettingsPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// Manager 管理多个下载任务：排队调度、设置、事件广播。
type Manager struct {
	mu       sync.Mutex
	settings Settings
	tasks    []*core.Task
	budget   *core.ConnBudget

	subs map[chan []byte]struct{}
	kick chan struct{}
}

// New 创建管理器并扫描下载目录中可续传的未完成任务。
func New(settings Settings) *Manager {
	settings.Normalize()
	m := &Manager{
		settings: settings,
		budget:   core.NewConnBudget(settings.MaxGlobalConns),
		subs:     make(map[chan []byte]struct{}),
		kick:     make(chan struct{}, 1),
	}
	m.importResumable()
	go m.broadcastLoop()
	return m
}

// importResumable 扫描下载目录中的 *.fuyun 断点文件，导入为暂停任务。
func (m *Manager) importResumable() {
	entries, err := os.ReadDir(m.settings.DownloadDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".fuyun") {
			continue
		}
		st, err := core.LoadState(filepath.Join(m.settings.DownloadDir, e.Name()))
		if err != nil || st == nil || st.URL == "" {
			continue
		}
		t := core.NewTask(core.Options{
			URL:       st.URL,
			Dir:       m.settings.DownloadDir,
			FileName:  st.FileName,
			MaxConns:  m.settings.MaxConnsPerTask,
			UserAgent: m.settings.UserAgent,
			Headers:   m.settings.Headers,
			Insecure:  m.settings.Insecure,
			Budget:    m.budget,
		})
		t.SetCachedState(st)
		t.MarkPausedIdle()
		m.tasks = append(m.tasks, t)
	}
}

// Settings 返回当前设置副本。
func (m *Manager) Settings() Settings {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.settings
	if s.Headers != nil {
		s.Headers = make(map[string]string, len(m.settings.Headers))
		for k, v := range m.settings.Headers {
			s.Headers[k] = v
		}
	}
	return s
}

// UpdateSettings 更新并持久化设置，同步到现有任务。
func (m *Manager) UpdateSettings(s Settings) error {
	s.Normalize()
	if _, err := os.Stat(s.DownloadDir); err != nil {
		if err := os.MkdirAll(s.DownloadDir, 0o755); err != nil {
			return fmt.Errorf("下载目录不可用: %w", err)
		}
	}
	if err := SaveSettings(s); err != nil {
		return err
	}
	m.mu.Lock()
	m.settings = s
	tasks := append([]*core.Task(nil), m.tasks...)
	m.mu.Unlock()
	for _, t := range tasks {
		t.Configure(core.Options{
			Dir:       s.DownloadDir,
			MaxConns:  s.MaxConnsPerTask,
			UserAgent: s.UserAgent,
			Headers:   s.Headers,
			Insecure:  s.Insecure,
		})
	}
	m.kickBroadcast()
	return nil
}

// AddTask 新增下载任务（排队后自动开始）。
func (m *Manager) AddTask(rawURL, fileName string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", errors.New("下载链接不能为空")
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("无效的下载链接（需 http/https）: %s", rawURL)
	}
	m.mu.Lock()
	s := m.settings
	m.mu.Unlock()

	t := core.NewTask(core.Options{
		URL:       rawURL,
		Dir:       s.DownloadDir,
		FileName:  strings.TrimSpace(fileName),
		MaxConns:  s.MaxConnsPerTask,
		UserAgent: s.UserAgent,
		Headers:   s.Headers,
		Insecure:  s.Insecure,
		Budget:    m.budget,
	})
	m.mu.Lock()
	m.tasks = append(m.tasks, t)
	m.mu.Unlock()
	m.kickBroadcast()
	m.dispatch()
	return t.ID(), nil
}

// List 返回全部任务快照。
func (m *Manager) List() []core.Snapshot {
	m.mu.Lock()
	tasks := append([]*core.Task(nil), m.tasks...)
	m.mu.Unlock()
	out := make([]core.Snapshot, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, t.Snapshot())
	}
	return out
}

func (m *Manager) get(id string) *core.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.tasks {
		if t.ID() == id {
			return t
		}
	}
	return nil
}

// Pause 暂停任务。
func (m *Manager) Pause(id string) error {
	t := m.get(id)
	if t == nil {
		return errors.New("任务不存在")
	}
	t.Pause()
	m.kickBroadcast()
	return nil
}

// Resume 恢复/重试任务（受最大并发数限制，必要时排队）。
func (m *Manager) Resume(id string) error {
	t := m.get(id)
	if t == nil {
		return errors.New("任务不存在")
	}
	switch t.State() {
	case core.StatusPaused, core.StatusFailed, core.StatusCanceled:
	default:
		return nil
	}
	t.MarkPending()
	m.kickBroadcast()
	m.dispatch()
	return nil
}

// Cancel 取消任务（保留断点）。
func (m *Manager) Cancel(id string) error {
	t := m.get(id)
	if t == nil {
		return errors.New("任务不存在")
	}
	t.Cancel()
	m.kickBroadcast()
	return nil
}

// Delete 移除任务记录；deleteFile 为 true 时同时删除文件与断点。
func (m *Manager) Delete(id string, deleteFile bool) error {
	t := m.get(id)
	if t == nil {
		return errors.New("任务不存在")
	}
	t.MarkDeleted()
	switch t.State() {
	case core.StatusProbing, core.StatusDownloading, core.StatusPausing:
		t.Cancel()
	}
	m.mu.Lock()
	for i, x := range m.tasks {
		if x == t {
			m.tasks = append(m.tasks[:i], m.tasks[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	if deleteFile {
		go func() {
			<-t.Done()
			if p := t.DisplayPath(); p != "" {
				_ = os.Remove(p)
				_ = os.Remove(core.StatePath(p))
			}
		}()
	}
	m.kickBroadcast()
	return nil
}

// PauseAll 暂停全部运行中任务（程序退出前调用）。
func (m *Manager) PauseAll() {
	m.mu.Lock()
	tasks := append([]*core.Task(nil), m.tasks...)
	m.mu.Unlock()
	for _, t := range tasks {
		switch t.State() {
		case core.StatusProbing, core.StatusDownloading:
			t.Pause()
		}
	}
}

// WaitIdle 等待所有任务离开运行状态（限时）。
func (m *Manager) WaitIdle(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		busy := false
		m.mu.Lock()
		for _, t := range m.tasks {
			switch t.State() {
			case core.StatusProbing, core.StatusDownloading, core.StatusPausing:
				busy = true
			}
		}
		m.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------- 内部调度 ----------

// dispatch 按顺序启动排队任务，直至达到并发上限。
func (m *Manager) dispatch() {
	m.mu.Lock()
	running := 0
	for _, t := range m.tasks {
		switch t.State() {
		case core.StatusProbing, core.StatusDownloading, core.StatusPausing:
			running++
		}
	}
	var toStart []*core.Task
	for _, t := range m.tasks {
		if running >= m.settings.MaxTasks {
			break
		}
		if t.State() == core.StatusPending {
			toStart = append(toStart, t)
			running++
		}
	}
	m.mu.Unlock()
	for _, t := range toStart {
		go m.runTask(t)
	}
}

func (m *Manager) runTask(t *core.Task) {
	_ = t.Run(context.Background())
	m.kickBroadcast()
	m.dispatch()
}

// ---------- 事件广播 ----------

// Subscribe 订阅任务快照流（SSE 用）。返回通道与退订函数。
func (m *Manager) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 8)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		delete(m.subs, ch)
		m.mu.Unlock()
	}
}

func (m *Manager) kickBroadcast() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// broadcastLoop 每秒推送一次全量快照；状态变化时立即推送。
func (m *Manager) broadcastLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.pushFrame()
		case <-m.kick:
			m.pushFrame()
		}
	}
}

func (m *Manager) pushFrame() {
	snaps := m.List()
	data, err := json.Marshal(snaps)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for ch := range m.subs {
		select {
		case ch <- data:
		default: // 慢消费者丢帧
		}
	}
}
