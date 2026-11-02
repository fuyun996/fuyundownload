package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 任务状态
const (
	StatusPending     = "pending"     // 排队中
	StatusProbing     = "probing"     // 连接/探测中
	StatusDownloading = "downloading" // 下载中
	StatusPausing     = "pausing"     // 暂停中
	StatusPaused      = "paused"      // 已暂停（可续传）
	StatusCompleted   = "completed"   // 已完成
	StatusFailed      = "failed"      // 失败
	StatusCanceled    = "canceled"    // 已取消
)

// 引擎参数
const (
	chunkSize          = 32 << 10         // 单次读取/写盘粒度 1 MiB
	maxSegmentAttempts = 10               // 单段最大重试次数
	stallTimeout       = 10 * time.Second // 连接无数据判定超时
	defaultMaxConns    = 32
	maxConnsLimit      = 64
	defaultUA          = "fuyun/" + Version
)

var (
	ErrPaused   = errors.New("下载已暂停")
	ErrCanceled = errors.New("下载已取消")
)

var tlsConfigInsecure = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // 用户显式选择忽略证书校验

// Options 是创建任务的配置。
type Options struct {
	URL       string
	Dir       string // 下载目录
	FileName  string // 可选，覆盖文件名
	MaxConns  int    // 单任务最大连接数
	UserAgent string
	Headers   map[string]string // 额外请求头
	Insecure  bool              // 忽略 HTTPS 证书校验
	Priority  int
	SHA256    string
	RateLimit int64       // bytes per second, per task
	Budget    *ConnBudget // 可选，全局连接预算
}

// ConnInfo 是单个连接的速度快照。
type ConnInfo struct {
	ID    int     `json:"id"`
	SegID int     `json:"seg"`
	Speed float64 `json:"speed"`
}

// Snapshot 是任务的对外状态快照（UI/CLI 展示用）。
type Snapshot struct {
	Priority    int        `json:"priority"`
	SHA256      string     `json:"sha256,omitempty"`
	RateLimit   int64      `json:"rate_limit"`
	ID          string     `json:"id"`
	URL         string     `json:"url"`
	FileName    string     `json:"file_name"`
	Dir         string     `json:"dir"`
	Path        string     `json:"path"`
	State       string     `json:"state"`
	TotalSize   int64      `json:"total_size"` // -1 表示未知
	Downloaded  int64      `json:"downloaded"`
	Speed       float64    `json:"speed"` // 字节/秒
	ETA         float64    `json:"eta"`   // 秒；-1 未知
	Connections int        `json:"connections"`
	MaxConns    int        `json:"max_conns"`
	Error       string     `json:"error,omitempty"`
	Conns       []ConnInfo `json:"conns,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt string     `json:"completed_at,omitempty"`
}

type connStat struct {
	id         int
	segID      atomic.Int64
	done       atomic.Int64 // 累计下载字节
	lastSample float64      // 上次采样值（connsMu 保护）
	speed      float64      // B/s（connsMu 保护）
}

// Task 是一个可暂停/续传的多连接下载任务。
type Task struct {
	id     string
	opts   Options
	optsMu sync.Mutex

	mu              sync.Mutex
	state           string
	errMsg          string
	info            *ProbeResult
	filePath        string
	createdAt       time.Time
	completedAt     string
	pauseRequested  bool
	cancelRequested bool
	deleted         bool
	speed           float64
	pauseCh         chan struct{}
	pauseFn         func() // 幂等关闭 pauseCh
	doneCh          chan struct{}

	// 运行期状态
	table    *SegmentTable
	meter    *SpeedMeter
	connSeq  atomic.Int64
	connsMu  sync.Mutex
	connList []*connStat
	active   atomic.Int64

	running     bool
	rateMu      sync.Mutex
	rateNext    time.Time
	cachedState *StateFile // 扫描导入时用于未运行状态的展示
}

// NewTask 创建任务（处于 pending 状态，调用 Run 开始下载）。
func NewTask(opts Options) *Task {
	if opts.MaxConns <= 0 {
		opts.MaxConns = defaultMaxConns
	}
	if opts.MaxConns > maxConnsLimit {
		opts.MaxConns = maxConnsLimit
	}
	if opts.UserAgent == "" {
		opts.UserAgent = defaultUA
	}
	if opts.Dir == "" {
		opts.Dir = "."
	}
	id := make([]byte, 5)
	_, _ = rand.Read(id)
	return &Task{
		id:        hex.EncodeToString(id),
		opts:      opts,
		state:     StatusPending,
		createdAt: time.Now(),
		pauseCh:   make(chan struct{}),
		pauseFn:   func() {}, // Run 启动时替换为真正的关闭函数
		doneCh:    make(chan struct{}),
	}
}

// ---------- 公开方法 ----------

func (t *Task) ID() string { return t.id }

func (t *Task) State() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Task) Path() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.filePath
}

// DisplayPath 返回目标文件路径；未运行过的扫描导入任务从缓存状态推导。
func (t *Task) DisplayPath() string {
	t.mu.Lock()
	p := t.filePath
	cs := t.cachedState
	t.mu.Unlock()
	if p != "" {
		return p
	}
	if cs == nil || cs.FileName == "" {
		return ""
	}
	t.optsMu.Lock()
	dir := t.opts.Dir
	t.optsMu.Unlock()
	return filepath.Join(dir, cs.FileName)
}

func (t *Task) Done() <-chan struct{} { t.mu.Lock(); defer t.mu.Unlock(); return t.doneCh }
func (t *Task) Running() bool         { t.mu.Lock(); defer t.mu.Unlock(); return t.running }

// Pause 请求暂停（优雅停止并持久化断点）。
func (t *Task) Pause() {
	t.mu.Lock()
	t.pauseRequested = true
	if !t.running {
		t.state = StatusPaused
	}
	fn := t.pauseFn
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// Cancel 请求取消（保留断点状态文件，可再次续传）。
func (t *Task) Cancel() {
	t.mu.Lock()
	t.cancelRequested = true
	if !t.running {
		t.state = StatusCanceled
	}
	fn := t.pauseFn
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// Configure 在运行前更新任务配置（管理器设置变更时调用）。
func (t *Task) Configure(opts Options) {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	if opts.Dir != "" {
		t.opts.Dir = opts.Dir
	}
	if opts.FileName != "" {
		t.opts.FileName = opts.FileName
	}
	if opts.MaxConns > 0 {
		if opts.MaxConns > maxConnsLimit {
			opts.MaxConns = maxConnsLimit
		}
		t.opts.MaxConns = opts.MaxConns
	}
	if opts.UserAgent != "" {
		t.opts.UserAgent = opts.UserAgent
	}
	if opts.Headers != nil {
		t.opts.Headers = opts.Headers
	}
	t.opts.Insecure = opts.Insecure
	if opts.Budget != nil {
		t.opts.Budget = opts.Budget
	}
}

// MarkPending 把任务标记为排队（供管理器调度），并清除旧的停止请求。
func (t *Task) MarkPending() {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case StatusPaused, StatusFailed, StatusCanceled:
		t.state = StatusPending
		t.errMsg = ""
		t.pauseRequested = false
		t.cancelRequested = false
	}
}

// MarkPausedIdle 用于扫描导入的未完成任务（不运行，仅展示）。
func (t *Task) MarkPausedIdle() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == StatusPending {
		t.state = StatusPaused
	}
}

// MarkDeleted 标记任务已从列表删除（收尾时不再写状态文件）。
func (t *Task) MarkDeleted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleted = true
}

// SetCachedState 供扫描导入时预填展示信息。
func (t *Task) SetCachedState(st *StateFile) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cachedState = st
}

// Snapshot 返回当前状态快照。
func (t *Task) Snapshot() Snapshot {
	t.optsMu.Lock()
	optsURL := t.opts.URL
	optsName := t.opts.FileName
	maxConns := t.opts.MaxConns
	dir := t.opts.Dir
	priority, checksum, rate := t.opts.Priority, t.opts.SHA256, t.opts.RateLimit
	t.optsMu.Unlock()

	t.mu.Lock()
	s := Snapshot{Priority: priority, SHA256: checksum, RateLimit: rate,
		ID:          t.id,
		URL:         optsURL,
		State:       t.state,
		Error:       t.errMsg,
		CreatedAt:   t.createdAt,
		CompletedAt: t.completedAt,
		MaxConns:    maxConns,
		Dir:         dir,
		Path:        t.filePath,
	}
	info := t.info
	speed := t.speed
	table := t.table
	cs := t.cachedState
	t.mu.Unlock()

	s.FileName = optsName
	if s.FileName == "" && cs != nil {
		s.FileName = cs.FileName
	}
	if s.FileName == "" && info != nil {
		s.FileName = info.FileName
	}
	if s.FileName == "" {
		// 服务器未提供 Content-Disposition 时，从 URL 路径推断用于展示
		if info != nil && info.FinalURL != "" {
			s.FileName = filenameFromURL(info.FinalURL)
		} else {
			s.FileName = filenameFromURL(optsURL)
		}
	}

	switch {
	case table != nil:
		done, total := table.Sum()
		s.Downloaded = done
		if total >= 0 {
			s.TotalSize = total
		} else if info != nil {
			s.TotalSize = info.Size
		} else {
			s.TotalSize = -1
		}
	case cs != nil:
		var done int64
		for _, ss := range cs.Segments {
			done += ss.Done
		}
		s.Downloaded = done
		s.TotalSize = cs.Size
	case info != nil:
		s.TotalSize = info.Size
	default:
		s.TotalSize = -1
	}

	s.Speed = speed
	s.Connections = int(t.active.Load())
	s.Conns = t.connSnapshot()
	if s.Speed > 1 && s.TotalSize > 0 && s.Downloaded < s.TotalSize {
		s.ETA = float64(s.TotalSize-s.Downloaded) / s.Speed
	} else {
		s.ETA = -1
	}
	return s
}

// connSnapshot 汇总各连接速度。
func (t *Task) connSnapshot() []ConnInfo {
	t.connsMu.Lock()
	defer t.connsMu.Unlock()
	out := make([]ConnInfo, 0, len(t.connList))
	for _, c := range t.connList {
		out = append(out, ConnInfo{ID: c.id, SegID: int(c.segID.Load()), Speed: c.speed})
	}
	return out
}

// ---------- 状态机 ----------

// Run 运行任务直至完成/暂停/取消/失败（阻塞）。
// 返回 nil 表示完成；ErrPaused 表示已暂停（可续传）；ErrCanceled 表示已取消；其他为失败原因。
func (t *Task) Run(ctx context.Context) error {
	t.mu.Lock()
	if t.running || t.deleted {
		t.mu.Unlock()
		return errors.New("任务已在运行中")
	}
	t.running = true
	t.state = StatusProbing
	t.errMsg = ""
	// 注意：不重置 pauseRequested/cancelRequested —— 排队期间收到的暂停/取消
	// 请求要在此处继承生效（MarkPending 恢复时才清除）
	t.pauseCh = make(chan struct{})
	var once sync.Once
	t.pauseFn = func() { once.Do(func() { close(t.pauseCh) }) }
	if t.pauseRequested || t.cancelRequested {
		t.pauseFn() // 启动前已被请求停止：立即进入暂停/取消收尾
	}
	t.doneCh = make(chan struct{})
	t.mu.Unlock()
	defer func() { t.mu.Lock(); t.running = false; close(t.doneCh); t.mu.Unlock() }()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-t.pauseCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	err := t.run(runCtx)

	// 收尾分类
	t.mu.Lock()
	t.speed = 0
	t.active.Store(0)
	switch {
	case err == nil:
		t.state = StatusCompleted
		t.completedAt = time.Now().Format(time.RFC3339)
		t.errMsg = ""
	case t.cancelRequested || (ctx.Err() != nil && !t.pauseRequested):
		t.state = StatusCanceled
		t.errMsg = "已取消"
		err = ErrCanceled
	case t.pauseRequested || errors.Is(err, context.Canceled):
		t.state = StatusPaused
		t.errMsg = ""
		err = ErrPaused
	default:
		t.state = StatusFailed
		t.errMsg = err.Error()
	}
	canPersist := t.info != nil && t.info.AcceptRanges && t.info.Size >= 0
	needPersist := t.state != StatusCompleted && !t.deleted && canPersist && t.table != nil
	t.mu.Unlock()

	if needPersist {
		if serr := t.persistState(); serr != nil {
			if err == nil || errors.Is(err, ErrPaused) || errors.Is(err, ErrCanceled) {
				err = fmt.Errorf("保存断点状态失败: %w", serr)
				t.mu.Lock()
				t.state = StatusFailed
				t.errMsg = err.Error()
				t.mu.Unlock()
			}
		}
	}
	return err
}

// ---------- 引擎主体 ----------

func (t *Task) maxConns() int {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	return t.opts.MaxConns
}

func (t *Task) headers() map[string]string {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	return t.opts.Headers
}

func (t *Task) userAgent() string {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	return t.opts.UserAgent
}

func (t *Task) insecure() bool {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	return t.opts.Insecure
}

func (t *Task) budget() *ConnBudget {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	return t.opts.Budget
}

func (t *Task) run(ctx context.Context) error {
	opts := t.optsSnapshot()
	client := t.buildClient()
	defer client.CloseIdleConnections()

	info, err := Probe(ctx, client, opts.URL, opts.Headers, opts.UserAgent)
	if err != nil {
		return fmt.Errorf("连接下载地址失败: %w", err)
	}

	// 确定目标文件名
	fileName := opts.FileName
	if fileName == "" {
		fileName = info.FileName
	}
	if fileName == "" {
		fileName = filenameFromURL(info.FinalURL)
	}
	if fileName == "" {
		fileName = "download"
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return fmt.Errorf("创建下载目录失败: %w", err)
	}
	fileName = sanitizeFileName(fileName)
	target, err := filepath.Abs(filepath.Join(opts.Dir, fileName))
	if err != nil {
		return err
	}
	pathMu.Lock()
	if activePaths[target] {
		pathMu.Unlock()
		return errors.New("同名文件正在下载")
	}
	activePaths[target] = true
	pathMu.Unlock()
	reservedTarget := target
	defer func() { pathMu.Lock(); delete(activePaths, reservedTarget); pathMu.Unlock() }()
	stPath := StatePath(target)
	hadStateFile := false
	if st, err := LoadState(stPath); err == nil && st.URL == opts.URL {
		hadStateFile = true
	}

	// 加载断点或新建分段表
	var table *SegmentTable
	resume := false
	if info.AcceptRanges && info.Size >= 0 {
		if st, err := LoadState(stPath); err == nil && st.Compatible(info, opts.URL) && fileMatchesState(st, partialPath(target)) {
			table = NewSegmentTableFromState(st)
			resume = true
		}
	}
	fresh := !resume
	if fresh {
		if hadStateFile {
			_ = os.Remove(stPath)
		} // 仅清除本任务的过期状态
		if info.AcceptRanges && info.Size >= 0 {
			table = NewSegmentTable(info.Size)
		} else {
			table = NewSingleSegmentTable(info.Size)
		}
		// 从未有过断点且目标已存在时改名，避免覆盖无关文件；
		// 有断点残留说明同名目标属于同一下载的旧进度，直接复用
		if !hadStateFile {
			target = dedupePath(target)
			stPath = StatePath(target)
		}
	}

	if target != reservedTarget {
		pathMu.Lock()
		if activePaths[target] {
			pathMu.Unlock()
			return errors.New("同名文件正在下载")
		}
		activePaths[target] = true
		delete(activePaths, reservedTarget)
		reservedTarget = target
		pathMu.Unlock()
	}
	t.optsMu.Lock()
	t.opts.FileName = filepath.Base(target)
	t.optsMu.Unlock()
	workPath := target + ".part"
	if resume && partialPath(target) == target {
		if err := os.Rename(target, workPath); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(workPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer func() { _ = f.Close() }()
	if info.Size >= 0 {
		if err := f.Truncate(info.Size); err != nil {
			return fmt.Errorf("预分配文件失败: %w", err)
		}
	} else if fresh {
		// 大小未知且非续传：从头写
		if err := f.Truncate(0); err != nil {
			return fmt.Errorf("重置文件失败: %w", err)
		}
	}

	t.mu.Lock()
	t.info = info
	t.filePath = target
	t.table = table
	t.meter = NewSpeedMeter()
	if t.state == StatusProbing {
		t.state = StatusDownloading
	}
	t.mu.Unlock()

	maxConns := opts.MaxConns
	if !info.AcceptRanges {
		maxConns = 1 // 不支持分段：单连接
	}

	if err := t.runSegments(ctx, client, info, table, f, maxConns, stPath); err != nil {
		return err
	}
	if opts.SHA256 != "" {
		if _, err := f.Seek(0, 0); err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		if !strings.EqualFold(fmt.Sprintf("%x", h.Sum(nil)), opts.SHA256) {
			table.Reset()
			return errors.New("SHA-256 校验失败")
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(workPath, target); err != nil {
		return err
	}
	_ = os.Remove(stPath)
	return nil
}

// optsSnapshot 原子读取配置快照。
func (t *Task) optsSnapshot() Options {
	t.optsMu.Lock()
	defer t.optsMu.Unlock()
	o := t.opts
	if o.Headers != nil {
		o.Headers = make(map[string]string, len(t.opts.Headers))
		for k, v := range t.opts.Headers {
			o.Headers[k] = v
		}
	}
	return o
}

// fileMatchesState 校验本地文件与断点状态一致（预分配后大小应等于总大小）。
func fileMatchesState(st *StateFile, target string) bool {
	if st.Size < 0 {
		return false
	}
	fi, err := os.Stat(target)
	if err != nil {
		return false
	}
	return fi.Size() == st.Size
}

// dedupePath 目标文件已存在且非空时，追加序号避免覆盖。
func dedupePath(target string) string {
	available := func(p string) bool {
		for _, suffix := range []string{"", ".part", ".fuyun"} {
			if _, err := os.Stat(p + suffix); !os.IsNotExist(err) {
				return false
			}
		}
		return true
	}
	if available(target) {
		return target
	}
	ext := filepath.Ext(target)
	base := strings.TrimSuffix(target, ext)
	for i := 1; ; i++ {
		p := fmt.Sprintf("%s(%d)%s", base, i, ext)
		if available(p) {
			return p
		}
	}
}

func (t *Task) buildClient() *http.Client {
	jar, _ := cookiejar.New(nil)
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   t.maxConns() + 4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		DisableCompression:    true,
	}
	if t.insecure() {
		transport.TLSClientConfig = tlsConfigInsecure
	}
	return &http.Client{Transport: transport, Jar: jar}
}

// ---------- 自适应并发调度 ----------

// scheduler 以实测速度为反馈，爬山式增减连接数，收敛到当前带宽的利用率拐点。
// 规则：加连接后速度提升 ≥2% → 继续加；下降 >5% → 回退并收敛；
// 连续两个窗口无变化（平台期）→ 收敛。收敛后若速度大幅下降（如其他任务释放带宽）则重新探索。
type scheduler struct {
	desired   int
	prevLevel int
	prevSpeed float64
	holds     int
	measured  bool
	converged bool
	bestSpeed float64
	lowStreak int
	age       int // 已决策次数
	trimFrom  int // 回落微调前的连接数（0 表示不在微调中）
}

func newScheduler(initial int) *scheduler {
	if initial < 1 {
		initial = 1
	}
	return &scheduler{desired: initial, prevLevel: initial}
}

func rampStep(cur, max int) int {
	remaining := max - cur
	if remaining <= 0 {
		return 0
	}
	step := remaining / 4
	if step < 1 {
		step = 1
	}
	if step > 8 {
		step = 8
	}
	return step
}

// decide 依据当前平滑速度返回期望连接数。
func (s *scheduler) decide(speed float64, maxConns, workUnits int) int {
	if workUnits < 1 {
		workUnits = 1
	}
	clamp := func(n int) int {
		if n > maxConns {
			n = maxConns
		}
		if n > workUnits {
			n = workUnits
		}
		if n < 1 {
			n = 1
		}
		return n
	}
	if s.converged {
		s.age++
		// 回落微调进行中：检验减少一个连接是否掉速
		if s.trimFrom > 0 {
			if speed >= s.bestSpeed*0.90 {
				// 未掉速：接受更少的连接
				if speed > s.bestSpeed {
					s.bestSpeed = speed
				}
				s.trimFrom = 0
			} else {
				// 掉速：回退到微调前水平
				s.desired = clamp(s.trimFrom)
				s.trimFrom = 0
			}
			s.desired = clamp(s.desired)
			return s.desired
		}
		// 带宽明显释放时（如其他任务结束）重新探索
		if s.bestSpeed > 1024*1024 && speed < s.bestSpeed*0.7 {
			s.lowStreak++
			if s.lowStreak >= 3 {
				s.converged = false
				s.holds = 0
				s.lowStreak = 0
				s.prevLevel = clamp(s.desired)
				s.prevSpeed = speed
				s.desired = clamp(s.desired + rampStep(s.desired, maxConns))
				return s.desired
			}
		} else {
			s.lowStreak = 0
		}
		// 周期性尝试减少一个连接，修剪爬坡阶段的过冲
		if s.age%6 == 0 && s.desired > 2 {
			s.trimFrom = s.desired
			s.desired = clamp(s.desired - 1)
			return s.desired
		}
		s.desired = clamp(s.desired)
		return s.desired
	}
	if !s.measured {
		s.measured = true
		s.prevLevel = s.desired
		s.prevSpeed = speed
		s.desired = clamp(s.desired + rampStep(s.desired, maxConns))
		return s.desired
	}
	if speed < 1024 {
		// 几乎无速度（网络故障或服务器限流），保持现状
		s.holds++
		if s.holds >= 4 {
			s.converged = true
			s.bestSpeed = speed
		}
		return s.desired
	}
	base := s.prevSpeed
	if base < 1024 {
		base = 1024
	}
	gain := (speed - base) / base
	switch {
	case gain >= 0.02:
		s.holds = 0
		next := clamp(s.desired + rampStep(s.desired, maxConns))
		if next == s.desired {
			// 已到上限且仍在提升：收敛
			s.converged = true
			s.bestSpeed = speed
		}
		s.prevLevel = next
		s.prevSpeed = speed
		s.desired = next
	case gain <= -0.05:
		// 增加连接反而变慢：回退到上一水平并收敛
		s.desired = clamp(s.prevLevel)
		s.bestSpeed = s.prevSpeed
		s.converged = true
		s.holds = 0
		s.lowStreak = 0
	default:
		s.holds++
		s.prevSpeed = speed
		if s.holds >= 2 {
			s.converged = true
			s.bestSpeed = speed
		}
	}
	return s.desired
}

// ---------- 多连接下载主循环 ----------

func (t *Task) runSegments(ctx context.Context, client *http.Client, info *ProbeResult, table *SegmentTable, f *os.File, maxConns int, stPath string) error {
	innerCtx, cancelInner := context.WithCancel(ctx)
	defer cancelInner()

	failCh := make(chan error, 1)
	shrink := make(chan struct{}, maxConns+1)
	var wg sync.WaitGroup

	initial := 4
	if initial > maxConns {
		initial = maxConns
	}
	if ic := table.IncompleteCount(); initial > ic {
		initial = ic
	}
	if initial < 1 {
		initial = 1
	}
	sched := newScheduler(initial)

	spawn := func(n int) {
		for i := 0; i < n; i++ {
			if b := t.budget(); b != nil && b.TryAcquire(1) == 0 {
				break
			}
			wg.Add(1)
			t.active.Add(1)
			go func() {
				defer wg.Done()
				defer t.active.Add(-1)
				if b := t.budget(); b != nil {
					defer b.Release(1)
				}
				defer func() {
					if r := recover(); r != nil {
						reportFatal(innerCtx, fmt.Errorf("内部错误: %v", r), failCh)
					}
				}()
				t.workerLoop(innerCtx, client, info, table, f, failCh, shrink)
			}()
		}
	}
	spawn(initial)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	decisions := 0
	var fatal error
	completed := false

loop:
	for {
		select {
		case <-innerCtx.Done():
			break loop
		case err := <-failCh:
			if err != nil {
				fatal = err
				cancelInner()
				break loop
			}
		case <-ticker.C:
			decisions++
			done, total := table.Sum()
			speed := t.meter.Sample(time.Now(), done)
			t.mu.Lock()
			t.speed = speed
			t.mu.Unlock()
			t.updateConnSpeeds()

			if table.AllComplete() || (total > 0 && done >= total) {
				completed = true
				break loop
			}

			if decisions%2 == 0 {
				desired := sched.decide(speed, maxConns, table.IncompleteCount())
				cur := int(t.active.Load())
				if desired > cur {
				drain:
					for {
						select {
						case <-shrink:
						default:
							break drain
						}
					}
					spawn(desired - cur)
				} else if desired < cur {
					for i := 0; i < cur-desired; i++ {
						select {
						case shrink <- struct{}{}:
						default:
						}
					}
				}
				// 周期性持久化断点（失败不中断下载，收尾时再落盘一次）
				_ = t.persistState()
			}
		}
	}

	wg.Wait()

	if completed {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("写盘失败: %w", err)
		}
		return nil
	}
	if fatal != nil {
		return fatal
	}
	return ctx.Err()
}

// workerLoop 是单个下载连接的主循环：反复领取工作单元直至全部完成。
func (t *Task) workerLoop(ctx context.Context, client *http.Client, info *ProbeResult, table *SegmentTable, f *os.File, failCh chan error, shrink chan struct{}) {
	stat := t.registerConn()
	defer t.unregisterConn(stat)
	buf := make([]byte, chunkSize)

	for {
		select {
		case <-shrink:
			return
		default:
		}
		if ctx.Err() != nil {
			return
		}
		seg := table.ClaimWork(stat.id)
		if seg == nil {
			if table.AllComplete() {
				return
			}
			if t.active.Load() <= 1 {
				return
			}
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return
			}
			continue
		}
		stat.segID.Store(int64(seg.ID))
		if !t.downloadSegment(ctx, client, info, table, f, seg, stat, buf, failCh) {
			return
		}
	}
}

// downloadSegment 下载一个分段（内部带重试）；返回 false 表示任务级致命错误已上报。
func (t *Task) downloadSegment(ctx context.Context, client *http.Client, info *ProbeResult, table *SegmentTable, f *os.File, seg *Segment, stat *connStat, buf []byte, failCh chan error) bool {
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			table.Release(seg)
			return true
		}
		start, end := table.Range(seg)
		if start > end {
			table.FinishSegment(seg)
			return true
		}

		resp, err := t.rangeGet(ctx, client, info, start, end)
		if err != nil {
			if ctx.Err() != nil {
				table.Release(seg)
				return true
			}
			if attempt < maxSegmentAttempts {
				sleepBackoff(ctx, attempt, 0)
				continue
			}
			table.Release(seg)
			return reportFatal(ctx, fmt.Errorf("请求失败（已重试 %d 次）: %w", maxSegmentAttempts, err), failCh)
		}

		if issue := checkSegmentResponse(resp, start, info, end); issue != nil {
			_ = resp.Body.Close()
			if issue.retryable && attempt < maxSegmentAttempts {
				sleepBackoff(ctx, attempt, issue.retryAfter)
				continue
			}
			table.Release(seg)
			if issue.retryable {
				issue.err = fmt.Errorf("重试 %d 次后仍失败: %w", maxSegmentAttempts, issue.err)
			}
			return reportFatal(ctx, issue.err, failCh)
		}

		outcome, serr := t.streamSegment(ctx, resp.Body, seg, table, f, stat, buf)
		_ = resp.Body.Close()

		switch outcome {
		case streamComplete:
			table.FinishSegment(seg)
			return true
		case streamAbort:
			table.Release(seg)
			return true
		case streamRetry:
			if ctx.Err() != nil {
				table.Release(seg)
				return true
			}
			if attempt < maxSegmentAttempts {
				sleepBackoff(ctx, attempt, 0)
				continue
			}
			table.Release(seg)
			return reportFatal(ctx, fmt.Errorf("分段下载失败（已重试 %d 次）: %w", maxSegmentAttempts, serr), failCh)
		default: // streamFatal
			table.Release(seg)
			return reportFatal(ctx, serr, failCh)
		}
	}
}

// reportFatal 上报任务级错误；若任务正在中止则不算致命。
func reportFatal(ctx context.Context, err error, failCh chan error) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case failCh <- err:
	default:
	}
	return false
}

// rangeGet 发起带 Range 的 GET 请求。
func (t *Task) rangeGet(ctx context.Context, client *http.Client, info *ProbeResult, start, end int64) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.FinalURL, nil)
	if err != nil {
		return nil, err
	}
	applyRequestHeaders(req, t.headers())
	if ua := t.userAgent(); ua != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", ua)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	// If-Range：若远端文件已变化，服务器将返回 200（完整内容），据此检测失效续传
	if info.ETag != "" {
		req.Header.Set("If-Range", info.ETag)
	} else if info.LastModified != "" {
		req.Header.Set("If-Range", info.LastModified)
	}
	return client.Do(req)
}

type respIssue struct {
	err        error
	retryable  bool
	retryAfter time.Duration
}

// checkSegmentResponse 校验分段响应状态码。
func checkSegmentResponse(resp *http.Response, start int64, info *ProbeResult, ends ...int64) *respIssue {
	sc := resp.StatusCode
	switch {
	case sc == http.StatusPartialContent:
		var a, b, total int64
		if _, err := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &a, &b, &total); err != nil || a != start || b < a || (len(ends) > 0 && b != ends[0]) {
			return &respIssue{err: errors.New("无效的 Content-Range 区间")}
		}
		if total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range")); ok && total >= 0 && info.Size >= 0 && total != info.Size {
			return &respIssue{err: fmt.Errorf("远端文件大小已变化（期望 %d 字节，实际 %d 字节）", info.Size, total)}
		}
		return nil
	case sc == http.StatusOK:
		if start > 0 || info.AcceptRanges {
			if info.hasValidator() {
				return &respIssue{err: errors.New("远端文件已变更（服务器返回完整内容而非分段）")}
			}
			return &respIssue{err: errors.New("服务器不支持分段下载（返回 200 而非 206）")}
		}
		return nil // start==0 的整文件响应，允许
	case sc == http.StatusRequestedRangeNotSatisfiable:
		return &respIssue{err: errors.New("请求区间越界（远端文件可能已变化）")}
	case sc == http.StatusRequestTimeout || sc == http.StatusTooManyRequests ||
		sc == http.StatusBadGateway || sc == http.StatusServiceUnavailable ||
		sc == http.StatusGatewayTimeout || sc >= 500:
		return &respIssue{
			err:        fmt.Errorf("服务器暂时不可用（HTTP %d）", sc),
			retryable:  true,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	case sc >= 400:
		return &respIssue{err: fmt.Errorf("服务器返回 HTTP %d", sc)}
	default:
		return &respIssue{err: fmt.Errorf("意外的响应状态 HTTP %d", sc)}
	}
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	var sec int
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &sec); err == nil && sec > 0 {
		if sec > 60 {
			sec = 60
		}
		return time.Duration(sec) * time.Second
	}
	return 0
}

type streamOutcome int

const (
	streamComplete streamOutcome = iota
	streamRetry
	streamAbort
	streamFatal
)

type fatalWriteError struct{ error }

func (e fatalWriteError) Error() string { return e.error.Error() }
func (e fatalWriteError) Unwrap() error { return e.error }

// streamSegment 流式读取响应体，按绝对偏移写入文件并记账。
func (t *Task) streamSegment(ctx context.Context, body io.Reader, seg *Segment, table *SegmentTable, f *os.File, stat *connStat, buf []byte) (streamOutcome, error) {
	var lastData atomic.Int64
	lastData.Store(time.Now().UnixNano())
	var stalled atomic.Bool
	var throttling atomic.Bool

	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()

	// 卡死看门狗：超过 stallTimeout 无数据则掐断当前请求（可重试）
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-reqCtx.Done():
				return
			case <-tick.C:
				if !throttling.Load() && time.Since(time.Unix(0, lastData.Load())) > stallTimeout {
					stalled.Store(true)
					cancelReq()
					if c, ok := body.(io.Closer); ok {
						_ = c.Close()
					}
					return
				}
			}
		}
	}()

	_, tblTotal := table.Sum()
	knownSize := tblTotal >= 0

	var readErr error
readLoop:
	for {
		throttling.Store(true)
		if err := t.waitRate(ctx, len(buf)); err != nil {
			return streamAbort, err
		}
		lastData.Store(time.Now().UnixNano())
		throttling.Store(false)
		n, err := body.Read(buf)
		if n > 0 {
			lastData.Store(time.Now().UnixNano())
			off, wl := table.PrepareWrite(seg, int64(n))
			if wl <= 0 {
				break readLoop // 剩余区间已被分裂给其他连接
			}
			if _, werr := f.WriteAt(buf[:wl], off); werr != nil {
				readErr = fatalWriteError{fmt.Errorf("写入文件失败: %w", werr)}
				break readLoop
			}
			counted := table.CommitWrite(seg, wl)
			stat.done.Add(counted)
			if counted < int64(n) {
				break readLoop // 到达本段边界
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break readLoop
			}
			readErr = err
			break readLoop
		}
	}

	if readErr != nil {
		var fe fatalWriteError
		if errors.As(readErr, &fe) {
			return streamFatal, readErr
		}
		if ctx.Err() != nil {
			return streamAbort, ctx.Err()
		}
		if stalled.Load() {
			return streamRetry, errors.New("连接超时无数据，已重置")
		}
		if errors.Is(readErr, context.Canceled) {
			return streamAbort, readErr
		}
		return streamRetry, readErr
	}

	// 大小未知（无 Content-Length）时，EOF 即完成
	if !knownSize {
		table.FinishSegment(seg)
		return streamComplete, nil
	}
	// 正常结束（EOF 或边界）；未完成说明响应被截断，需重试剩余区间
	if table.IsComplete(seg) {
		return streamComplete, nil
	}
	return streamRetry, errors.New("响应提前结束（数据不完整）")
}

// sleepBackoff 指数退避等待（尊重 Retry-After）。
func sleepBackoff(ctx context.Context, attempt int, retryAfter time.Duration) {
	d := 500 * time.Millisecond << min(attempt, 6)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	if retryAfter > d {
		if retryAfter > 60*time.Second {
			retryAfter = 60 * time.Second
		}
		d = retryAfter
	}
	tmr := time.NewTimer(d)
	defer tmr.Stop()
	select {
	case <-ctx.Done():
	case <-tmr.C:
	}
}

// ---------- 连接统计 ----------

func (t *Task) registerConn() *connStat {
	c := &connStat{id: int(t.connSeq.Add(1))}
	t.connsMu.Lock()
	t.connList = append(t.connList, c)
	t.connsMu.Unlock()
	return c
}

func (t *Task) unregisterConn(c *connStat) {
	t.connsMu.Lock()
	defer t.connsMu.Unlock()
	for i, x := range t.connList {
		if x == c {
			t.connList = append(t.connList[:i], t.connList[i+1:]...)
			break
		}
	}
}

// updateConnSpeeds 每 1s 采样一次各连接速度。
func (t *Task) updateConnSpeeds() {
	t.connsMu.Lock()
	defer t.connsMu.Unlock()
	for _, c := range t.connList {
		cur := float64(c.done.Load())
		c.speed = cur - c.lastSample
		if c.speed < 0 {
			c.speed = 0
		}
		c.lastSample = cur
	}
}

// ---------- 断点持久化 ----------

// persistState 原子写入断点状态文件（失败不致命）。
func (t *Task) persistState() error {
	t.mu.Lock()
	if t.deleted || t.info == nil || t.table == nil || t.filePath == "" {
		t.mu.Unlock()
		return nil
	}
	info := t.info
	table := t.table
	target := t.filePath
	t.mu.Unlock()
	if !info.AcceptRanges || info.Size < 0 {
		return nil
	}

	fileName := filepath.Base(target)
	st := &StateFile{
		Version:      stateVersion,
		URL:          t.optsSnapshot().URL,
		FinalURL:     info.FinalURL,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		FileName:     fileName,
		Segments:     table.Snapshot(),
	}
	return SaveState(StatePath(target), st)
}

// canPersist 报告当前是否支持断点续传。
func (t *Task) canPersist() bool {
	t.mu.Lock()
	info := t.info
	t.mu.Unlock()
	return info != nil && info.AcceptRanges && info.Size >= 0
}

var pathMu sync.Mutex
var activePaths = map[string]bool{}

func partialPath(target string) string {
	if _, err := os.Stat(target + ".part"); err == nil {
		return target + ".part"
	}
	return target
}
func (t *Task) waitRate(ctx context.Context, n int) error {
	rate := t.optsSnapshot().RateLimit
	if rate <= 0 {
		return nil
	}
	t.rateMu.Lock()
	now := time.Now()
	at := t.rateNext
	if at.Before(now) {
		at = now
	}
	t.rateNext = at.Add(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
	t.rateMu.Unlock()
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
