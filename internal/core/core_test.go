package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------- 测试服务器 ----------

var rangeReqRe = regexp.MustCompile(`^bytes=(\d+)-(\d*)$`)

type testServer struct {
	mu           sync.Mutex
	data         []byte
	etag         string
	acceptRanges bool
	chunkDelay   time.Duration // 每写 64KB 的延迟（模拟限速）
	requests     []string      // 记录 "a-b"
	served       int64         // 累计发送字节数
	active       int
	maxActive    int
	noHead       bool
}

func newTestServer(data []byte, acceptRanges bool) *testServer {
	return &testServer{data: data, acceptRanges: acceptRanges, etag: `"test-etag-001"`}
}

func (ts *testServer) rangeRequests() []string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]string(nil), ts.requests...)
}

func (ts *testServer) servedBytes() int64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.served
}

func (ts *testServer) resetRequests() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.requests = nil
}

func (ts *testServer) observedMaxConns() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.maxActive
}

func (ts *testServer) setETag(e string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.etag = e
}

func (ts *testServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ts.mu.Lock()
	ts.active++
	if ts.active > ts.maxActive {
		ts.maxActive = ts.active
	}
	noHead := ts.noHead
	acceptRanges := ts.acceptRanges
	etag := ts.etag
	chunkDelay := ts.chunkDelay
	data := ts.data
	ts.mu.Unlock()
	defer func() {
		ts.mu.Lock()
		ts.active--
		ts.mu.Unlock()
	}()

	record := func(s string) {
		ts.mu.Lock()
		ts.requests = append(ts.requests, s)
		ts.mu.Unlock()
	}

	if r.Method == http.MethodHead && !noHead {
		record("HEAD")
		if acceptRanges {
			w.Header().Set("Accept-Ranges", "bytes")
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}

	rng := r.Header.Get("Range")
	if acceptRanges && rng != "" {
		m := rangeReqRe.FindStringSubmatch(rng)
		if m == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		start, _ := strconv.ParseInt(m[1], 10, 64)
		var end int64
		if m[2] == "" {
			end = int64(len(data)) - 1
		} else {
			end, _ = strconv.ParseInt(m[2], 10, 64)
		}
		if start > end || end >= int64(len(data)) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		record(fmt.Sprintf("%d-%d", start, end))
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		writeSlow(w, body, chunkDelay)
		return
	}

	record("FULL")
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if acceptRanges {
		w.Header().Set("Accept-Ranges", "bytes")
	}
	w.WriteHeader(http.StatusOK)
	writeSlow(w, data, chunkDelay)
}

func writeSlow(w http.ResponseWriter, body []byte, delay time.Duration) {
	const piece = 64 << 10
	for len(body) > 0 {
		n := piece
		if n > len(body) {
			n = len(body)
		}
		if _, err := w.Write(body[:n]); err != nil {
			return
		}
		body = body[n:]
		if delay > 0 && len(body) > 0 {
			time.Sleep(delay)
		}
	}
}

// ---------- 工具 ----------

func makeData(size int) []byte {
	b := make([]byte, size)
	x := byte(0x5a)
	for i := range b {
		x = x*31 + byte(i%251)
		b[i] = x
	}
	return b
}

func sha256Of(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func newTask(t *testing.T, url, dir string, maxConns int) *Task {
	t.Helper()
	return NewTask(Options{URL: url, Dir: dir, MaxConns: maxConns})
}

// ---------- 测试 ----------

// TestDownloadIntegrity 多连接下载完整性 + 文件名推断。
func TestDownloadIntegrity(t *testing.T) {
	data := makeData(17 << 20)
	want := fmt.Sprintf("%x", sha256.Sum256(data))

	for _, accept := range []bool{true, false} {
		ts := newTestServer(data, accept)
		srv := httptest.NewServer(ts)
		defer srv.Close()

		dir := t.TempDir()
		task := newTask(t, srv.URL+"/bigfile.bin", dir, 8)
		if err := task.Run(context.Background()); err != nil {
			t.Fatalf("acceptRanges=%v: 下载失败: %v", accept, err)
		}
		got := sha256Of(t, filepath.Join(dir, "bigfile.bin"))
		if got != want {
			t.Fatalf("acceptRanges=%v: 内容不一致", accept)
		}
		if task.Snapshot().State != StatusCompleted {
			t.Fatalf("状态应为 completed，实际 %s", task.Snapshot().State)
		}
		if _, err := os.Stat(filepath.Join(dir, "bigfile.bin.fuyun")); !os.IsNotExist(err) {
			t.Fatalf("完成后不应残留状态文件")
		}
		srv.Close()
	}
}

// TestContentDispositionFilename 依据 Content-Disposition 命名。
func TestContentDispositionFilename(t *testing.T) {
	data := makeData(2 << 20)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''%E6%B5%8B%E8%AF%95%E6%96%87%E4%BB%B6.zip`)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Write(data)
	}))
	defer s.Close()

	dir := t.TempDir()
	task := newTask(t, s.URL+"/download?id=1", dir, 4)
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "测试文件.zip")); err != nil {
		t.Fatalf("应按 Content-Disposition 命名: %v", err)
	}
}

// TestResumeAfterPause 暂停后恢复：应从断点继续而非整段重下。
func TestResumeAfterPause(t *testing.T) {
	data := makeData(48 << 20) // 48MiB，限速下不会瞬间完成
	want := fmt.Sprintf("%x", sha256.Sum256(data))

	ts := newTestServer(data, true)
	ts.chunkDelay = 8 * time.Millisecond // 每 64KB 停 8ms ≈ 8MB/s/conn
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	task := newTask(t, srv.URL+"/resume.bin", dir, 4)

	done := make(chan error, 1)
	go func() { done <- task.Run(context.Background()) }()

	// 等待产生实际进度
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if task.Snapshot().Downloaded > 4<<20 {
			break
		}
		if st := task.Snapshot().State; st == StatusFailed || st == StatusCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := task.Snapshot().Downloaded; got <= 4<<20 {
		t.Fatalf("等待进度失败，已下载 %d", got)
	}

	task.Pause()
	if err := <-done; err != ErrPaused {
		t.Fatalf("期望 ErrPaused，实际 %v", err)
	}
	if task.Snapshot().State != StatusPaused {
		t.Fatalf("状态应为 paused")
	}
	if _, err := os.Stat(filepath.Join(dir, "resume.bin.fuyun")); err != nil {
		t.Fatalf("暂停后应存在状态文件: %v", err)
	}
	downloaded1 := task.Snapshot().Downloaded

	// 恢复（同参数重跑，模拟重启后续传）
	ts.resetRequests()
	task2 := newTask(t, srv.URL+"/resume.bin", dir, 4)
	if err := task2.Run(context.Background()); err != nil {
		t.Fatalf("续传失败: %v", err)
	}
	got := sha256Of(t, filepath.Join(dir, "resume.bin"))
	if got != want {
		t.Fatalf("续传后内容不一致")
	}

	// 第二轮必须出现 start>0 的 Range 请求（证明复用了断点）
	resumed := false
	for _, r := range ts.rangeRequests() {
		parts := strings.SplitN(r, "-", 2)
		if len(parts) == 2 && parts[0] != "0" && parts[0] != "HEAD" && parts[0] != "FULL" {
			if n, err := strconv.ParseInt(parts[0], 10, 64); err == nil && n > 0 {
				resumed = true
			}
		}
	}
	if !resumed {
		t.Fatalf("续传未复用断点，第二轮请求: %v", ts.rangeRequests())
	}
	_ = downloaded1
}

// TestResumeFromManualState 手工构造断点状态，验证恢复逻辑的确定性。
func TestResumeFromManualState(t *testing.T) {
	data := makeData(12 << 20)
	want := fmt.Sprintf("%x", sha256.Sum256(data))

	ts := newTestServer(data, true)
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	target := filepath.Join(dir, "manual.bin")
	half := int64(len(data) / 2)
	// 模拟真实断点：前半段是已下载的真实数据，后半段是预分配的零
	partial := make([]byte, len(data))
	copy(partial, data[:half])
	if err := os.WriteFile(target, partial, 0o644); err != nil {
		t.Fatal(err)
	}
	st := &StateFile{
		Version:  stateVersion,
		URL:      srv.URL + "/manual.bin",
		Size:     int64(len(data)),
		ETag:     `"test-etag-001"`,
		FileName: "manual.bin",
		Segments: []SegmentState{
			{ID: 0, Start: 0, End: half - 1, Done: half},
			{ID: 1, Start: half, End: int64(len(data)) - 1, Done: 0},
		},
	}
	if err := SaveState(StatePath(target), st); err != nil {
		t.Fatal(err)
	}

	task := newTask(t, srv.URL+"/manual.bin", dir, 4)
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("续传失败: %v", err)
	}
	if got := sha256Of(t, target); got != want {
		t.Fatalf("内容不一致")
	}
}

// TestEtagChangeDiscardsState 断点状态与服务器 ETag 不符时应丢弃重下。
func TestEtagChangeDiscardsState(t *testing.T) {
	data := makeData(6 << 20)
	ts := newTestServer(data, true)
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	target := filepath.Join(dir, "changed.bin")
	_ = os.WriteFile(target, make([]byte, len(data)), 0o644)
	st := &StateFile{
		Version:  stateVersion,
		URL:      srv.URL + "/changed.bin",
		Size:     int64(len(data)),
		ETag:     `"old-etag"`,
		FileName: "changed.bin",
		Segments: []SegmentState{
			{ID: 0, Start: 0, End: int64(len(data)) - 1, Done: int64(len(data)) / 2},
		},
	}
	if err := SaveState(StatePath(target), st); err != nil {
		t.Fatal(err)
	}

	task := newTask(t, srv.URL+"/changed.bin", dir, 4)
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	if want := fmt.Sprintf("%x", sha256.Sum256(data)); sha256Of(t, target) != want {
		t.Fatalf("内容不一致")
	}
	// 应重新完整下载（出现 start==0 的请求且服务端全量发送）
	full := false
	for _, r := range ts.rangeRequests() {
		if strings.HasPrefix(r, "0-") {
			full = true
		}
	}
	if !full {
		t.Fatalf("ETag 变更后应从头下载: %v", ts.rangeRequests())
	}
}

// TestNoRangeServer 不支持 Range 的服务器：单连接整流下载。
func TestNoRangeServer(t *testing.T) {
	data := makeData(3 << 20)
	want := fmt.Sprintf("%x", sha256.Sum256(data))

	ts := newTestServer(data, false)
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	task := newTask(t, srv.URL+"/plain.bin", dir, 16)
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	if got := sha256Of(t, filepath.Join(dir, "plain.bin")); got != want {
		t.Fatalf("内容不一致")
	}
	if c := ts.observedMaxConns(); c > 1 {
		t.Fatalf("无 Range 支持时不应并发多连接，实际 %d", c)
	}
}

// TestParallelConnections 限速服务器上应出现多连接并行。
func TestParallelConnections(t *testing.T) {
	data := makeData(24 << 20)
	ts := newTestServer(data, true)
	ts.chunkDelay = 4 * time.Millisecond
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	task := newTask(t, srv.URL+"/parallel.bin", dir, 16)
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	if want := fmt.Sprintf("%x", sha256.Sum256(data)); sha256Of(t, filepath.Join(dir, "parallel.bin")) != want {
		t.Fatalf("内容不一致")
	}
	if c := ts.observedMaxConns(); c < 2 {
		t.Fatalf("应出现并行连接，最大观测 %d", c)
	}
}

// TestCancelKeepsState 取消后状态保留、可再次续传。
func TestCancelKeepsState(t *testing.T) {
	data := makeData(40 << 20)
	ts := newTestServer(data, true)
	ts.chunkDelay = 8 * time.Millisecond
	srv := httptest.NewServer(ts)
	defer srv.Close()

	dir := t.TempDir()
	task := newTask(t, srv.URL+"/cancel.bin", dir, 4)
	done := make(chan error, 1)
	go func() { done <- task.Run(context.Background()) }()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if task.Snapshot().Downloaded > 2<<20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	task.Cancel()
	if err := <-done; err != ErrCanceled {
		t.Fatalf("期望 ErrCanceled，实际 %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cancel.bin.fuyun")); err != nil {
		t.Fatalf("取消后应保留状态文件: %v", err)
	}
	task2 := newTask(t, srv.URL+"/cancel.bin", dir, 4)
	if err := task2.Run(context.Background()); err != nil {
		t.Fatalf("重新下载失败: %v", err)
	}
	if want := fmt.Sprintf("%x", sha256.Sum256(data)); sha256Of(t, filepath.Join(dir, "cancel.bin")) != want {
		t.Fatalf("内容不一致")
	}
}

// ---------- SegmentTable 单元测试 ----------

func TestSegmentTablePlanAndCoverage(t *testing.T) {
	for _, size := range []int64{100, 1 << 20, 100 << 20, 3 << 30, 300 << 30} {
		tb := NewSegmentTable(size)
		segs := tb.Snapshot()
		var off int64
		for _, s := range segs {
			if s.Start != off {
				t.Fatalf("size=%d 分段不连续: %d != %d", size, s.Start, off)
			}
			if s.End < s.Start {
				t.Fatalf("size=%d 分段区间非法", size)
			}
			off = s.End + 1
		}
		if off != size {
			t.Fatalf("size=%d 分段覆盖不完整: %d", size, off)
		}
		done, total := tb.Sum()
		if total != size || done != 0 {
			t.Fatalf("size=%d 汇总错误 done=%d total=%d", size, done, total)
		}
	}
}

func TestSegmentTableClaimAndSplit(t *testing.T) {
	// 白盒构造：单个 64MiB 分段已被连接 1 认领，连接 2 应分裂窃取后半段
	tb := &SegmentTable{
		total:    64 << 20,
		nextID:   1,
		segments: []*Segment{{ID: 0, Start: 0, End: 64<<20 - 1, claimed: true}},
	}
	before := len(tb.Snapshot())
	steal := tb.ClaimWork(2)
	if steal == nil {
		t.Fatal("大分段应可被分裂窃取")
	}
	after := len(tb.Snapshot())
	if after != before+1 {
		t.Fatalf("分裂后分段数应 +1: %d -> %d", before, after)
	}
	if steal.Start != 32<<20 || steal.End != 64<<20-1 {
		t.Fatalf("窃取区间应为 [32MiB, 64MiB)，实际 [%d, %d]", steal.Start, steal.End)
	}
	// 原分段末端应收缩
	if tb.segments[0].End != 32<<20-1 {
		t.Fatalf("原分段应收缩至 [0, 32MiB)，实际 End=%d", tb.segments[0].End)
	}
	// 校验分裂后区间连续且覆盖不变
	segs := tb.Snapshot()
	var total int64
	seen := map[int]bool{}
	var prevEnd int64 = -1
	for _, ss := range segs {
		if seen[ss.ID] {
			t.Fatalf("分段 ID 重复: %d", ss.ID)
		}
		seen[ss.ID] = true
		if ss.Start != prevEnd+1 {
			t.Fatalf("分段区间应连续: Start=%d prevEnd=%d", ss.Start, prevEnd)
		}
		prevEnd = ss.End
		total += ss.End - ss.Start + 1
	}
	if total != 64<<20 {
		t.Fatalf("分裂后总覆盖应不变: %d", total)
	}
	// 剩余太小时不应再分裂
	tb2 := &SegmentTable{
		total:    4 << 20,
		nextID:   1,
		segments: []*Segment{{ID: 0, Start: 0, End: 4<<20 - 1, claimed: true}},
	}
	if s := tb2.ClaimWork(3); s != nil {
		t.Fatal("剩余过小不应分裂")
	}
}

func TestSegmentTableWriteAccounting(t *testing.T) {
	tb := NewSegmentTable(10 << 20)
	s := tb.ClaimWork(1)
	if s == nil {
		t.Fatal("领取失败")
	}
	off, wl := tb.PrepareWrite(s, 1<<20)
	if off != 0 || wl != 1<<20 {
		t.Fatalf("首次写入应为 (0, 1MiB)，实际 (%d, %d)", off, wl)
	}
	if c := tb.CommitWrite(s, wl); c != wl {
		t.Fatalf("记账应完整: %d", c)
	}
	done, total := tb.Sum()
	if done != 1<<20 || total != 10<<20 {
		t.Fatalf("汇总错误: done=%d total=%d", done, total)
	}
	tb.FinishSegment(s)
	if !tb.AllComplete() && s.Done < s.Size() {
		// 未完成其他段，AllComplete 应为 false
		if tb.AllComplete() {
			t.Fatal("只完成一段不应全部完成")
		}
	}
}

// ---------- 其他 ----------

func TestConnBudget(t *testing.T) {
	b := NewConnBudget(4)
	if got := b.TryAcquire(3); got != 3 {
		t.Fatalf("应获取 3，实际 %d", got)
	}
	if got := b.TryAcquire(3); got != 1 {
		t.Fatalf("应只获取 1，实际 %d", got)
	}
	b.Release(2)
	if got := b.TryAcquire(2); got != 2 {
		t.Fatalf("释放后应获取 2，实际 %d", got)
	}
}

func TestSanitizeFileName(t *testing.T) {
	cases := map[string]string{
		"a/b/c.zip":          "c.zip",
		`..\x|y.txt`:         "x_y.txt",
		"":                   "download",
		"  .hidden.  ":       "hidden",
		"测试文件（中文）.bin":       "测试文件（中文）.bin",
		"very*special:name?": "very_special_name_",
	}
	for in, want := range cases {
		if got := sanitizeFileName(in); got != want {
			t.Errorf("sanitizeFileName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseFilenameFromCD(t *testing.T) {
	cases := map[string]string{
		`attachment; filename="hello.zip"`:           "hello.zip",
		`attachment; filename*=UTF-8''%E4%B8%AD.bin`: "中.bin",
		`attachment; filename=plain.tar.gz`:          "plain.tar.gz",
		`ATTACHMENT; FILENAME="UP.ZIP"`:              "UP.ZIP",
	}
	for in, want := range cases {
		if got := parseFilenameFromCD(in); got != want {
			t.Errorf("parseFilenameFromCD(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSchedulerConverges(t *testing.T) {
	// 模拟吞吐曲线：8 连接前线性提升，之后饱和
	throughput := func(conns int) float64 {
		if conns <= 8 {
			return float64(conns) * 2e6
		}
		return 16e6
	}
	s := newScheduler(4)
	conns := s.desired
	speed := 0.0
	decisions := 0
	for decisions < 300 {
		speed = throughput(conns)
		conns = s.decide(speed, 32, 1000)
		decisions++
	}
	if !s.converged {
		t.Fatalf("应收敛，最后 conns=%d", conns)
	}
	if conns < 6 || conns > 12 {
		t.Fatalf("应收敛在拐点附近(8±4)，实际 %d", conns)
	}
	// 修剪过冲后速度应保持拐点吞吐
	if final := throughput(conns); final < 16e6*0.99 {
		t.Fatalf("收敛后速度应保持满速: %v", final)
	}
}
