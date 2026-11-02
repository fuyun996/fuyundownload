package core

import (
	"math"
	"sync"
)

// minSplitRemainder：剩余区间小于 2×该值时不再对半分裂，避免产生过碎的请求。
const minSplitRemainder = 8 << 20 // 8 MiB

// Segment 表示文件的一个连续区间。End 为闭区间末端字节偏移；
// Done 表示该区间内已下载的字节数（相对 Start）。
type Segment struct {
	ID       int   `json:"id"`
	Start    int64 `json:"start"`
	End      int64 `json:"end"`
	Done     int64 `json:"done"`
	claimed  bool
	complete bool
}

func (s *Segment) Size() int64 { return s.End - s.Start + 1 }

func (s *Segment) Remaining() int64 {
	r := s.Size() - s.Done
	if r < 0 {
		r = 0
	}
	return r
}

func (s *Segment) Complete() bool { return s.complete || s.Done >= s.Size() }

// SegmentTable 管理全部分段，提供原子的工作领取、按偏移写入记账，
// 以及"对半分裂"工作窃取：当所有分段都被认领且仍有连接空闲时，
// 把剩余量最大的分段一分为二，让空闲连接并行下载后半段。
type SegmentTable struct {
	mu       sync.Mutex
	segments []*Segment
	nextID   int
	done     int64
	total    int64 // -1 表示大小未知
}

// NewSegmentTable 按 size 切分初始分段：
// 目标段数 = clamp(size/4MiB, 4, 1024)，极小文件退化为单段。
func NewSegmentTable(size int64) *SegmentTable {
	t := &SegmentTable{total: size}
	n := int(size / (4 << 20))
	if size > 256<<10 && n < 4 {
		n = 4
	}
	if n > 1024 {
		n = 1024
	}
	if int64(n) > size {
		n = int(size)
	}
	if n < 1 {
		n = 1
	}
	base := size / int64(n)
	rem := size % int64(n)
	var off int64
	for i := 0; i < n; i++ {
		ln := base
		if int64(i) < rem {
			ln++
		}
		t.segments = append(t.segments, &Segment{ID: i, Start: off, End: off + ln - 1})
		off += ln
	}
	t.nextID = n
	return t
}

// NewSingleSegmentTable 构造单段表（服务器不支持 Range 或大小未知时使用）。
// size >= 0 时为已知大小的整文件单段；size < 0 时为大小未知的流式单段。
func NewSingleSegmentTable(size int64) *SegmentTable {
	if size >= 0 {
		return &SegmentTable{
			total:    size,
			nextID:   1,
			segments: []*Segment{{ID: 0, Start: 0, End: size - 1}},
		}
	}
	return &SegmentTable{
		total:    -1,
		nextID:   1,
		segments: []*Segment{{ID: 0, Start: 0, End: math.MaxInt64 / 2}},
	}
}

// NewSegmentTableFromState 从断点状态恢复分段表。
func NewSegmentTableFromState(st *StateFile) *SegmentTable {
	t := &SegmentTable{total: st.Size}
	for _, ss := range st.Segments {
		s := &Segment{ID: ss.ID, Start: ss.Start, End: ss.End, Done: ss.Done}
		if s.Done > s.Size() {
			s.Done = s.Size()
		}
		if s.Done >= s.Size() {
			s.complete = true
		}
		t.done += s.Done
		t.segments = append(t.segments, s)
		if ss.ID >= t.nextID {
			t.nextID = ss.ID + 1
		}
	}
	return t
}

// ClaimWork 为一个新请求领取工作单元。
// 优先取剩余量最大的未认领分段；若全部被认领，则把剩余量最大的分段
// 剩余区间对半分裂，将后半段交给新请求。返回 nil 表示暂无可开始的工作。
func (t *SegmentTable) ClaimWork(by int) *Segment {
	t.mu.Lock()
	defer t.mu.Unlock()

	var best *Segment
	for _, s := range t.segments {
		if s.claimed || s.Complete() {
			continue
		}
		if best == nil || s.Remaining() > best.Remaining() {
			best = s
		}
	}
	if best != nil {
		best.claimed = true
		return best
	}

	// 全部被认领：尝试对半分裂剩余量最大者
	var largest *Segment
	for _, s := range t.segments {
		if s.Complete() {
			continue
		}
		if largest == nil || s.Remaining() > largest.Remaining() {
			largest = s
		}
	}
	if largest == nil {
		return nil // 全部完成
	}
	if largest.Remaining()/2 < minSplitRemainder {
		return nil // 剩余太小，不值得分裂
	}
	offset := largest.Start + largest.Done
	half := largest.Remaining() / 2
	steal := &Segment{
		ID:      t.nextID,
		Start:   offset + half,
		End:     largest.End,
		claimed: true,
	}
	t.nextID++
	largest.End = offset + half - 1
	t.segments = append(t.segments, steal)
	return steal
}

// Release 归还未完成的分段（可被其他连接重新领取）。
func (t *SegmentTable) Release(s *Segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !s.complete {
		s.claimed = false
	}
}

// FinishSegment 标记分段完成并释放。
func (t *SegmentTable) FinishSegment(s *Segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.total >= 0 && s.Done > s.Size() {
		t.done -= s.Done - s.Size()
		s.Done = s.Size()
	}
	s.complete = true
	s.claimed = false
}

// Range 返回该分段当前待下载的闭区间 [start, end]。
func (t *SegmentTable) Range(s *Segment) (start, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return s.Start + s.Done, s.End
}

// IsComplete 报告分段是否已完成。
func (t *SegmentTable) IsComplete(s *Segment) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return s.Complete()
}

// PrepareWrite 计算本块可写的绝对偏移与长度（可能因分裂而小于 n）。
func (t *SegmentTable) PrepareWrite(s *Segment, n int64) (off, wl int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	off = s.Start + s.Done
	wl = n
	if off+wl-1 > s.End {
		wl = s.End - off + 1
	}
	if wl < 0 {
		wl = 0
	}
	return off, wl
}

// CommitWrite 记账已写入的 wl 字节，返回计入进度的字节数
// （若 End 已被分裂缩小，超出部分不计，由接手该区间的连接重新计入）。
func (t *SegmentTable) CommitWrite(s *Segment, wl int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	counted := wl
	if max := s.Size() - s.Done; counted > max {
		counted = max
	}
	if counted < 0 {
		counted = 0
	}
	s.Done += counted
	t.done += counted
	return counted
}

// Sum 返回累计完成字节与总大小（total 为 -1 表示未知）。
func (t *SegmentTable) Sum() (done, total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done, t.total
}

// AllComplete 报告所有分段是否完成。
func (t *SegmentTable) AllComplete() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segments {
		if !s.Complete() {
			return false
		}
	}
	return len(t.segments) > 0
}

// IncompleteCount 返回未完成的分段数（作为并行工作单元上限）。
func (t *SegmentTable) IncompleteCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, s := range t.segments {
		if !s.Complete() {
			n++
		}
	}
	return n
}

// Snapshot 导出分段状态用于持久化。
func (t *SegmentTable) Snapshot() []SegmentState {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]SegmentState, 0, len(t.segments))
	for _, s := range t.segments {
		out = append(out, SegmentState{ID: s.ID, Start: s.Start, End: s.End, Done: s.Done})
	}
	return out
}

func (t *SegmentTable) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = 0
	for _, s := range t.segments {
		s.Done = 0
		s.complete = false
		s.claimed = false
	}
}
