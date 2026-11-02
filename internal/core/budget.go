package core

import "sync"

// ConnBudget 是跨任务共享的全局连接数预算，防止多任务叠加产生过量连接。
type ConnBudget struct {
	mu   sync.Mutex
	used int
	max  int
}

func NewConnBudget(max int) *ConnBudget {
	if max <= 0 {
		max = 64
	}
	return &ConnBudget{max: max}
}

// TryAcquire 尝试获取最多 n 个配额，返回实际获取数（0 表示已耗尽）。
func (b *ConnBudget) TryAcquire(n int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	got := b.max - b.used
	if got > n {
		got = n
	}
	if got < 0 {
		got = 0
	}
	b.used += got
	return got
}

// Release 归还配额。
func (b *ConnBudget) Release(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
}

// Available 返回剩余配额。
func (b *ConnBudget) Available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.max - b.used
}
