package core

import (
	"sync"
	"time"
)

// SpeedMeter 是基于 EWMA 平滑的字节速率估计器。
// 通过周期性调用 Sample 传入累计下载字节数，得到平滑后的速度（字节/秒）。
type SpeedMeter struct {
	mu        sync.Mutex
	started   bool
	lastTime  time.Time
	lastBytes int64
	ewma      float64
	hasEwma   bool
}

func NewSpeedMeter() *SpeedMeter { return &SpeedMeter{} }

// Sample 用当前累计字节数更新估计并返回平滑速度。
func (m *SpeedMeter) Sample(now time.Time, total int64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		m.started = true
		m.lastTime = now
		m.lastBytes = total
		return 0
	}
	dt := now.Sub(m.lastTime).Seconds()
	if dt < 0.2 {
		return m.ewma
	}
	instant := float64(total-m.lastBytes) / dt
	if instant < 0 {
		instant = 0
	}
	if !m.hasEwma {
		m.ewma = instant
		m.hasEwma = true
	} else {
		const alpha = 0.35
		m.ewma = alpha*instant + (1-alpha)*m.ewma
	}
	m.lastTime = now
	m.lastBytes = total
	return m.ewma
}
