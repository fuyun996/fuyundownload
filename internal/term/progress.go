package term

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"fuyun/internal/core"
)

// ---------- 格式化 ----------

func formatBytes(n float64) string {
	if n < 0 || math.IsNaN(n) {
		return "—"
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f%s", n, units[i])
	}
	return fmt.Sprintf("%.1f%s", n, units[i])
}

func formatSpeed(n float64) string {
	if n <= 0.5 {
		return "—"
	}
	return formatBytes(n) + "/s"
}

func formatETA(sec float64) string {
	if sec < 0 || math.IsInf(sec, 0) || math.IsNaN(sec) {
		return "--:--"
	}
	s := int64(math.Ceil(sec))
	h := s / 3600
	m := (s % 3600) / 60
	x := s % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, x)
	}
	return fmt.Sprintf("%02d:%02d", m, x)
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return ""
	}
	return string(r[:n-1]) + "…"
}

var stateIcon = map[string]string{
	core.StatusPending:     "⏳",
	core.StatusProbing:     "⋯",
	core.StatusDownloading: "⬇",
	core.StatusPausing:     "⏸",
	core.StatusPaused:      "⏸",
	core.StatusCompleted:   "✔",
	core.StatusFailed:      "✘",
	core.StatusCanceled:    "✖",
}

var stateText = map[string]string{
	core.StatusPending:     "排队中",
	core.StatusProbing:     "连接中",
	core.StatusDownloading: "下载中",
	core.StatusPausing:     "暂停中",
	core.StatusPaused:      "已暂停",
	core.StatusCompleted:   "已完成",
	core.StatusFailed:      "失败",
	core.StatusCanceled:    "已取消",
}

func renderBar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// ---------- 渲染器 ----------

// Renderer 在终端上原地刷新任务进度。非 TTY 时降级为低频日志。
type Renderer struct {
	w       io.Writer
	tty     bool
	lines   int
	lastLog time.Time
	quiet   bool
}

func NewRenderer(w *os.File, quiet bool) *Renderer {
	tty := false
	if w != nil {
		if fi, err := w.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
			tty = true
		}
	}
	return &Renderer{w: w, tty: tty, quiet: quiet}
}

// Render 渲染一次进度快照；force 为 true 时非 TTY 模式也输出。
func (r *Renderer) Render(tasks []core.Snapshot, force bool) {
	if r.quiet || r.w == nil {
		return
	}
	if !r.tty {
		// 非 TTY：低频单行日志
		if !force && time.Since(r.lastLog) < 5*time.Second {
			return
		}
		r.lastLog = time.Now()
		for _, s := range tasks {
			switch {
			case s.State == core.StatusDownloading || s.State == core.StatusProbing || s.State == core.StatusPausing:
				if s.TotalSize > 0 {
					pct := float64(s.Downloaded) / float64(s.TotalSize) * 100
					fmt.Fprintf(r.w, "[fuyun] %s %.1f%% ↓%s 连接 %d/%d\n",
						truncateRunes(s.FileName, 40), pct, formatSpeed(s.Speed), s.Connections, s.MaxConns)
				} else {
					fmt.Fprintf(r.w, "[fuyun] %s ↓%s 已下载 %s\n",
						truncateRunes(s.FileName, 40), formatSpeed(s.Speed), formatBytes(float64(s.Downloaded)))
				}
			case force && s.State == core.StatusCompleted:
				fmt.Fprintf(r.w, "[fuyun] ✔ %s 已完成（%s）\n", truncateRunes(s.FileName, 40), formatBytes(float64(s.TotalSize)))
			case force && (s.State == core.StatusFailed || s.State == core.StatusPaused || s.State == core.StatusCanceled):
				msg := stateText[s.State]
				if s.Error != "" {
					msg += "：" + s.Error
				}
				fmt.Fprintf(r.w, "[fuyun] ✘ %s %s\n", truncateRunes(s.FileName, 40), msg)
			}
		}
		return
	}

	var b strings.Builder
	if r.lines > 0 {
		fmt.Fprintf(&b, "\x1b[%dA\x1b[J", r.lines)
	}
	n := 0
	line := func(s string) {
		b.WriteString("\x1b[2K")
		b.WriteString(s)
		b.WriteByte('\n')
		n++
	}

	doneCount := 0
	var totalSpeed float64
	for _, s := range tasks {
		totalSpeed += s.Speed
		if s.State == core.StatusCompleted {
			doneCount++
		}
	}
	line(fmt.Sprintf("fuyun · %d/%d 完成 · ↓ %s", doneCount, len(tasks), formatSpeed(totalSpeed)))

	for _, s := range tasks {
		line(r.taskLine(s))
		if s.Error != "" && s.State != core.StatusPaused && s.State != core.StatusPending {
			line("   └ " + truncateRunes(s.Error, 90))
		}
	}
	fmt.Fprint(r.w, b.String())
	r.lines = n
}

// Finish 结束渲染（收尾换行）。
func (r *Renderer) Finish() {
	if r.tty && !r.quiet {
		fmt.Fprint(r.w, "\x1b[0m")
	}
}

func (r *Renderer) taskLine(s core.Snapshot) string {
	icon := stateIcon[s.State]
	name := truncateRunes(s.FileName, 42)
	if name == "" {
		name = truncateRunes(s.URL, 42)
	}
	switch s.State {
	case core.StatusDownloading, core.StatusProbing, core.StatusPausing:
		var extra string
		if s.TotalSize > 0 {
			pct := float64(s.Downloaded) / float64(s.TotalSize) * 100
			extra = fmt.Sprintf("%s %5.1f%%  ↓%-12s ETA %-7s 连接 %d/%d",
				renderBar(pct, 22), pct, formatSpeed(s.Speed), formatETA(s.ETA), s.Connections, s.MaxConns)
		} else {
			extra = fmt.Sprintf("↓%-12s 已下载 %s 连接 %d/%d",
				formatSpeed(s.Speed), formatBytes(float64(s.Downloaded)), s.Connections, s.MaxConns)
		}
		return fmt.Sprintf("%s %s  %s", icon, name, extra)
	default:
		info := stateText[s.State]
		switch s.State {
		case core.StatusCompleted:
			info += "  " + formatBytes(float64(s.TotalSize))
		case core.StatusPaused:
			info += fmt.Sprintf("  %s/%s · 重跑同一命令或回到 Web 界面可续传",
				formatBytes(float64(s.Downloaded)), formatBytes(float64(s.TotalSize)))
		case core.StatusFailed:
			info += "  " + truncateRunes(s.Error, 60)
		case core.StatusPending:
			// 排队中
		}
		return fmt.Sprintf("%s %s  %s", icon, name, info)
	}
}
