package term

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fuyun/internal/core"
)

// Options 是命令行模式的参数。
type Options struct {
	URLs      []string
	Dir       string
	FileName  string // 仅单个 URL 时有效
	MaxConns  int
	Headers   []string // "Key: Value" 形式
	UserAgent string
	Jobs      int // 并行任务数
	Insecure  bool
	Quiet     bool
}

// Run 执行命令行下载，返回进程退出码。
func Run(o Options) int {
	hdrs := map[string]string{}
	for _, h := range o.Headers {
		i := strings.Index(h, ":")
		if i <= 0 {
			fmt.Fprintf(os.Stderr, "无效请求头: %q（应为 \"Key: Value\"）\n", h)
			return 2
		}
		hdrs[strings.TrimSpace(h[:i])] = strings.TrimSpace(h[i+1:])
	}

	dir := o.Dir
	if dir == "" {
		dir = defaultDownloadDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "创建下载目录失败: %v\n", err)
		return 2
	}

	var tasks []*core.Task
	for _, raw := range o.URLs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			fmt.Fprintf(os.Stderr, "无效的下载链接（需 http/https）: %s\n", raw)
			return 2
		}
		name := ""
		if len(o.URLs) == 1 {
			name = o.FileName
		}
		tasks = append(tasks, core.NewTask(core.Options{
			URL:       raw,
			Dir:       dir,
			FileName:  name,
			MaxConns:  o.MaxConns,
			UserAgent: o.UserAgent,
			Headers:   hdrs,
			Insecure:  o.Insecure,
		}))
	}
	if len(tasks) == 0 {
		fmt.Fprintln(os.Stderr, "未提供下载链接。用法: fuyun <url>... [-o 目录] [-O 文件名] [-c 连接数]")
		return 2
	}

	jobs := o.Jobs
	if jobs <= 0 {
		jobs = 1
	}
	if jobs > len(tasks) {
		jobs = len(tasks)
	}

	renderer := NewRenderer(os.Stdout, o.Quiet)
	snaps := func() []core.Snapshot {
		out := make([]core.Snapshot, 0, len(tasks))
		for _, t := range tasks {
			out = append(out, t.Snapshot())
		}
		return out
	}

	stopRender := make(chan struct{})
	go func() {
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopRender:
				return
			case <-ticker.C:
				renderer.Render(snaps(), false)
			}
		}
	}()

	// 信号处理：首次优雅暂停，二次强制退出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	var signaled atomic.Bool
	go func() {
		<-sigCh
		signaled.Store(true)
		cancel()
		for _, t := range tasks {
			t.Pause()
		}
		go func() {
			<-sigCh
			os.Exit(130)
		}()
	}()

	sem := make(chan struct{}, jobs)
	var wg sync.WaitGroup
	var failed atomic.Int32
	var paused atomic.Int32
	for _, t := range tasks {
		sem <- struct{}{}
		wg.Add(1)
		go func(t *core.Task) {
			defer wg.Done()
			defer func() { <-sem }()
			err := t.Run(ctx)
			switch {
			case err == nil, errors.Is(err, core.ErrCanceled):
			case errors.Is(err, core.ErrPaused):
				paused.Add(1)
			default:
				failed.Add(1)
			}
		}(t)
	}
	wg.Wait()
	close(stopRender)
	renderer.Render(snaps(), true)
	renderer.Finish()

	if signaled.Load() && paused.Load() > 0 {
		fmt.Fprintln(os.Stderr, "已暂停：重新运行同一命令将继续下载。")
		return 130
	}
	if f := failed.Load(); f > 0 {
		return 1
	}
	return 0
}

func defaultDownloadDir() string {
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		cand := filepath.Join(home, "Downloads")
		if fi, err := os.Stat(cand); err == nil && fi.IsDir() {
			return cand
		}
	}
	return "."
}
