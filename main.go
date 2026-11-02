// fuyun 是一个多连接高速下载工具：
//
//	fuyun                     启动 Web 界面（浏览器自动打开）
//	fuyun ui [--port N]       启动 Web 界面
//	fuyun <url>... [flags]    命令行下载
package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"fuyun/internal/core"
	"fuyun/internal/manager"
	"fuyun/internal/term"
	"fuyun/internal/ui"
)

const usage = `fuyun ` + core.Version + ` — 多连接高速下载工具（断点续传 · 自适应调度）

用法:
  fuyun                        启动 Web 界面（浏览器自动打开）
  fuyun ui [--port N] [--dir 目录] [--no-browser]
  fuyun <url>... [flags]       命令行下载

命令行选项:
  -o, --output-dir 目录        保存目录（默认 ~/Downloads）
  -O, --output 文件名          指定文件名（仅单个链接时有效）
  -c, --conns N                单任务最大连接数（默认 32）
  -j, --jobs N                 同时下载的任务数（默认 1）
  -H, --header "K: V"          附加请求头（可重复）
  -A, --user-agent UA          自定义 User-Agent
      --insecure               忽略 HTTPS 证书错误
  -q, --quiet                  静默模式（不显示进度）
  -h, --help                   显示帮助

示例:
  fuyun https://example.com/ubuntu-24.04.iso
  fuyun https://a.com/f1.zip https://b.com/f2.zip -o ~/Downloads -c 16
  fuyun ui --port 9000

中断后重新运行同一命令即可断点续传。设置文件: ~/.fuyun/config.json
`

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		runUI(uiFlags{})
		return
	}
	switch args[0] {
	case "ui", "serve":
		runUI(parseUIFlags(args[1:]))
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	case "version", "-v", "--version":
		fmt.Println("fuyun", core.Version)
		return
	}

	opts, err := parseCLIArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	os.Exit(term.Run(opts))
}

// ---------- Web UI 模式 ----------

type uiFlags struct {
	port      int
	dir       string
	noBrowser bool
}

func parseUIFlags(args []string) uiFlags {
	f := uiFlags{}
	for i := 0; i < len(args); i++ {
		name := strings.TrimLeft(args[i], "-")
		switch name {
		case "port", "p":
			if i+1 < len(args) {
				i++
				fmt.Sscanf(args[i], "%d", &f.port)
			}
		case "dir", "d", "output-dir", "o":
			if i+1 < len(args) {
				i++
				f.dir = args[i]
			}
		case "no-browser":
			f.noBrowser = true
		case "help", "h":
			fmt.Print(usage)
			os.Exit(0)
		}
	}
	return f
}

func runUI(f uiFlags) {
	settings := manager.LoadSettings()
	if f.dir != "" {
		if abs, err := filepath.Abs(f.dir); err == nil {
			settings.DownloadDir = abs
		}
	}
	if f.port > 0 {
		settings.Port = f.port
	}
	if f.noBrowser {
		settings.NoBrowserOpen = true
	}

	mgr := manager.New(settings)
	srv := ui.NewServer(mgr)
	addr, err := srv.Listen(settings.Port)
	if err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		os.Exit(1)
	}
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		os.Exit(1)
	}
	fmt.Printf("fuyun %s 已启动: %s （下载目录: %s）\n", core.Version, addr, settings.DownloadDir)
	fmt.Println("按 Ctrl+C 退出（未完成任务自动暂停，下次启动可续传）")
	if !settings.NoBrowserOpen {
		ui.OpenBrowser(addr)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("\n正在暂停任务…")
	mgr.PauseAll()
	mgr.WaitIdle(8 * time.Second)
	srv.Stop()
	fmt.Println("已退出。")
}

// ---------- CLI 参数解析 ----------

// 带值的选项
var cliValueFlags = map[string]bool{
	"o": true, "output-dir": true,
	"O": true, "output": true,
	"c": true, "conns": true,
	"j": true, "jobs": true,
	"H": true, "header": true,
	"A": true, "user-agent": true,
}

// 布尔选项
var cliBoolFlags = map[string]bool{
	"insecure": true,
	"q":        true, "quiet": true,
	"h": true, "help": true,
}

// parseCLIArgs 解析命令行参数，允许 URL 与选项交错（fuyun url -o dir 和 fuyun -o dir url 均可）。
func parseCLIArgs(args []string) (term.Options, error) {
	var o term.Options
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			o.URLs = append(o.URLs, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			name := strings.TrimLeft(a, "-")
			if cliBoolFlags[name] {
				switch name {
				case "insecure":
					o.Insecure = true
				case "q", "quiet":
					o.Quiet = true
				case "h", "help":
					fmt.Print(usage)
					os.Exit(0)
				}
				continue
			}
			if !cliValueFlags[name] {
				return o, errors.New("未知选项: " + a)
			}
			i++
			if i >= len(args) {
				return o, errors.New("选项缺少值: " + a)
			}
			v := args[i]
			switch name {
			case "o", "output-dir":
				o.Dir = v
			case "O", "output":
				o.FileName = v
			case "c", "conns":
				if _, err := fmt.Sscanf(v, "%d", &o.MaxConns); err != nil {
					return o, errors.New("无效的连接数: " + v)
				}
			case "j", "jobs":
				if _, err := fmt.Sscanf(v, "%d", &o.Jobs); err != nil {
					return o, errors.New("无效的任务数: " + v)
				}
			case "H", "header":
				o.Headers = append(o.Headers, v)
			case "A", "user-agent":
				o.UserAgent = v
			}
			continue
		}
		o.URLs = append(o.URLs, a)
	}
	if o.MaxConns <= 0 {
		o.MaxConns = 32
	}
	return o, nil
}
