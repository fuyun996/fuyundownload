package ui

import (
	"os/exec"
	"runtime"
)

// OpenBrowser 用系统默认浏览器打开地址（失败时静默忽略，用户可手动访问）。
func OpenBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
