// Package web 通过 go:embed 内嵌 Web UI 静态资源。
package web

import "embed"

//go:embed index.html app.js style.css
var Files embed.FS
