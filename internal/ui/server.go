package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"fuyun/internal/manager"
	"fuyun/web"
)

// Server 是本地 Web UI 服务器（仅绑定 127.0.0.1）。
type Server struct {
	mgr *manager.Manager
	ln  net.Listener
	srv *http.Server
}

func NewServer(mgr *manager.Manager) *Server {
	return &Server{mgr: mgr}
}

// Listen 绑定本地端口：优先用首选端口，被占用则依次向后尝试，最终回退随机端口。
// 返回实际可访问的 URL。
func (s *Server) Listen(preferred int) (string, error) {
	var lastErr error
	for attempt := 0; attempt <= 50; attempt++ {
		port := preferred + attempt
		if preferred <= 0 && attempt > 0 {
			port = 0
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			s.ln = ln
			return fmt.Sprintf("http://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port), nil
		}
		lastErr = err
		if preferred <= 0 {
			break
		}
	}
	return "", fmt.Errorf("无法绑定本地端口: %w", lastErr)
}

// Start 开始对外服务（异步）。
func (s *Server) Start() error {
	if s.ln == nil {
		return errors.New("尚未调用 Listen")
	}
	s.srv = &http.Server{Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.srv.Serve(s.ln) }()
	return nil
}

// Stop 优雅停止。
func (s *Server) Stop() {
	if s.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, "index.html", "text/html; charset=utf-8")
	})
	mux.HandleFunc("GET /style.css", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, "style.css", "text/css; charset=utf-8")
	})
	mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, "app.js", "text/javascript; charset=utf-8")
	})

	mux.HandleFunc("GET /api/tasks", s.handleListTasks)
	mux.HandleFunc("POST /api/tasks", s.handleAddTask)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("POST /api/tasks/{id}/pause", s.handlePause)
	mux.HandleFunc("POST /api/tasks/{id}/resume", s.handleResume)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.handleCancel)
	mux.HandleFunc("DELETE /api/tasks/{id}", s.handleDelete)
	mux.HandleFunc("GET /api/settings", s.handleGetSettings)
	mux.HandleFunc("PUT /api/settings", s.handlePutSettings)
	return mux
}

func serveAsset(w http.ResponseWriter, name, ctype string) {
	data, err := web.Files.ReadFile(name)
	if err != nil {
		http.Error(w, "资源缺失", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// ---------- API 处理 ----------

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.List())
}

func (s *Server) handleAddTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL      string `json:"url"`
		FileName string `json:"file_name"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id, err := s.mgr.AddTask(body.URL, body.FileName)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("请求体格式错误: %w", err)
	}
	return nil
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.Pause(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.Resume(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.Cancel(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	deleteFile := r.URL.Query().Get("files") == "1"
	if err := s.mgr.Delete(r.PathValue("id"), deleteFile); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.Settings())
}

func (s *Server) handlePutSettings(w http.ResponseWriter, r *http.Request) {
	var body manager.Settings
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.mgr.UpdateSettings(body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.mgr.Settings())
}

// handleEvents 推送任务快照流（Server-Sent Events）。
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "不支持流式响应", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	writeFrame := func(data []byte) bool {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	// 先推一帧当前快照
	if data, err := json.Marshal(s.mgr.List()); err == nil {
		if !writeFrame(data) {
			return
		}
	}

	ch, unsub := s.mgr.Subscribe()
	defer unsub()
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			if !writeFrame(data) {
				return
			}
		}
	}
}
