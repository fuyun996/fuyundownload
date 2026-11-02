package ui

import (
	"fuyun/internal/manager"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAddValidation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	settings := manager.DefaultSettings()
	settings.DownloadDir = t.TempDir()
	m := manager.New(settings)
	defer func() { m.PauseAll(); m.WaitIdle(time.Second) }()
	handler := NewServer(m).routes()
	for _, body := range []string{`{"url":"file:///etc/passwd"}`, `{"url":"https://example.com","sha256":"bad"}`, `{"url":"https://example.com","rate_limit":-1}`} {
		r := httptest.NewRequest("POST", "/api/tasks", strings.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
	}
}
