package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRejectWrongRange(t *testing.T) {
	for _, cr := range []string{"", "bytes 1-9/10", "bytes 0-8/10"} {
		r := &http.Response{StatusCode: 206, Header: http.Header{"Content-Range": {cr}}}
		if checkSegmentResponse(r, 0, &ProbeResult{Size: 10, AcceptRanges: true}, 9) == nil {
			t.Fatalf("accepted %q", cr)
		}
	}
}
func TestChecksumAndTemporaryFile(t *testing.T) {
	data := makeData(256 << 10)
	srv := httptest.NewServer(newTestServer(data, true))
	defer srv.Close()
	for _, good := range []bool{false, true} {
		dir := t.TempDir()
		checksum := strings.Repeat("0", 64)
		if good {
			checksum = fmt.Sprintf("%x", sha256.Sum256(data))
		}
		task := NewTask(Options{URL: srv.URL + "/file.bin", Dir: dir, SHA256: checksum})
		err := task.Run(context.Background())
		if (err == nil) != good {
			t.Fatalf("good=%v: %v", good, err)
		}
		_, err = os.Stat(filepath.Join(dir, "file.bin"))
		if good && err != nil {
			t.Fatal(err)
		}
		if !good && !os.IsNotExist(err) {
			t.Fatal("invalid file published")
		}
		if !good {
			st, e := LoadState(filepath.Join(dir, "file.bin.fuyun"))
			if e != nil {
				t.Fatal(e)
			}
			for _, s := range st.Segments {
				if s.Done != 0 {
					t.Fatal("bad checksum retained complete progress")
				}
			}
		}
	}
}
func TestRateWaitCancelable(t *testing.T) {
	task := NewTask(Options{RateLimit: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task.rateNext = time.Now().Add(time.Hour)
	if task.waitRate(ctx, 100) == nil {
		t.Fatal("expected cancellation")
	}
}
func TestBudgetUpdate(t *testing.T) {
	b := NewConnBudget(2)
	b.TryAcquire(2)
	b.SetMax(1)
	if b.TryAcquire(1) != 0 {
		t.Fatal("limit ignored")
	}
	b.Release(2)
	if b.TryAcquire(3) != 1 {
		t.Fatal("wrong new limit")
	}
}
