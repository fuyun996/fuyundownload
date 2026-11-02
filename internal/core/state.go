package core

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

const stateVersion = 1

// SegmentState 是持久化到磁盘的分段进度。
type SegmentState struct {
	ID    int   `json:"id"`
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  int64 `json:"done"`
}

// StateFile 是断点续传状态文件（<目标文件>.fuyun）的内容。
type StateFile struct {
	Version      int            `json:"version"`
	URL          string         `json:"url"`
	FinalURL     string         `json:"final_url,omitempty"`
	Size         int64          `json:"size"`
	ETag         string         `json:"etag,omitempty"`
	LastModified string         `json:"last_modified,omitempty"`
	FileName     string         `json:"file_name"`
	Segments     []SegmentState `json:"segments"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// StatePath 返回目标文件对应的断点状态文件路径。
func StatePath(target string) string { return target + ".fuyun" }

// SaveState 原子写入状态文件（先写临时文件再 rename）。
func SaveState(path string, st *StateFile) error {
	st.UpdatedAt = time.Now()
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadState 读取状态文件；文件不存在或格式不符时返回错误。
func LoadState(path string) (*StateFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var st StateFile
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Version != stateVersion {
		return nil, errors.New("state version mismatch")
	}
	if len(st.Segments) == 0 {
		return nil, errors.New("empty state")
	}
	return &st, nil
}

// Compatible 判断状态文件与当前探测结果是否匹配（匹配才允许续传）。
func (st *StateFile) Compatible(info *ProbeResult, origURL string) bool {
	if st == nil || st.Version != stateVersion {
		return false
	}
	if st.Size < 0 || info.Size < 0 || st.Size != info.Size {
		return false
	}
	if st.URL != origURL {
		return false
	}
	// 校验远端文件未变化：优先 ETag，其次 Last-Modified
	if st.ETag != "" && info.ETag != "" {
		if st.ETag != info.ETag {
			return false
		}
	} else if st.LastModified != "" && info.LastModified != "" {
		if st.LastModified != info.LastModified {
			return false
		}
	}
	return true
}
