package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// ProbeResult 是对下载链接探测得到的信息。
type ProbeResult struct {
	OriginalURL  string
	FinalURL     string // 跟随重定向后的最终地址
	Size         int64  // 字节数；-1 表示未知
	AcceptRanges bool   // 服务器是否支持 Range 分段
	ETag         string
	LastModified string
	FileName     string // 由 Content-Disposition 推断（可能为空）
	ContentType  string
}

func (r *ProbeResult) hasValidator() bool { return r.ETag != "" || r.LastModified != "" }

// Probe 探测下载链接：HEAD 优先，失败或不明确时用 Range: bytes=0-0 的 GET 兜底。
func Probe(ctx context.Context, client *http.Client, rawURL string, headers map[string]string, userAgent string) (*ProbeResult, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("无效的下载链接: %q", rawURL)
	}

	res := &ProbeResult{OriginalURL: rawURL, FinalURL: rawURL, Size: -1}

	// 1) HEAD 探测
	if resp, err := headProbe(ctx, client, rawURL, headers, userAgent); err == nil {
		if resp.StatusCode == http.StatusOK {
			fillFromResponse(res, resp)
			if resp.Request != nil && resp.Request.URL != nil {
				res.FinalURL = resp.Request.URL.String()
			}
			if res.Size >= 0 && res.AcceptRanges {
				resp.Body.Close()
				return res, nil
			}
		}
		resp.Body.Close()
	}

	// 2) GET bytes=0-0 探测（确认 Range 支持与真实大小）
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	applyRequestHeaders(req, headers)
	if userAgent != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		if res.Size >= 0 {
			// HEAD 已拿到足够信息，降级返回
			return res, nil
		}
		return nil, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	switch resp.StatusCode {
	case http.StatusPartialContent:
		res.AcceptRanges = true
		if total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range")); ok && total >= 0 {
			res.Size = total
		}
	case http.StatusOK:
		res.AcceptRanges = false
		if res.Size < 0 && resp.ContentLength > 0 {
			res.Size = resp.ContentLength
		}
		if resp.ContentLength == 0 && res.Size < 0 {
			res.Size = 0
		}
	default:
		if res.Size < 0 {
			return nil, fmt.Errorf("探测链接失败: HTTP %d", resp.StatusCode)
		}
		res.AcceptRanges = false
	}
	if resp.Request != nil && resp.Request.URL != nil {
		res.FinalURL = resp.Request.URL.String()
	}
	mergeFromResponse(res, resp)
	if res.FileName == "" {
		res.FileName = filenameFromURL(rawURL)
	}
	return res, nil
}

func headProbe(ctx context.Context, client *http.Client, rawURL string, headers map[string]string, userAgent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return nil, err
	}
	applyRequestHeaders(req, headers)
	if userAgent != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", userAgent)
	}
	return client.Do(req)
}

func fillFromResponse(res *ProbeResult, resp *http.Response) {
	if resp.ContentLength > 0 {
		res.Size = resp.ContentLength
	} else if resp.ContentLength == 0 {
		res.Size = 0
	}
	if resp.Header.Get("Accept-Ranges") == "bytes" {
		res.AcceptRanges = true
	}
	mergeFromResponse(res, resp)
}

func mergeFromResponse(res *ProbeResult, resp *http.Response) {
	if res.ETag == "" {
		res.ETag = resp.Header.Get("ETag")
	}
	if res.LastModified == "" {
		res.LastModified = resp.Header.Get("Last-Modified")
	}
	if res.ContentType == "" {
		res.ContentType = resp.Header.Get("Content-Type")
	}
	if res.FileName == "" {
		if name := parseFilenameFromCD(resp.Header.Get("Content-Disposition")); name != "" {
			res.FileName = name
		}
	}
}

// applyRequestHeaders 设置自定义请求头与默认 User-Agent。
func applyRequestHeaders(req *http.Request, headers map[string]string) {
	for k, v := range headers {
		// Host 头需要特殊设置
		if strings.EqualFold(k, "Host") {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
}

// parseContentRangeTotal 解析 "bytes 0-0/12345" 返回总大小。
func parseContentRangeTotal(cr string) (int64, bool) {
	i := strings.LastIndexByte(cr, '/')
	if i < 0 {
		return 0, false
	}
	total := strings.TrimSpace(cr[i+1:])
	if total == "*" {
		return -1, true
	}
	var n int64
	if _, err := fmt.Sscanf(total, "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

// parseFilenameFromCD 解析 Content-Disposition 中的文件名（RFC 6266/5987）。
func parseFilenameFromCD(cd string) string {
	if cd == "" {
		return ""
	}
	var plain string
	for _, part := range strings.Split(cd, ";") {
		part = strings.TrimSpace(part)
		lower := strings.ToLower(part)
		switch {
		case strings.HasPrefix(lower, "filename*="):
			v := part[len("filename*="):]
			v = strings.Trim(v, `"`)
			// 去掉 charset'lang' 前缀
			if i := strings.Index(v, "''"); i >= 0 {
				v = v[i+2:]
			}
			if dec, err := url.QueryUnescape(v); err == nil && dec != "" {
				return sanitizeFileName(dec)
			}
		case strings.HasPrefix(lower, "filename="):
			v := strings.Trim(part[len("filename="):], `"`)
			if v != "" {
				plain = v
			}
		}
	}
	if plain != "" {
		return sanitizeFileName(plain)
	}
	return ""
}

// filenameFromURL 从 URL 路径推断文件名。
func filenameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.Path == "" || u.Path == "/" {
		return ""
	}
	base := path.Base(u.Path)
	if dec, err := url.PathUnescape(base); err == nil {
		base = dec
	}
	return sanitizeFileName(base)
}

// sanitizeFileName 清理文件名中的非法字符与路径成分。
func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	var b strings.Builder
	for _, r := range name {
		if r < 32 || r == 127 {
			continue
		}
		switch r {
		case ':', '*', '?', '"', '<', '>', '|', 0:
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	name = strings.TrimSpace(b.String())
	name = strings.Trim(name, ". ")
	// rune 安全截断
	if len([]rune(name)) > 200 {
		r := []rune(name)
		name = string(r[:200])
	}
	if name == "" {
		name = "download"
	}
	return name
}
