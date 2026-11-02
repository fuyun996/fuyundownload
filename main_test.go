package main

import "testing"

func TestDownloadFlags(t *testing.T) {
	o, e := parseCLIArgs([]string{"https://example.com/f", "--limit", "128", "--sha256", "abc"})
	if e != nil || o.RateLimit != 128*1024 || o.SHA256 != "abc" {
		t.Fatalf("%+v %v", o, e)
	}
	if _, e := parseCLIArgs([]string{"--limit", "-1"}); e == nil {
		t.Fatal("negative limit accepted")
	}
}
