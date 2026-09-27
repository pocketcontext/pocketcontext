package tracing

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSinkRotationAndPrivateFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traces.jsonl")
	sink, err := NewSink(Config{Enabled: true, Path: path, Service: "test", MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 80; i++ {
		sink.Submit(&Trace{SQL: strings.Repeat("x", 16000)})
	}
	sink.Close()
	for _, name := range []string{path, path + ".1"} {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 || info.Size() > 1<<20 {
			t.Fatal(info)
		}
	}
	public := filepath.Join(t.TempDir(), "public")
	if err := os.WriteFile(public, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSink(Config{Enabled: true, Path: public, Service: "test"}); err == nil {
		t.Fatal("accepted public trace file")
	}
	link := filepath.Join(t.TempDir(), "link")
	os.Symlink(path, link)
	if _, err := NewSink(Config{Enabled: true, Path: link, Service: "test"}); err == nil {
		t.Fatal("accepted symbolic link")
	}
}

func TestConfigServiceAndLimits(t *testing.T) {
	for _, service := range []string{"", "contains space", "line\nbreak", strings.Repeat("a", 101)} {
		c := Config{Enabled: true, Path: "unused", Service: service}
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted service %q", service)
		}
	}
	c := Config{Enabled: true, Path: "unused", Service: "app:region-1.v2"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
