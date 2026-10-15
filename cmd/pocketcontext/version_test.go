package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Re-execute the real entrypoint in a subprocess so eager initialization and
// log.Fatal are covered, without bootstrapping an application in the test runner.
func TestVersionProcess(t *testing.T) {
	if os.Getenv("POCKETCONTEXT_VERSION_TEST") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
				break
			}
		}
		version = "9.8.7"
		commit = "0123456789abcdef"
		main()
		os.Exit(0)
	}

	for _, scenario := range []struct {
		args        []string
		wantVersion bool
	}{
		{[]string{"--version"}, true},
		{[]string{"-v"}, true},
		{[]string{"--version=true"}, true},
		{[]string{"--dir", "pb_data", "--version"}, true},
		{[]string{"--version", "--dir", "pb_data"}, true},
		{[]string{"--dir", "pb_data", "--version=false"}, false},
		{[]string{"--dir", "pb_data", "--contextConfig", "--version"}, false},
		{[]string{"--dir", "pb_data", "--", "--version"}, false},
		{[]string{"--dir", "pb_data", "serve", "--origins", "--version"}, false},
		{[]string{"--dir", "pb_data", "serve", "--http", "--version"}, false},
	} {
		args := scenario.args
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			dir := t.TempDir()
			fixtures := map[string][]byte{
				"pb_data/maintenance.json":       []byte("invalid synthetic maintenance state"),
				"pb_migrations/001_synthetic.js": []byte("throw new Error('version loaded a migration');"),
				"pb_hooks/empty.pb.js":           {},
				"pocketcontext.json":             []byte("invalid synthetic config"),
			}
			for name, data := range fixtures {
				path := filepath.Join(dir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command(os.Args[0], append([]string{"-test.run=^TestVersionProcess$", "--"}, args...)...)
			command.Dir = dir
			command.Env = append(os.Environ(), "POCKETCONTEXT_VERSION_TEST=1")
			var stderr bytes.Buffer
			command.Stderr = &stderr
			output, err := command.Output()
			if !scenario.wantVersion {
				if err == nil || len(output) != 0 || !strings.Contains(stderr.String(), "maintenance state could not be loaded") {
					t.Fatalf("non-version arguments bypassed startup: %v\n%s\n%s", err, output, stderr.String())
				}
			} else if err != nil {
				t.Fatalf("version failed: %v\n%s\n%s", err, output, stderr.String())
			} else if stderr.Len() != 0 {
				t.Fatalf("version wrote to stderr: %s", stderr.String())
			} else if !strings.HasPrefix(string(output), "pocketcontext 9.8.7 (PocketBase ") || !strings.HasSuffix(string(output), "; commit 0123456789abcdef)\n") {
				t.Fatalf("unexpected version: %s", output)
			}
			seen := 0
			if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				name, err := filepath.Rel(dir, path)
				if err != nil {
					return err
				}
				expected, exists := fixtures[name]
				actual, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if !exists || !bytes.Equal(actual, expected) {
					t.Errorf("version changed or created %s", name)
				}
				seen++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if seen != len(fixtures) {
				t.Fatal("version removed fixture files")
			}
		})
	}
}
