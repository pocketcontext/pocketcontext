package main

import (
	"fmt"
	"runtime/debug"
)

// Release builds set these with -ldflags. Source builds remain identifiable as
// development builds even when their checkout happens to have a release tag.
var version = "dev"
var commit = ""

func versionDetails() string {
	pocketBaseVersion := "unknown"
	revision := commit
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			if dependency.Path == "ganigeorgiev.com/pocketbase/pocketbase" {
				pocketBaseVersion = dependency.Version
				if dependency.Replace != nil {
					pocketBaseVersion += " (replaced)"
				}
				break
			}
		}
		if revision == "" {
			modified := false
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					revision = setting.Value
				case "vcs.modified":
					modified = setting.Value == "true"
				}
			}
			if modified && revision != "" {
				revision += "-dirty"
			}
		}
	}
	if revision == "" {
		revision = "unknown"
	}
	return fmt.Sprintf("%s (PocketBase %s; commit %s)", version, pocketBaseVersion, revision)
}
