// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration && !windows
// +build integration,!windows

package integration

import (
	"os"
	"syscall"

	"github.com/syncthing/syncthing/lib/build"
)

func studioUsage(state *os.ProcessState) studioProcessUsage {
	result := studioProcessUsage{
		UserCPUSeconds:   state.UserTime().Seconds(),
		SystemCPUSeconds: state.SystemTime().Seconds(),
	}
	if usage, ok := state.SysUsage().(*syscall.Rusage); ok {
		result.PeakRSSKiB = usage.Maxrss
		if build.IsDarwin {
			result.PeakRSSKiB /= 1024
		}
	}
	return result
}
