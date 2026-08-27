// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration && windows
// +build integration,windows

package integration

import "os"

func studioUsage(state *os.ProcessState) studioProcessUsage {
	return studioProcessUsage{
		UserCPUSeconds:   state.UserTime().Seconds(),
		SystemCPUSeconds: state.SystemTime().Seconds(),
	}
}
