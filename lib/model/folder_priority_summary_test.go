// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model_test

import (
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/model"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
)

type schedulerStateModel struct {
	*modelmocks.Model
	state model.FolderPrioritySchedulerState
}

func (m *schedulerStateModel) FolderPrioritySchedulerState(string) model.FolderPrioritySchedulerState {
	return m.state
}

func TestFolderSummaryReportsFolderPrioritySchedulerState(t *testing.T) {
	wrapper := config.Wrap(filepath.Join(t.TempDir(), "config.xml"), config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "alpha"}},
	}, protocol.LocalDeviceID, events.NoopLogger)
	provider := &schedulerStateModel{
		Model: new(modelmocks.Model),
		state: model.FolderPrioritySchedulerState{
			Active: true,
			Upload: model.FolderPrioritySchedulerDirectionState{
				QueuedBytes:                 11,
				ActiveBytes:                 12,
				OldestSchedulingWaitSeconds: 13,
			},
			Download: model.FolderPrioritySchedulerDirectionState{
				QueuedBytes:                 21,
				ActiveBytes:                 22,
				OldestSchedulingWaitSeconds: 23,
			},
			SourceHashWork: model.FolderPrioritySourceHashWorkState{
				Queued:                      31,
				Active:                      32,
				OldestSchedulingWaitSeconds: 33,
				HashCapacity:                34,
				RetainedHandles:             35,
				RetainedHandleBudget:        36,
			},
		},
	}
	service := model.NewFolderSummaryService(wrapper, provider, protocol.LocalDeviceID, events.NoopLogger)

	got, err := service.Summary("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !got.FolderPrioritySchedulingActive {
		t.Fatal("folder summary reports Folder Priority scheduling inactive")
	}
	if got.FolderPriorityScheduling != provider.state {
		t.Fatalf("folder summary Folder Priority scheduling = %#v, want %#v", got.FolderPriorityScheduling, provider.state)
	}
}
