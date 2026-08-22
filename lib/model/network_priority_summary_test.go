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
	state model.NetworkPrioritySchedulerState
}

func (m *schedulerStateModel) NetworkPrioritySchedulerState(string) model.NetworkPrioritySchedulerState {
	return m.state
}

func TestFolderSummaryReportsNetworkPrioritySchedulerState(t *testing.T) {
	wrapper := config.Wrap(filepath.Join(t.TempDir(), "config.xml"), config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "alpha"}},
	}, protocol.LocalDeviceID, events.NoopLogger)
	provider := &schedulerStateModel{
		Model: new(modelmocks.Model),
		state: model.NetworkPrioritySchedulerState{
			Active: true,
			Upload: model.NetworkPrioritySchedulerDirectionState{
				QueuedBytes:                 11,
				ActiveBytes:                 12,
				OldestSchedulingWaitSeconds: 13,
			},
			Download: model.NetworkPrioritySchedulerDirectionState{
				QueuedBytes:                 21,
				ActiveBytes:                 22,
				OldestSchedulingWaitSeconds: 23,
			},
		},
	}
	service := model.NewFolderSummaryService(wrapper, provider, protocol.LocalDeviceID, events.NoopLogger)

	got, err := service.Summary("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !got.NetworkPrioritySchedulingActive {
		t.Fatal("folder summary reports Network Priority scheduling inactive")
	}
	if got.NetworkPriorityScheduling != provider.state {
		t.Fatalf("folder summary Network Priority scheduling = %#v, want %#v", got.NetworkPriorityScheduling, provider.state)
	}
}
