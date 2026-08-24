// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import "github.com/syncthing/syncthing/lib/scanner"

// FolderPrioritySchedulerDirectionState describes current Block Transfer
// work for one folder in one transfer direction.
type FolderPrioritySchedulerDirectionState struct {
	QueuedBytes                 int64   `json:"queuedBytes"`
	ActiveBytes                 int64   `json:"activeBytes"`
	OldestSchedulingWaitSeconds float64 `json:"oldestSchedulingWaitSeconds"`
}

// FolderPrioritySourceHashWorkState describes current Source Hash Work and
// the node-wide Hash Capacity and retained-handle resources it uses.
type FolderPrioritySourceHashWorkState struct {
	Queued                      int     `json:"queued"`
	Active                      int     `json:"active"`
	OldestSchedulingWaitSeconds float64 `json:"oldestSchedulingWaitSeconds"`
	HashCapacity                int     `json:"hashCapacity"`
	RetainedHandles             int     `json:"retainedHandles"`
	RetainedHandleBudget        int     `json:"retainedHandleBudget"`
}

// FolderPrioritySchedulerState describes whether Folder Priority scheduling
// is active and the current Block Transfer and Source Hash Work for one Folder.
type FolderPrioritySchedulerState struct {
	Active         bool                                  `json:"-"`
	Upload         FolderPrioritySchedulerDirectionState `json:"upload"`
	Download       FolderPrioritySchedulerDirectionState `json:"download"`
	SourceHashWork FolderPrioritySourceHashWorkState     `json:"sourceHashWork"`
}

// FolderPrioritySchedulerStateProvider exposes stable scheduler state without
// exposing scheduler queues or admission accounting.
type FolderPrioritySchedulerStateProvider interface {
	FolderPrioritySchedulerState(folder string) FolderPrioritySchedulerState
}

func (m *model) FolderPrioritySchedulerState(folder string) FolderPrioritySchedulerState {
	upload := m.uploadScheduler.directionState(folder)
	download := m.downloadScheduler.directionState(folder)
	var sourceHashWork FolderPrioritySourceHashWorkState
	if provider, ok := m.sourceHashCoordinator.(scanner.SourceHashWorkStateProvider); ok {
		state := provider.SourceHashWorkState(folder)
		sourceHashWork = FolderPrioritySourceHashWorkState{
			Queued:                      state.Queued,
			Active:                      state.Active,
			OldestSchedulingWaitSeconds: state.OldestSchedulingWaitSeconds,
			HashCapacity:                state.HashCapacity,
			RetainedHandles:             state.RetainedHandles,
			RetainedHandleBudget:        state.RetainedHandleBudget,
		}
	}
	return FolderPrioritySchedulerState{
		Active:         true,
		Upload:         upload,
		Download:       download,
		SourceHashWork: sourceHashWork,
	}
}

func (s *blockTransferScheduler) directionState(folder string) FolderPrioritySchedulerDirectionState {
	s.mut.Lock()
	defer s.mut.Unlock()

	state := FolderPrioritySchedulerDirectionState{
		ActiveBytes: s.activeBytes[folder],
	}
	var oldestWait float64
	now := s.now()
	for _, waiter := range s.queued {
		if waiter.descriptor.folder != folder {
			continue
		}
		state.QueuedBytes += int64(max(waiter.descriptor.bytes, 0))
		wait := now.Sub(waiter.enqueuedAt).Seconds()
		if wait > oldestWait {
			oldestWait = wait
		}
	}
	state.OldestSchedulingWaitSeconds = oldestWait
	return state
}
