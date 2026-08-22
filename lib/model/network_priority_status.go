// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

// NetworkPrioritySchedulerDirectionState describes current Block Transfer
// work for one folder in one transfer direction.
type NetworkPrioritySchedulerDirectionState struct {
	QueuedBytes                 int64   `json:"queuedBytes"`
	ActiveBytes                 int64   `json:"activeBytes"`
	OldestSchedulingWaitSeconds float64 `json:"oldestSchedulingWaitSeconds"`
}

// NetworkPrioritySchedulerState describes whether Network Priority scheduling
// is active and the current upload and download work for one folder.
type NetworkPrioritySchedulerState struct {
	Active   bool                                   `json:"-"`
	Upload   NetworkPrioritySchedulerDirectionState `json:"upload"`
	Download NetworkPrioritySchedulerDirectionState `json:"download"`
}

// NetworkPrioritySchedulerStateProvider exposes stable scheduler state without
// exposing scheduler queues or admission accounting.
type NetworkPrioritySchedulerStateProvider interface {
	NetworkPrioritySchedulerState(folder string) NetworkPrioritySchedulerState
}

func (m *model) NetworkPrioritySchedulerState(folder string) NetworkPrioritySchedulerState {
	upload := m.uploadScheduler.directionState(folder)
	download := m.downloadScheduler.directionState(folder)
	return NetworkPrioritySchedulerState{
		Active:   true,
		Upload:   upload,
		Download: download,
	}
}

func (s *blockTransferScheduler) directionState(folder string) NetworkPrioritySchedulerDirectionState {
	s.mut.Lock()
	defer s.mut.Unlock()

	state := NetworkPrioritySchedulerDirectionState{
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
