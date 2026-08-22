// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"sort"
	"sync"

	"github.com/syncthing/syncthing/lib/protocol"
)

type blockTransferDescriptor struct {
	folder     string
	device     protocol.DeviceID
	connection string
	bytes      int
}

type blockTransferScheduler struct {
	mut sync.Mutex

	enabled         bool
	globalLimit     int
	globalInFlight  int
	deviceLimits    map[protocol.DeviceID]int
	deviceInFlight  map[protocol.DeviceID]int
	folders         map[string]blockTransferFolder
	folderBytes     map[blockTransferFolderShare]int64
	deviceBytes     map[blockTransferDeviceShare]int64
	activeFolders   map[blockTransferFolderShare]int
	activeDevices   map[blockTransferDeviceShare]int
	connectionTails map[string]chan struct{}
	readyResponse   chan struct{}
	queued          []*blockTransferWaiter
	nextSequence    uint64
}

type blockTransferFolder struct {
	priority int
	runnable bool
}

type blockTransferSchedulerConfiguration struct {
	enabled      bool
	globalLimit  int
	deviceLimits map[protocol.DeviceID]int
	folders      map[string]blockTransferFolder
}

type blockTransferFolderShare struct {
	priority int
	folder   string
}

type blockTransferDeviceShare struct {
	priority int
	folder   string
	device   protocol.DeviceID
}

type blockTransferWaiter struct {
	descriptor blockTransferDescriptor
	sequence   uint64
	result     chan blockTransferResult
}

type blockTransferResult struct {
	admission *blockTransferAdmission
	err       error
}

type blockTransferAdmission struct {
	scheduler        *blockTransferScheduler
	device           protocol.DeviceID
	globalBytes      int
	deviceBytes      int
	connection       string
	previousResponse <-chan struct{}
	responseDone     chan struct{}
	folderShare      blockTransferFolderShare
	deviceShare      blockTransferDeviceShare
	once             sync.Once
}

func newBlockTransferScheduler() *blockTransferScheduler {
	readyResponse := make(chan struct{})
	close(readyResponse)
	return &blockTransferScheduler{
		deviceLimits:    make(map[protocol.DeviceID]int),
		deviceInFlight:  make(map[protocol.DeviceID]int),
		folders:         make(map[string]blockTransferFolder),
		folderBytes:     make(map[blockTransferFolderShare]int64),
		deviceBytes:     make(map[blockTransferDeviceShare]int64),
		activeFolders:   make(map[blockTransferFolderShare]int),
		activeDevices:   make(map[blockTransferDeviceShare]int),
		connectionTails: make(map[string]chan struct{}),
		readyResponse:   readyResponse,
	}
}

func (s *blockTransferScheduler) setEnabled(enabled bool) {
	s.mut.Lock()
	s.enabled = enabled
	if !enabled {
		s.finishQueuedLocked(func(*blockTransferWaiter) bool { return true }, blockTransferResult{})
	}
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) configure(cfg blockTransferSchedulerConfiguration) {
	s.mut.Lock()
	if !cfg.enabled {
		s.finishQueuedLocked(func(*blockTransferWaiter) bool { return true }, blockTransferResult{})
	} else {
		s.finishQueuedLocked(func(waiter *blockTransferWaiter) bool {
			folder, ok := cfg.folders[waiter.descriptor.folder]
			return !ok || !folder.runnable
		}, blockTransferResult{err: protocol.ErrGeneric})
	}
	s.enabled = cfg.enabled
	s.globalLimit = max(cfg.globalLimit, 0)
	s.deviceLimits = cfg.deviceLimits
	s.folders = cfg.folders
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) setGlobalLimit(bytes int) {
	s.mut.Lock()
	s.globalLimit = max(bytes, 0)
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) setDeviceLimit(device protocol.DeviceID, bytes int) {
	s.mut.Lock()
	s.deviceLimits[device] = max(bytes, 0)
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) setFolder(folder string, priority int, runnable bool) {
	s.mut.Lock()
	s.folders[folder] = blockTransferFolder{priority: priority, runnable: runnable}
	if !runnable {
		s.finishQueuedLocked(func(waiter *blockTransferWaiter) bool {
			return waiter.descriptor.folder == folder
		}, blockTransferResult{err: protocol.ErrGeneric})
	}
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) cancelConnection(connection string) {
	s.mut.Lock()
	s.finishQueuedLocked(func(waiter *blockTransferWaiter) bool {
		return waiter.descriptor.connection == connection
	}, blockTransferResult{err: protocol.ErrGeneric})
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) enqueue(descriptor blockTransferDescriptor) *blockTransferWaiter {
	s.mut.Lock()
	waiter := &blockTransferWaiter{
		descriptor: descriptor,
		sequence:   s.nextSequence,
		result:     make(chan blockTransferResult, 1),
	}
	s.nextSequence++
	if !s.enabled {
		waiter.result <- blockTransferResult{}
		s.mut.Unlock()
		return waiter
	}
	if folder, ok := s.folders[descriptor.folder]; !ok || !folder.runnable {
		waiter.result <- blockTransferResult{err: protocol.ErrGeneric}
		s.mut.Unlock()
		return waiter
	}
	s.initializeSharesLocked(descriptor)
	s.queued = append(s.queued, waiter)
	s.scheduleLocked()
	s.mut.Unlock()
	return waiter
}

func (s *blockTransferScheduler) initializeSharesLocked(descriptor blockTransferDescriptor) {
	priority := s.folders[descriptor.folder].priority
	folderShare := blockTransferFolderShare{priority: priority, folder: descriptor.folder}
	if !s.folderParticipatingLocked(folderShare) {
		s.folderBytes[folderShare] = s.minimumFolderBytesLocked(priority)
	}
	deviceShare := blockTransferDeviceShare{priority: priority, folder: descriptor.folder, device: descriptor.device}
	if !s.deviceParticipatingLocked(deviceShare) {
		s.deviceBytes[deviceShare] = s.minimumDeviceBytesLocked(priority, descriptor.folder)
	}
}

func (s *blockTransferScheduler) folderParticipatingLocked(share blockTransferFolderShare) bool {
	if s.activeFolders[share] > 0 {
		return true
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if ok && folder.priority == share.priority && waiter.descriptor.folder == share.folder {
			return true
		}
	}
	return false
}

func (s *blockTransferScheduler) deviceParticipatingLocked(share blockTransferDeviceShare) bool {
	if s.activeDevices[share] > 0 {
		return true
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if ok && folder.priority == share.priority && waiter.descriptor.folder == share.folder && waiter.descriptor.device == share.device {
			return true
		}
	}
	return false
}

func (s *blockTransferScheduler) minimumFolderBytesLocked(priority int) int64 {
	var minimum int64
	found := false
	for share, count := range s.activeFolders {
		if count > 0 && share.priority == priority && (!found || s.folderBytes[share] < minimum) {
			minimum = s.folderBytes[share]
			found = true
		}
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if !ok || folder.priority != priority {
			continue
		}
		share := blockTransferFolderShare{priority: priority, folder: waiter.descriptor.folder}
		if !found || s.folderBytes[share] < minimum {
			minimum = s.folderBytes[share]
			found = true
		}
	}
	return minimum
}

func (s *blockTransferScheduler) minimumDeviceBytesLocked(priority int, folderID string) int64 {
	var minimum int64
	found := false
	for share, count := range s.activeDevices {
		if count > 0 && share.priority == priority && share.folder == folderID && (!found || s.deviceBytes[share] < minimum) {
			minimum = s.deviceBytes[share]
			found = true
		}
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if !ok || folder.priority != priority || waiter.descriptor.folder != folderID {
			continue
		}
		share := blockTransferDeviceShare{priority: priority, folder: folderID, device: waiter.descriptor.device}
		if !found || s.deviceBytes[share] < minimum {
			minimum = s.deviceBytes[share]
			found = true
		}
	}
	return minimum
}

func (w *blockTransferWaiter) wait() (*blockTransferAdmission, error) {
	result := <-w.result
	return result.admission, result.err
}

func (s *blockTransferScheduler) finishQueuedLocked(match func(*blockTransferWaiter) bool, result blockTransferResult) {
	remaining := s.queued[:0]
	for _, waiter := range s.queued {
		if match(waiter) {
			waiter.result <- result
		} else {
			remaining = append(remaining, waiter)
		}
	}
	s.queued = remaining
}

func (s *blockTransferScheduler) scheduleLocked() {
	if !s.enabled {
		return
	}
	for {
		priorities := s.queuedPrioritiesLocked()
		reservedGlobal := 0
		reservedDevices := make(map[protocol.DeviceID]int)
		selected := -1
		for _, priority := range priorities {
			selected = s.firstFittingLocked(priority, reservedGlobal, reservedDevices)
			if selected >= 0 {
				break
			}
			for _, waiter := range s.queued {
				folder, ok := s.folders[waiter.descriptor.folder]
				if !ok || !folder.runnable || folder.priority != priority {
					continue
				}
				reservedGlobal = max(reservedGlobal, s.globalCharge(waiter.descriptor.bytes))
				deviceCharge := s.deviceCharge(waiter.descriptor.device, waiter.descriptor.bytes)
				reservedDevices[waiter.descriptor.device] = max(reservedDevices[waiter.descriptor.device], deviceCharge)
			}
		}
		if selected < 0 {
			return
		}

		waiter := s.queued[selected]
		s.queued = append(s.queued[:selected], s.queued[selected+1:]...)
		globalCharge := s.globalCharge(waiter.descriptor.bytes)
		deviceCharge := s.deviceCharge(waiter.descriptor.device, waiter.descriptor.bytes)
		s.globalInFlight += globalCharge
		s.deviceInFlight[waiter.descriptor.device] += deviceCharge
		folder := s.folders[waiter.descriptor.folder]
		folderShare := blockTransferFolderShare{priority: folder.priority, folder: waiter.descriptor.folder}
		deviceShare := blockTransferDeviceShare{priority: folder.priority, folder: waiter.descriptor.folder, device: waiter.descriptor.device}
		s.folderBytes[folderShare] += int64(waiter.descriptor.bytes)
		s.deviceBytes[deviceShare] += int64(waiter.descriptor.bytes)
		s.activeFolders[folderShare]++
		s.activeDevices[deviceShare]++
		previousResponse := (<-chan struct{})(s.connectionTails[waiter.descriptor.connection])
		if previousResponse == nil {
			previousResponse = s.readyResponse
		}
		responseDone := make(chan struct{})
		s.connectionTails[waiter.descriptor.connection] = responseDone
		waiter.result <- blockTransferResult{admission: &blockTransferAdmission{
			scheduler:        s,
			device:           waiter.descriptor.device,
			globalBytes:      globalCharge,
			deviceBytes:      deviceCharge,
			connection:       waiter.descriptor.connection,
			previousResponse: previousResponse,
			responseDone:     responseDone,
			folderShare:      folderShare,
			deviceShare:      deviceShare,
		}}
	}
}

func (s *blockTransferScheduler) queuedPrioritiesLocked() []int {
	seen := make(map[int]struct{})
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if ok && folder.runnable {
			seen[folder.priority] = struct{}{}
		}
	}
	priorities := make([]int, 0, len(seen))
	for priority := range seen {
		priorities = append(priorities, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(priorities)))
	return priorities
}

func (s *blockTransferScheduler) firstFittingLocked(priority, reservedGlobal int, reservedDevices map[protocol.DeviceID]int) int {
	selectedFolder := ""
	var selectedFolderBytes int64
	var selectedFolderSequence uint64
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if !ok || !folder.runnable || folder.priority != priority {
			continue
		}
		if !s.fitsLocked(waiter, reservedGlobal, reservedDevices) {
			continue
		}
		folderBytes := s.folderBytes[blockTransferFolderShare{priority: priority, folder: waiter.descriptor.folder}]
		if selectedFolder == "" || folderBytes < selectedFolderBytes || (folderBytes == selectedFolderBytes && waiter.sequence < selectedFolderSequence) {
			selectedFolder = waiter.descriptor.folder
			selectedFolderBytes = folderBytes
			selectedFolderSequence = waiter.sequence
		}
	}

	selected := -1
	var selectedDeviceBytes int64
	for i, waiter := range s.queued {
		if waiter.descriptor.folder != selectedFolder || !s.fitsLocked(waiter, reservedGlobal, reservedDevices) {
			continue
		}
		deviceBytes := s.deviceBytes[blockTransferDeviceShare{priority: priority, folder: selectedFolder, device: waiter.descriptor.device}]
		if selected < 0 || deviceBytes < selectedDeviceBytes || (deviceBytes == selectedDeviceBytes && waiter.sequence < s.queued[selected].sequence) {
			selected = i
			selectedDeviceBytes = deviceBytes
		}
	}
	return selected
}

func (s *blockTransferScheduler) fitsLocked(waiter *blockTransferWaiter, reservedGlobal int, reservedDevices map[protocol.DeviceID]int) bool {
	globalAvailable := s.available(s.globalLimit, s.globalInFlight) - reservedGlobal
	if s.globalCharge(waiter.descriptor.bytes) > globalAvailable {
		return false
	}
	device := waiter.descriptor.device
	deviceAvailable := s.available(s.deviceLimits[device], s.deviceInFlight[device]) - reservedDevices[device]
	return s.deviceCharge(device, waiter.descriptor.bytes) <= deviceAvailable
}

func (*blockTransferScheduler) available(limit, inFlight int) int {
	if limit == 0 {
		return int(^uint(0) >> 1)
	}
	return max(limit-inFlight, 0)
}

func (s *blockTransferScheduler) globalCharge(bytes int) int {
	if s.globalLimit == 0 {
		return 0
	}
	return min(max(bytes, 0), s.globalLimit)
}

func (s *blockTransferScheduler) deviceCharge(device protocol.DeviceID, bytes int) int {
	limit := s.deviceLimits[device]
	if limit == 0 {
		return 0
	}
	return min(max(bytes, 0), limit)
}

func (a *blockTransferAdmission) close() {
	a.once.Do(func() {
		a.scheduler.mut.Lock()
		close(a.responseDone)
		if a.scheduler.connectionTails[a.connection] == a.responseDone {
			delete(a.scheduler.connectionTails, a.connection)
		}
		a.scheduler.globalInFlight -= a.globalBytes
		a.scheduler.deviceInFlight[a.device] -= a.deviceBytes
		a.scheduler.activeFolders[a.folderShare]--
		if a.scheduler.activeFolders[a.folderShare] == 0 {
			delete(a.scheduler.activeFolders, a.folderShare)
		}
		a.scheduler.activeDevices[a.deviceShare]--
		if a.scheduler.activeDevices[a.deviceShare] == 0 {
			delete(a.scheduler.activeDevices, a.deviceShare)
		}
		a.scheduler.scheduleLocked()
		a.scheduler.mut.Unlock()
	})
}

func (a *blockTransferAdmission) waitForResponse() {
	<-a.previousResponse
}
