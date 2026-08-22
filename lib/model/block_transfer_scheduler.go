// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"sort"
	"sync"

	"github.com/syncthing/syncthing/lib/protocol"
)

type blockTransferDescriptor struct {
	folder         string
	name           string
	sources        []blockTransferSource
	// refreshSources runs with the scheduler mutex held so source changes
	// serialize with enqueue and admission. It must not call back into the
	// scheduler or wait on code that does.
	refreshSources func([]blockTransferSource) []blockTransferSource
	bytes          int
}

type blockTransferSource struct {
	device        protocol.DeviceID
	connections   []string
	fromTemporary bool
}

type blockTransferScheduler struct {
	mut sync.Mutex

	enabled         bool
	globalLimit     int
	globalInFlight  int
	deviceLimits    map[protocol.DeviceID]int
	deviceInFlight  map[protocol.DeviceID]int
	folders         map[string]blockTransferFolder
	folderBytes     map[blockTransferFolderAccount]int64
	deviceBytes     map[blockTransferDeviceAccount]int64
	activeFolders   map[blockTransferFolderAccount]int
	activeDevices   map[blockTransferDeviceAccount]int
	connectionBytes map[string]int
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

type blockTransferFolderAccount struct {
	priority int
	folder   string
}

type blockTransferDeviceAccount struct {
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

type blockTransferSelection struct {
	index  int
	source blockTransferSource
}

type blockTransferAdmission struct {
	scheduler        *blockTransferScheduler
	device           protocol.DeviceID
	globalBytes      int
	deviceBytes      int
	connection       string
	connectionBytes  int
	fromTemporary    bool
	previousResponse <-chan struct{}
	responseDone     chan struct{}
	folderAccount    blockTransferFolderAccount
	deviceAccount    blockTransferDeviceAccount
	once             sync.Once
}

func newBlockTransferScheduler() *blockTransferScheduler {
	readyResponse := make(chan struct{})
	close(readyResponse)
	return &blockTransferScheduler{
		deviceLimits:    make(map[protocol.DeviceID]int),
		deviceInFlight:  make(map[protocol.DeviceID]int),
		folders:         make(map[string]blockTransferFolder),
		folderBytes:     make(map[blockTransferFolderAccount]int64),
		deviceBytes:     make(map[blockTransferDeviceAccount]int64),
		activeFolders:   make(map[blockTransferFolderAccount]int),
		activeDevices:   make(map[blockTransferDeviceAccount]int),
		connectionBytes: make(map[string]int),
		connectionTails: make(map[string]chan struct{}),
		readyResponse:   readyResponse,
	}
}

func (s *blockTransferScheduler) configure(cfg blockTransferSchedulerConfiguration) {
	s.mut.Lock()
	previousFolders := s.folders
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
	if cfg.enabled {
		s.initializeReprioritizedFairnessAccountsLocked(previousFolders)
	}
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) cancelConnection(connection string) {
	s.mut.Lock()
	s.finishQueuedLocked(func(waiter *blockTransferWaiter) bool {
		remainingSources := waiter.descriptor.sources[:0]
		for _, source := range waiter.descriptor.sources {
			remainingConnections := source.connections[:0]
			for _, candidate := range source.connections {
				if candidate != connection {
					remainingConnections = append(remainingConnections, candidate)
				}
			}
			source.connections = remainingConnections
			if len(source.connections) > 0 {
				remainingSources = append(remainingSources, source)
			}
		}
		waiter.descriptor.sources = remainingSources
		return len(remainingSources) == 0
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
	if descriptor.refreshSources != nil {
		descriptor.sources = descriptor.refreshSources(descriptor.sources)
		waiter.descriptor.sources = descriptor.sources
		if len(descriptor.sources) == 0 {
			waiter.result <- blockTransferResult{err: protocol.ErrGeneric}
			s.mut.Unlock()
			return waiter
		}
	}
	s.initializeFairnessAccountsLocked(descriptor)
	s.queued = append(s.queued, waiter)
	s.scheduleLocked()
	s.mut.Unlock()
	return waiter
}

func (s *blockTransferScheduler) refreshRunnableTransfers(match func(blockTransferDescriptor) bool) {
	s.mut.Lock()
	s.finishQueuedLocked(func(waiter *blockTransferWaiter) bool {
		if waiter.descriptor.refreshSources == nil || !match(waiter.descriptor) {
			return false
		}
		waiter.descriptor.sources = waiter.descriptor.refreshSources(waiter.descriptor.sources)
		return len(waiter.descriptor.sources) == 0
	}, blockTransferResult{err: protocol.ErrGeneric})
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *blockTransferScheduler) initializeFairnessAccountsLocked(descriptor blockTransferDescriptor) {
	priority := s.folders[descriptor.folder].priority
	folderAccount := blockTransferFolderAccount{priority: priority, folder: descriptor.folder}
	if !s.folderParticipatingLocked(folderAccount) {
		s.folderBytes[folderAccount] = s.minimumFolderBytesLocked(priority, nil)
	}
	seen := make(map[protocol.DeviceID]struct{})
	for _, source := range descriptor.sources {
		if _, ok := seen[source.device]; ok {
			continue
		}
		seen[source.device] = struct{}{}
		deviceAccount := blockTransferDeviceAccount{priority: priority, folder: descriptor.folder, device: source.device}
		if !s.deviceParticipatingLocked(deviceAccount) {
			s.deviceBytes[deviceAccount] = s.minimumDeviceBytesLocked(priority, descriptor.folder, true)
		}
	}
}

func (s *blockTransferScheduler) initializeReprioritizedFairnessAccountsLocked(previousFolders map[string]blockTransferFolder) {
	reprioritizedFolders := make(map[int]map[string]struct{})
	reprioritizedDevices := make(map[blockTransferDeviceAccount]struct{})
	for _, waiter := range s.queued {
		folder := s.folders[waiter.descriptor.folder]
		previous, existed := previousFolders[waiter.descriptor.folder]
		if existed && previous.priority == folder.priority {
			continue
		}
		if reprioritizedFolders[folder.priority] == nil {
			reprioritizedFolders[folder.priority] = make(map[string]struct{})
		}
		reprioritizedFolders[folder.priority][waiter.descriptor.folder] = struct{}{}
		for _, source := range waiter.descriptor.sources {
			deviceAccount := blockTransferDeviceAccount{priority: folder.priority, folder: waiter.descriptor.folder, device: source.device}
			reprioritizedDevices[deviceAccount] = struct{}{}
		}
	}

	for priority, folders := range reprioritizedFolders {
		minimum := s.minimumFolderBytesLocked(priority, folders)
		for folder := range folders {
			s.folderBytes[blockTransferFolderAccount{priority: priority, folder: folder}] = minimum
		}
	}
	for account := range reprioritizedDevices {
		s.deviceBytes[account] = s.minimumDeviceBytesLocked(account.priority, account.folder, false)
	}
}

func (s *blockTransferScheduler) folderParticipatingLocked(account blockTransferFolderAccount) bool {
	if s.activeFolders[account] > 0 {
		return true
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if ok && folder.priority == account.priority && waiter.descriptor.folder == account.folder {
			return true
		}
	}
	return false
}

func (s *blockTransferScheduler) deviceParticipatingLocked(account blockTransferDeviceAccount) bool {
	if s.activeDevices[account] > 0 {
		return true
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if ok && folder.priority == account.priority && waiter.descriptor.folder == account.folder && waiter.descriptor.hasDevice(account.device) {
			return true
		}
	}
	return false
}

func (s *blockTransferScheduler) minimumFolderBytesLocked(priority int, excludedFolders map[string]struct{}) int64 {
	var minimum int64
	found := false
	for account, count := range s.activeFolders {
		_, excluded := excludedFolders[account.folder]
		if count > 0 && account.priority == priority && !excluded && (!found || s.folderBytes[account] < minimum) {
			minimum = s.folderBytes[account]
			found = true
		}
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		_, excluded := excludedFolders[waiter.descriptor.folder]
		if !ok || folder.priority != priority || excluded {
			continue
		}
		account := blockTransferFolderAccount{priority: priority, folder: waiter.descriptor.folder}
		if !found || s.folderBytes[account] < minimum {
			minimum = s.folderBytes[account]
			found = true
		}
	}
	return minimum
}

func (s *blockTransferScheduler) minimumDeviceBytesLocked(priority int, folderID string, includeQueued bool) int64 {
	var minimum int64
	found := false
	for account, count := range s.activeDevices {
		if count > 0 && account.priority == priority && account.folder == folderID && (!found || s.deviceBytes[account] < minimum) {
			minimum = s.deviceBytes[account]
			found = true
		}
	}
	if !includeQueued {
		return minimum
	}
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if !ok || folder.priority != priority || waiter.descriptor.folder != folderID {
			continue
		}
		for _, source := range waiter.descriptor.sources {
			account := blockTransferDeviceAccount{priority: priority, folder: folderID, device: source.device}
			if !found || s.deviceBytes[account] < minimum {
				minimum = s.deviceBytes[account]
				found = true
			}
		}
	}
	return minimum
}

func (d blockTransferDescriptor) hasDevice(device protocol.DeviceID) bool {
	for _, source := range d.sources {
		if source.device == device {
			return true
		}
	}
	return false
}

func (w *blockTransferWaiter) wait() (*blockTransferAdmission, error) {
	result := <-w.result
	return result.admission, result.err
}

func (w *blockTransferWaiter) waitContext(ctx context.Context, scheduler *blockTransferScheduler) (*blockTransferAdmission, error) {
	select {
	case result := <-w.result:
		return result.admission, result.err
	case <-ctx.Done():
		if scheduler.cancelWaiter(w) {
			return nil, ctx.Err()
		}
		result := <-w.result
		if result.admission != nil {
			result.admission.close()
		}
		return nil, ctx.Err()
	}
}

func (s *blockTransferScheduler) cancelWaiter(waiter *blockTransferWaiter) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	for i, queued := range s.queued {
		if queued != waiter {
			continue
		}
		s.queued = append(s.queued[:i], s.queued[i+1:]...)
		s.scheduleLocked()
		return true
	}
	return false
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
		selected := blockTransferSelection{index: -1}
		for _, priority := range priorities {
			selected = s.firstFittingLocked(priority, reservedGlobal, reservedDevices)
			if selected.index >= 0 {
				break
			}
			for _, waiter := range s.queued {
				folder, ok := s.folders[waiter.descriptor.folder]
				if !ok || !folder.runnable || folder.priority != priority {
					continue
				}
				reservedGlobal = max(reservedGlobal, s.globalCharge(waiter.descriptor.bytes))
				for _, source := range waiter.descriptor.sources {
					deviceCharge := s.deviceCharge(source.device, waiter.descriptor.bytes)
					reservedDevices[source.device] = max(reservedDevices[source.device], deviceCharge)
				}
			}
		}
		if selected.index < 0 {
			return
		}

		waiter := s.queued[selected.index]
		s.queued = append(s.queued[:selected.index], s.queued[selected.index+1:]...)
		globalCharge := s.globalCharge(waiter.descriptor.bytes)
		deviceCharge := s.deviceCharge(selected.source.device, waiter.descriptor.bytes)
		s.globalInFlight += globalCharge
		s.deviceInFlight[selected.source.device] += deviceCharge
		folder := s.folders[waiter.descriptor.folder]
		folderAccount := blockTransferFolderAccount{priority: folder.priority, folder: waiter.descriptor.folder}
		deviceAccount := blockTransferDeviceAccount{priority: folder.priority, folder: waiter.descriptor.folder, device: selected.source.device}
		s.folderBytes[folderAccount] += int64(waiter.descriptor.bytes)
		s.deviceBytes[deviceAccount] += int64(waiter.descriptor.bytes)
		s.activeFolders[folderAccount]++
		s.activeDevices[deviceAccount]++
		connection := s.leastLoadedConnectionLocked(selected.source.connections)
		connectionBytes := max(waiter.descriptor.bytes, 0)
		s.connectionBytes[connection] += connectionBytes
		previousResponse := (<-chan struct{})(s.connectionTails[connection])
		if previousResponse == nil {
			previousResponse = s.readyResponse
		}
		responseDone := make(chan struct{})
		s.connectionTails[connection] = responseDone
		waiter.result <- blockTransferResult{admission: &blockTransferAdmission{
			scheduler:        s,
			device:           selected.source.device,
			globalBytes:      globalCharge,
			deviceBytes:      deviceCharge,
			connection:       connection,
			connectionBytes:  connectionBytes,
			fromTemporary:    selected.source.fromTemporary,
			previousResponse: previousResponse,
			responseDone:     responseDone,
			folderAccount:    folderAccount,
			deviceAccount:    deviceAccount,
		}}
	}
}

func (s *blockTransferScheduler) leastLoadedConnectionLocked(connections []string) string {
	if len(connections) == 0 {
		return ""
	}

	selected := connections[0]
	for _, connection := range connections[1:] {
		if s.connectionBytes[connection] < s.connectionBytes[selected] ||
			(s.connectionBytes[connection] == s.connectionBytes[selected] && connection < selected) {
			selected = connection
		}
	}
	return selected
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

func (s *blockTransferScheduler) firstFittingLocked(priority, reservedGlobal int, reservedDevices map[protocol.DeviceID]int) blockTransferSelection {
	selectedFolder := ""
	var selectedFolderBytes int64
	var selectedFolderSequence uint64
	for _, waiter := range s.queued {
		folder, ok := s.folders[waiter.descriptor.folder]
		if !ok || !folder.runnable || folder.priority != priority {
			continue
		}
		if _, ok := s.firstFittingSourceLocked(waiter, priority, reservedGlobal, reservedDevices); !ok {
			continue
		}
		folderBytes := s.folderBytes[blockTransferFolderAccount{priority: priority, folder: waiter.descriptor.folder}]
		if selectedFolder == "" || folderBytes < selectedFolderBytes || (folderBytes == selectedFolderBytes && waiter.sequence < selectedFolderSequence) {
			selectedFolder = waiter.descriptor.folder
			selectedFolderBytes = folderBytes
			selectedFolderSequence = waiter.sequence
		}
	}

	selected := blockTransferSelection{index: -1}
	var selectedDeviceBytes int64
	for i, waiter := range s.queued {
		if waiter.descriptor.folder != selectedFolder {
			continue
		}
		source, ok := s.firstFittingSourceLocked(waiter, priority, reservedGlobal, reservedDevices)
		if !ok {
			continue
		}
		deviceBytes := s.deviceBytes[blockTransferDeviceAccount{priority: priority, folder: selectedFolder, device: source.device}]
		if selected.index < 0 || deviceBytes < selectedDeviceBytes || (deviceBytes == selectedDeviceBytes && waiter.sequence < s.queued[selected.index].sequence) {
			selected = blockTransferSelection{index: i, source: source}
			selectedDeviceBytes = deviceBytes
		}
	}
	return selected
}

func (s *blockTransferScheduler) firstFittingSourceLocked(waiter *blockTransferWaiter, priority, reservedGlobal int, reservedDevices map[protocol.DeviceID]int) (blockTransferSource, bool) {
	globalAvailable := s.available(s.globalLimit, s.globalInFlight) - reservedGlobal
	if s.globalCharge(waiter.descriptor.bytes) > globalAvailable {
		return blockTransferSource{}, false
	}

	var selected blockTransferSource
	var selectedDeviceBytes int64
	found := false
	for _, source := range waiter.descriptor.sources {
		deviceAvailable := s.available(s.deviceLimits[source.device], s.deviceInFlight[source.device]) - reservedDevices[source.device]
		if s.deviceCharge(source.device, waiter.descriptor.bytes) > deviceAvailable {
			continue
		}
		deviceBytes := s.deviceBytes[blockTransferDeviceAccount{priority: priority, folder: waiter.descriptor.folder, device: source.device}]
		if !found || deviceBytes < selectedDeviceBytes {
			selected = source
			selectedDeviceBytes = deviceBytes
			found = true
		}
	}
	return selected, found
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
		a.scheduler.release(a)
	})
}

func (s *blockTransferScheduler) release(admission *blockTransferAdmission) {
	s.mut.Lock()
	close(admission.responseDone)
	if s.connectionTails[admission.connection] == admission.responseDone {
		delete(s.connectionTails, admission.connection)
	}
	s.globalInFlight -= admission.globalBytes
	s.deviceInFlight[admission.device] -= admission.deviceBytes
	s.connectionBytes[admission.connection] -= admission.connectionBytes
	if s.connectionBytes[admission.connection] == 0 {
		delete(s.connectionBytes, admission.connection)
	}
	s.activeFolders[admission.folderAccount]--
	if s.activeFolders[admission.folderAccount] == 0 {
		delete(s.activeFolders, admission.folderAccount)
	}
	s.activeDevices[admission.deviceAccount]--
	if s.activeDevices[admission.deviceAccount] == 0 {
		delete(s.activeDevices, admission.deviceAccount)
	}
	s.scheduleLocked()
	s.mut.Unlock()
}

func (a *blockTransferAdmission) waitForResponse() {
	<-a.previousResponse
}
