// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"sync"

	"github.com/syncthing/syncthing/lib/protocol"
)

// HashingQuantumWork executes sequential Hashing Quanta for one file.
type HashingQuantumWork interface {
	HashNext(context.Context) (HashingQuantumResult, error)
	NextHashingQuantumBytes() int64
	Close()
}

// SourceHashFolder is the current scheduling policy for one Folder.
type SourceHashFolder struct {
	ID            string
	Priority      int
	HasherCeiling int
}

// SourceHashRequest transfers ownership of one file's Source Hash Work to the
// coordinator.
type SourceHashRequest struct {
	Folder SourceHashFolder
	Work   HashingQuantumWork
}

// SourceHashCompletion reports terminal Source Hash Work. Bytes includes all
// source bytes consumed, including bytes consumed before a terminal error.
type SourceHashCompletion struct {
	File  protocol.FileInfo
	Bytes int64
	Err   error
}

// SourceHashSubmission exposes first admission separately from terminal
// completion so callers can coordinate deterministic work without inspecting
// coordinator state.
type SourceHashSubmission struct {
	Admitted   <-chan struct{}
	Completion <-chan SourceHashCompletion
}

// SourceHashEpoch keeps one Folder's Equal-Priority Share open while its scan
// may still discover or submit Source Hash Work. Close marks discovery and all
// submissions for the epoch complete; fairness resets after the remaining
// queued and active work drains.
type SourceHashEpoch interface {
	Close()
}

// SourceHashCoordinator admits Source Hash Work against one node-wide Hash
// Capacity pool. Submit synchronously enrolls work and transfers its ownership
// to the coordinator. Completion, continuation eligibility, slot release, and
// replacement selection are serialized as one transition.
type SourceHashCoordinator interface {
	Configure(capacity int, priorities map[string]int)
	BeginSourceHashEpoch(SourceHashFolder) SourceHashEpoch
	Submit(context.Context, SourceHashRequest) SourceHashSubmission
}

type sourceHashCoordinator struct {
	mut sync.Mutex

	capacity      int
	active        int
	priorities    map[string]int
	configured    bool
	activeWorks   map[*coordinatedSourceHashWork]struct{}
	byFolder      map[string]int
	charged       map[equalPriorityShareKey]int64
	activeBytes   map[equalPriorityShareKey]int64
	activeByShare map[equalPriorityShareKey]int
	epochs        map[string]int
	epochPriority map[string]int
	queued        []*coordinatedSourceHashWork
}

type equalPriorityShareKey struct {
	priority int
	folder   string
}

type sourceHashEpoch struct {
	coordinator *sourceHashCoordinator
	folder      string
	once        sync.Once
}

type coordinatedSourceHashWork struct {
	request     SourceHashRequest
	ctx         context.Context
	completion  chan SourceHashCompletion
	admitted    chan struct{}
	wasAdmitted bool
	bytes       int64
	activeBytes int64
	activeShare equalPriorityShareKey
	canceled    bool
}

// NewSourceHashCoordinator returns a coordinator with the given positive Hash
// Capacity.
func NewSourceHashCoordinator(capacity int) SourceHashCoordinator {
	if capacity < 1 {
		panic("Hash Capacity must be positive")
	}
	return &sourceHashCoordinator{
		capacity:      capacity,
		priorities:    make(map[string]int),
		activeWorks:   make(map[*coordinatedSourceHashWork]struct{}),
		byFolder:      make(map[string]int),
		charged:       make(map[equalPriorityShareKey]int64),
		activeBytes:   make(map[equalPriorityShareKey]int64),
		activeByShare: make(map[equalPriorityShareKey]int),
		epochs:        make(map[string]int),
		epochPriority: make(map[string]int),
	}
}

// Configure applies live node-wide Hash Capacity and Folder Priority policy.
// Active Hashing Quanta remain non-preemptive; queued work is reordered before
// newly available capacity is admitted.
func (c *sourceHashCoordinator) Configure(capacity int, priorities map[string]int) {
	if capacity < 1 {
		panic("Hash Capacity must be positive")
	}
	nextPriorities := make(map[string]int, len(priorities))
	for folder, priority := range priorities {
		nextPriorities[folder] = priority
	}
	c.mut.Lock()
	reprioritized := make(map[int]map[string]struct{})
	if c.configured {
		for folder, priority := range nextPriorities {
			if previous, ok := c.priorities[folder]; ok && previous != priority {
				if reprioritized[priority] == nil {
					reprioritized[priority] = make(map[string]struct{})
				}
				reprioritized[priority][folder] = struct{}{}
			}
		}
	}
	baselines := make(map[int]int64, len(reprioritized))
	for priority, folders := range reprioritized {
		baselines[priority] = c.minimumFairnessLocked(priority, folders)
	}

	c.capacity = capacity
	c.priorities = nextPriorities
	for priority, folders := range reprioritized {
		for folder := range folders {
			c.charged[equalPriorityShareKey{priority: priority, folder: folder}] = baselines[priority]
		}
	}
	for folder := range c.epochs {
		if priority, ok := c.priorities[folder]; ok {
			c.epochPriority[folder] = priority
		} else {
			delete(c.epochPriority, folder)
		}
	}
	c.configured = true
	for work := range c.activeWorks {
		priority, ok := c.priorities[work.request.Folder.ID]
		if !ok {
			work.canceled = true
			continue
		}
		work.request.Folder.Priority = priority
	}
	canceled := make([]*coordinatedSourceHashWork, 0)
	queued := c.queued[:0]
	for _, work := range c.queued {
		priority, ok := c.priorities[work.request.Folder.ID]
		if !ok {
			canceled = append(canceled, work)
			continue
		}
		work.request.Folder.Priority = priority
		queued = append(queued, work)
	}
	c.queued = queued
	c.scheduleLocked()
	c.mut.Unlock()
	for _, work := range canceled {
		c.cancel(work)
	}
}

func (*sourceHashCoordinator) cancel(work *coordinatedSourceHashWork) {
	work.request.Work.Close()
	completeCanceledSourceHashWork(work)
}

func completeCanceledSourceHashWork(work *coordinatedSourceHashWork) {
	work.completion <- SourceHashCompletion{
		Bytes: work.bytes,
		Err:   context.Canceled,
	}
	close(work.completion)
}

func (c *sourceHashCoordinator) BeginSourceHashEpoch(folder SourceHashFolder) SourceHashEpoch {
	c.mut.Lock()
	if priority, ok := c.priorities[folder.ID]; c.configured && ok {
		folder.Priority = priority
	}
	shareKey := equalPriorityShareKey{priority: folder.Priority, folder: folder.ID}
	c.initializeFairnessLocked(shareKey)
	c.epochs[folder.ID]++
	c.epochPriority[folder.ID] = folder.Priority
	c.mut.Unlock()
	return &sourceHashEpoch{
		coordinator: c,
		folder:      folder.ID,
	}
}

func (e *sourceHashEpoch) Close() {
	e.once.Do(func() {
		e.coordinator.mut.Lock()
		e.coordinator.epochs[e.folder]--
		if e.coordinator.epochs[e.folder] == 0 {
			delete(e.coordinator.epochs, e.folder)
			delete(e.coordinator.epochPriority, e.folder)
		}
		e.coordinator.mut.Unlock()
	})
}

func (c *sourceHashCoordinator) Submit(ctx context.Context, request SourceHashRequest) SourceHashSubmission {
	work := &coordinatedSourceHashWork{
		request:    request,
		ctx:        ctx,
		completion: make(chan SourceHashCompletion, 1),
		admitted:   make(chan struct{}),
	}
	c.mut.Lock()
	if c.configured {
		priority, ok := c.priorities[request.Folder.ID]
		if !ok {
			c.mut.Unlock()
			c.cancel(work)
			return SourceHashSubmission{
				Admitted:   work.admitted,
				Completion: work.completion,
			}
		}
		work.request.Folder.Priority = priority
	}
	c.initializeFairnessLocked(work.shareKey())
	c.queued = append(c.queued, work)
	c.scheduleLocked()
	c.mut.Unlock()
	return SourceHashSubmission{
		Admitted:   work.admitted,
		Completion: work.completion,
	}
}

func (c *sourceHashCoordinator) scheduleLocked() {
	for c.active < c.capacity && len(c.queued) > 0 {
		next := c.nextLocked()
		if next < 0 {
			return
		}
		work := c.queued[next]
		c.queued = append(c.queued[:next], c.queued[next+1:]...)
		c.active++
		c.activeWorks[work] = struct{}{}
		c.byFolder[work.request.Folder.ID]++
		work.activeShare = work.shareKey()
		c.activeByShare[work.activeShare]++
		work.activeBytes = max(work.request.Work.NextHashingQuantumBytes(), 0)
		c.activeBytes[work.activeShare] += work.activeBytes
		if !work.wasAdmitted {
			work.wasAdmitted = true
			close(work.admitted)
		}
		go c.runQuantum(work)
	}
}

func (c *sourceHashCoordinator) nextLocked() int {
	next := -1
	for i := range c.queued {
		request := c.queued[i].request
		if request.Folder.HasherCeiling > 0 && c.byFolder[request.Folder.ID] >= request.Folder.HasherCeiling {
			continue
		}
		if next < 0 || request.Folder.Priority > c.queued[next].request.Folder.Priority ||
			(request.Folder.Priority == c.queued[next].request.Folder.Priority &&
				c.fairnessScoreLocked(c.queued[i].shareKey()) < c.fairnessScoreLocked(c.queued[next].shareKey())) {
			next = i
		}
	}
	return next
}

func (c *sourceHashCoordinator) initializeFairnessLocked(shareKey equalPriorityShareKey) {
	if c.participatingLocked(shareKey) {
		return
	}
	var minimum int64
	found := false
	for candidate := range c.charged {
		if candidate.priority != shareKey.priority || !c.participatingLocked(candidate) {
			continue
		}
		bytes := c.fairnessScoreLocked(candidate)
		if !found || bytes < minimum {
			minimum = bytes
			found = true
		}
	}
	c.charged[shareKey] = minimum
}

func (c *sourceHashCoordinator) minimumFairnessLocked(priority int, excludedFolders map[string]struct{}) int64 {
	var minimum int64
	found := false
	for candidate := range c.charged {
		if candidate.priority != priority || !c.participatingLocked(candidate) {
			continue
		}
		if _, excluded := excludedFolders[candidate.folder]; excluded {
			continue
		}
		bytes := c.fairnessScoreLocked(candidate)
		if !found || bytes < minimum {
			minimum = bytes
			found = true
		}
	}
	return minimum
}

func (c *sourceHashCoordinator) fairnessScoreLocked(shareKey equalPriorityShareKey) int64 {
	return c.charged[shareKey] + c.activeBytes[shareKey]
}

func (c *sourceHashCoordinator) participatingLocked(shareKey equalPriorityShareKey) bool {
	if c.epochs[shareKey.folder] > 0 && c.epochPriority[shareKey.folder] == shareKey.priority {
		return true
	}
	if c.activeByShare[shareKey] > 0 {
		return true
	}
	for _, work := range c.queued {
		if work.shareKey() == shareKey {
			return true
		}
	}
	return false
}

func (c *sourceHashCoordinator) runQuantum(work *coordinatedSourceHashWork) {
	result, err := work.request.Work.HashNext(work.ctx)

	c.mut.Lock()
	work.bytes += result.Bytes
	c.charged[work.activeShare] += result.Bytes
	c.activeBytes[work.activeShare] -= work.activeBytes
	work.activeBytes = 0
	if work.canceled {
		c.mut.Unlock()
		work.request.Work.Close()
		c.mut.Lock()
		c.releaseActiveLocked(work)
		c.scheduleLocked()
		c.mut.Unlock()
		completeCanceledSourceHashWork(work)
		return
	}
	c.releaseActiveLocked(work)
	if !work.canceled && err == nil && !result.Done {
		c.queued = append(c.queued, work)
		c.scheduleLocked()
		c.mut.Unlock()
		return
	}
	c.scheduleLocked()
	c.mut.Unlock()

	work.request.Work.Close()
	work.completion <- SourceHashCompletion{
		File:  result.File,
		Bytes: work.bytes,
		Err:   err,
	}
	close(work.completion)
}

func (c *sourceHashCoordinator) releaseActiveLocked(work *coordinatedSourceHashWork) {
	c.active--
	delete(c.activeWorks, work)
	c.byFolder[work.request.Folder.ID]--
	c.activeByShare[work.activeShare]--
	work.activeShare = equalPriorityShareKey{}
}

func (w *coordinatedSourceHashWork) shareKey() equalPriorityShareKey {
	return equalPriorityShareKey{
		priority: w.request.Folder.Priority,
		folder:   w.request.Folder.ID,
	}
}
