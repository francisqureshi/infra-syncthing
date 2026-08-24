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
	BeginSourceHashEpoch(SourceHashFolder) SourceHashEpoch
	Submit(context.Context, SourceHashRequest) SourceHashSubmission
}

type sourceHashCoordinator struct {
	mut sync.Mutex

	capacity      int
	active        int
	byFolder      map[string]int
	charged       map[equalPriorityShareKey]int64
	activeBytes   map[equalPriorityShareKey]int64
	activeByShare map[equalPriorityShareKey]int
	epochs        map[equalPriorityShareKey]int
	queued        []*coordinatedSourceHashWork
}

type equalPriorityShareKey struct {
	priority int
	folder   string
}

type sourceHashEpoch struct {
	coordinator *sourceHashCoordinator
	account     equalPriorityShareKey
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
}

// NewSourceHashCoordinator returns a coordinator with the given positive Hash
// Capacity.
func NewSourceHashCoordinator(capacity int) SourceHashCoordinator {
	if capacity < 1 {
		panic("Hash Capacity must be positive")
	}
	return &sourceHashCoordinator{
		capacity:      capacity,
		byFolder:      make(map[string]int),
		charged:       make(map[equalPriorityShareKey]int64),
		activeBytes:   make(map[equalPriorityShareKey]int64),
		activeByShare: make(map[equalPriorityShareKey]int),
		epochs:        make(map[equalPriorityShareKey]int),
	}
}

func (c *sourceHashCoordinator) BeginSourceHashEpoch(folder SourceHashFolder) SourceHashEpoch {
	account := equalPriorityShareKey{priority: folder.Priority, folder: folder.ID}
	c.mut.Lock()
	c.initializeFairnessLocked(account)
	c.epochs[account]++
	c.mut.Unlock()
	return &sourceHashEpoch{
		coordinator: c,
		account:     account,
	}
}

func (e *sourceHashEpoch) Close() {
	e.once.Do(func() {
		e.coordinator.mut.Lock()
		e.coordinator.epochs[e.account]--
		if e.coordinator.epochs[e.account] == 0 {
			delete(e.coordinator.epochs, e.account)
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
	c.initializeFairnessLocked(work.account())
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
		c.byFolder[work.request.Folder.ID]++
		c.activeByShare[work.account()]++
		work.activeBytes = max(work.request.Work.NextHashingQuantumBytes(), 0)
		c.activeBytes[work.account()] += work.activeBytes
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
				c.fairnessScoreLocked(c.queued[i].account()) < c.fairnessScoreLocked(c.queued[next].account())) {
			next = i
		}
	}
	return next
}

func (c *sourceHashCoordinator) initializeFairnessLocked(account equalPriorityShareKey) {
	if c.participatingLocked(account) {
		return
	}
	var minimum int64
	found := false
	for candidate := range c.charged {
		if candidate.priority != account.priority || !c.participatingLocked(candidate) {
			continue
		}
		bytes := c.fairnessScoreLocked(candidate)
		if !found || bytes < minimum {
			minimum = bytes
			found = true
		}
	}
	c.charged[account] = minimum
}

func (c *sourceHashCoordinator) fairnessScoreLocked(account equalPriorityShareKey) int64 {
	return c.charged[account] + c.activeBytes[account]
}

func (c *sourceHashCoordinator) participatingLocked(account equalPriorityShareKey) bool {
	if c.epochs[account] > 0 || c.activeByShare[account] > 0 {
		return true
	}
	for _, work := range c.queued {
		if work.account() == account {
			return true
		}
	}
	return false
}

func (c *sourceHashCoordinator) runQuantum(work *coordinatedSourceHashWork) {
	result, err := work.request.Work.HashNext(work.ctx)

	c.mut.Lock()
	work.bytes += result.Bytes
	c.charged[work.account()] += result.Bytes
	c.activeBytes[work.account()] -= work.activeBytes
	work.activeBytes = 0
	c.active--
	c.byFolder[work.request.Folder.ID]--
	c.activeByShare[work.account()]--
	if err == nil && !result.Done {
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

func (w *coordinatedSourceHashWork) account() equalPriorityShareKey {
	return equalPriorityShareKey{
		priority: w.request.Folder.Priority,
		folder:   w.request.Folder.ID,
	}
}
