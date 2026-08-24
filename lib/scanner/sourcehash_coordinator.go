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

// SourceHashCoordinator admits Source Hash Work against one node-wide Hash
// Capacity pool. Submit synchronously enrolls work and transfers its ownership
// to the coordinator. Completion, continuation eligibility, slot release, and
// replacement selection are serialized as one transition.
type SourceHashCoordinator interface {
	Submit(context.Context, SourceHashRequest) SourceHashSubmission
}

type sourceHashCoordinator struct {
	mut sync.Mutex

	capacity int
	active   int
	byFolder map[string]int
	queued   []*coordinatedSourceHashWork
}

type coordinatedSourceHashWork struct {
	request     SourceHashRequest
	ctx         context.Context
	completion  chan SourceHashCompletion
	admitted    chan struct{}
	wasAdmitted bool
	bytes       int64
}

// NewSourceHashCoordinator returns a coordinator with the given positive Hash
// Capacity.
func NewSourceHashCoordinator(capacity int) SourceHashCoordinator {
	if capacity < 1 {
		panic("Hash Capacity must be positive")
	}
	return &sourceHashCoordinator{
		capacity: capacity,
		byFolder: make(map[string]int),
	}
}

func (c *sourceHashCoordinator) Submit(ctx context.Context, request SourceHashRequest) SourceHashSubmission {
	work := &coordinatedSourceHashWork{
		request:    request,
		ctx:        ctx,
		completion: make(chan SourceHashCompletion, 1),
		admitted:   make(chan struct{}),
	}
	c.mut.Lock()
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
		if next < 0 || request.Folder.Priority > c.queued[next].request.Folder.Priority {
			next = i
		}
	}
	return next
}

func (c *sourceHashCoordinator) runQuantum(work *coordinatedSourceHashWork) {
	result, err := work.request.Work.HashNext(work.ctx)

	c.mut.Lock()
	work.bytes += result.Bytes
	c.active--
	c.byFolder[work.request.Folder.ID]--
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
