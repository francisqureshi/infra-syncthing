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

type sourceHashWork interface {
	HashNext(context.Context) (HashingQuantumResult, error)
	Close()
}

type sourceHashRequest struct {
	folder   string
	priority int
	ceiling  int
	work     sourceHashWork
}

type sourceHashCompletion struct {
	file  protocol.FileInfo
	bytes int64
	err   error
}

// SourceHashCoordinator admits Source Hash Work against one node-wide Hash
// Capacity pool. Completion, continuation eligibility, slot release, and
// replacement selection are serialized as one transition.
type SourceHashCoordinator struct {
	mut sync.Mutex

	capacity int
	active   int
	byFolder map[string]int
	queued   []*coordinatedSourceHashWork
}

type coordinatedSourceHashWork struct {
	request    sourceHashRequest
	ctx        context.Context
	completion chan sourceHashCompletion
	bytes      int64
}

// NewSourceHashCoordinator returns a coordinator with the given positive Hash
// Capacity.
func NewSourceHashCoordinator(capacity int) *SourceHashCoordinator {
	if capacity < 1 {
		panic("Hash Capacity must be positive")
	}
	return &SourceHashCoordinator{
		capacity: capacity,
		byFolder: make(map[string]int),
	}
}

func (c *SourceHashCoordinator) submit(ctx context.Context, request sourceHashRequest) <-chan sourceHashCompletion {
	work := &coordinatedSourceHashWork{
		request:    request,
		ctx:        ctx,
		completion: make(chan sourceHashCompletion, 1),
	}
	c.mut.Lock()
	c.queued = append(c.queued, work)
	c.scheduleLocked()
	c.mut.Unlock()
	return work.completion
}

func (c *SourceHashCoordinator) scheduleLocked() {
	for c.active < c.capacity && len(c.queued) > 0 {
		next := c.nextLocked()
		if next < 0 {
			return
		}
		work := c.queued[next]
		c.queued = append(c.queued[:next], c.queued[next+1:]...)
		c.active++
		c.byFolder[work.request.folder]++
		go c.runQuantum(work)
	}
}

func (c *SourceHashCoordinator) nextLocked() int {
	next := -1
	for i := range c.queued {
		request := c.queued[i].request
		if request.ceiling > 0 && c.byFolder[request.folder] >= request.ceiling {
			continue
		}
		if next < 0 || request.priority > c.queued[next].request.priority {
			next = i
		}
	}
	return next
}

func (c *SourceHashCoordinator) runQuantum(work *coordinatedSourceHashWork) {
	result, err := work.request.work.HashNext(work.ctx)

	c.mut.Lock()
	work.bytes += result.Bytes
	c.active--
	c.byFolder[work.request.folder]--
	if err == nil && !result.Done {
		c.queued = append(c.queued, work)
		c.scheduleLocked()
		c.mut.Unlock()
		return
	}
	c.scheduleLocked()
	c.mut.Unlock()

	work.request.work.Close()
	work.completion <- sourceHashCompletion{
		file:  result.File,
		bytes: work.bytes,
		err:   err,
	}
	close(work.completion)
}
