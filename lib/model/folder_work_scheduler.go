// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"sync"
)

// folderWorkScheduler limits concurrent folder I/O. When Network Priority is
// enabled, queued pulls and scans are admitted by Network Priority. Work that
// does not enable network traffic, such as version cleanup, keeps its arrival
// order without participating in Network Priority.
type folderWorkScheduler struct {
	mut sync.Mutex

	enabled    bool
	capacity   int
	active     int
	priorities map[string]int
	waiters    []*folderWorkWaiter
}

type folderWorkWaiter struct {
	folder   string
	class    folderWorkClass
	ready    chan struct{}
	admitted bool
}

type folderWorkClass uint8

const (
	folderWorkNetwork folderWorkClass = iota
	folderWorkMaintenance
)

func newFolderWorkScheduler() *folderWorkScheduler {
	return &folderWorkScheduler{
		priorities: make(map[string]int),
	}
}

func (s *folderWorkScheduler) configure(enabled bool, capacity int, priorities map[string]int) {
	if capacity < 0 {
		capacity = 0
	}
	s.mut.Lock()
	s.enabled = enabled
	s.capacity = capacity
	s.priorities = priorities
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *folderWorkScheduler) takeWithContext(ctx context.Context, folder string, class folderWorkClass) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	waiter := &folderWorkWaiter{
		folder: folder,
		class:  class,
		ready:  make(chan struct{}),
	}
	s.mut.Lock()
	s.waiters = append(s.waiters, waiter)
	s.scheduleLocked()
	admitted := waiter.admitted
	s.mut.Unlock()
	if admitted {
		return nil
	}

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		s.mut.Lock()
		if waiter.admitted {
			s.mut.Unlock()
			return nil
		}
		for i, queued := range s.waiters {
			if queued == waiter {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}
		s.scheduleLocked()
		s.mut.Unlock()
		return ctx.Err()
	}
}

func (s *folderWorkScheduler) give() {
	s.mut.Lock()
	if s.active > 0 {
		s.active--
	}
	s.scheduleLocked()
	s.mut.Unlock()
}

func (s *folderWorkScheduler) scheduleLocked() {
	for len(s.waiters) > 0 && (s.capacity == 0 || s.active < s.capacity) {
		next := s.nextWaiterLocked()
		waiter := s.waiters[next]
		s.waiters = append(s.waiters[:next], s.waiters[next+1:]...)
		waiter.admitted = true
		s.active++
		close(waiter.ready)
	}
}

func (s *folderWorkScheduler) nextWaiterLocked() int {
	if !s.enabled {
		return 0
	}

	// Already-waiting maintenance retains its opportunity, but once network
	// work is at the front, all queued network work participates in strict
	// Network Priority regardless of maintenance arrival positions.
	if s.waiters[0].class == folderWorkMaintenance {
		return 0
	}

	next := 0
	nextPriority := s.priorities[s.waiters[0].folder]
	for i := 1; i < len(s.waiters); i++ {
		if s.waiters[i].class == folderWorkMaintenance {
			continue
		}
		priority := s.priorities[s.waiters[i].folder]
		if priority > nextPriority {
			next = i
			nextPriority = priority
		}
	}
	return next
}
