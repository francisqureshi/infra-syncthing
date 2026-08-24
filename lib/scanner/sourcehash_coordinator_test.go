// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

func TestSourceHashCoordinatorSerializesContinuationWithSlotReplacement(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 3)
	highFirst := make(chan struct{})
	highSecond := make(chan struct{})
	lowOnly := make(chan struct{})

	highResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "high",
		priority: 100,
		work: &controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "high-1", release: highFirst, bytes: 4},
				{label: "high-2", release: highSecond, bytes: 3, done: true},
			},
		},
	})
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("first admission = %q, want high-1", got)
	}

	lowResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "low",
		priority: 0,
		work: &controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "low-1", release: lowOnly, bytes: 2, done: true},
			},
		},
	})

	close(highFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-2" {
		t.Fatalf("replacement admission = %q, want ready high continuation", got)
	}

	close(highSecond)
	if result := awaitCoordinatorResult(t, highResult); result.err != nil || result.bytes != 7 {
		t.Fatalf("high result = %+v, want 7 consumed bytes and no error", result)
	}
	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("admission after high completion = %q, want low-1", got)
	}
	close(lowOnly)
	if result := awaitCoordinatorResult(t, lowResult); result.err != nil || result.bytes != 2 {
		t.Fatalf("low result = %+v, want 2 consumed bytes and no error", result)
	}
}

func TestSourceHashCoordinatorAdmitsNewHigherPriorityWorkAtQuantumBoundary(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 3)
	lowFirst := make(chan struct{})
	lowSecond := make(chan struct{})
	highOnly := make(chan struct{})

	lowResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "low",
		priority: 0,
		work: &controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "low-1", release: lowFirst, bytes: 4},
				{label: "low-2", release: lowSecond, bytes: 3, done: true},
			},
		},
	})
	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("active admission = %q, want low-1", got)
	}
	highResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "high",
		priority: 100,
		work:     newSingleQuantumCoordinatorWork(started, "high-1", highOnly, 2),
	})

	close(lowFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("boundary admission = %q, want newly runnable high-1", got)
	}
	close(highOnly)
	if completed := awaitCoordinatorResult(t, highResult); completed.err != nil {
		t.Fatal(completed.err)
	}
	if got := awaitCoordinatorStart(t, started); got != "low-2" {
		t.Fatalf("admission after High drains = %q, want low-2", got)
	}
	close(lowSecond)
	if completed := awaitCoordinatorResult(t, lowResult); completed.err != nil || completed.bytes != 7 {
		t.Fatalf("Low completion = %+v, want 7 consumed bytes and no error", completed)
	}
}

func TestSourceHashCoordinatorUsesSpareCapacityAroundFolderCeiling(t *testing.T) {
	coordinator := NewSourceHashCoordinator(2)
	started := make(chan string, 3)
	highFirst := make(chan struct{})
	highSecond := make(chan struct{})
	lowOnly := make(chan struct{})

	highFirstResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "high",
		priority: 100,
		ceiling:  1,
		work:     newSingleQuantumCoordinatorWork(started, "high-1", highFirst, 4),
	})
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("first admission = %q, want high-1", got)
	}
	highSecondResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "high",
		priority: 100,
		ceiling:  1,
		work:     newSingleQuantumCoordinatorWork(started, "high-2", highSecond, 4),
	})
	lowResult := coordinator.submit(t.Context(), sourceHashRequest{
		folder:   "low",
		priority: 0,
		work:     newSingleQuantumCoordinatorWork(started, "low-1", lowOnly, 2),
	})

	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("spare-slot admission = %q, want low-1", got)
	}
	close(highFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-2" {
		t.Fatalf("admission after ceiling allows work = %q, want high-2", got)
	}
	close(highSecond)
	close(lowOnly)

	for description, result := range map[string]<-chan sourceHashCompletion{
		"first high":  highFirstResult,
		"second high": highSecondResult,
		"low":         lowResult,
	} {
		if completed := awaitCoordinatorResult(t, result); completed.err != nil {
			t.Fatalf("%s result: %v", description, completed.err)
		}
	}
}

type controlledCoordinatorQuantum struct {
	label   string
	release <-chan struct{}
	bytes   int64
	done    bool
}

type controlledCoordinatorWork struct {
	started chan<- string
	quanta  []controlledCoordinatorQuantum
	next    int
}

func newSingleQuantumCoordinatorWork(started chan<- string, label string, release <-chan struct{}, bytes int64) *controlledCoordinatorWork {
	return &controlledCoordinatorWork{
		started: started,
		quanta: []controlledCoordinatorQuantum{
			{label: label, release: release, bytes: bytes, done: true},
		},
	}
}

func (w *controlledCoordinatorWork) HashNext(ctx context.Context) (HashingQuantumResult, error) {
	quantum := w.quanta[w.next]
	w.next++
	select {
	case w.started <- quantum.label:
	case <-ctx.Done():
		return HashingQuantumResult{}, ctx.Err()
	}
	<-quantum.release
	return HashingQuantumResult{
		Bytes: quantum.bytes,
		Done:  quantum.done,
		File:  protocol.FileInfo{Name: quantum.label},
	}, nil
}

func (*controlledCoordinatorWork) Close() {}

func awaitCoordinatorStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case label := <-started:
		return label
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Hashing Quantum admission")
		return ""
	}
}

func awaitCoordinatorResult(t *testing.T, result <-chan sourceHashCompletion) sourceHashCompletion {
	t.Helper()
	select {
	case completed := <-result:
		return completed
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Source Hash Work completion")
		return sourceHashCompletion{}
	}
}
