// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

func TestSourceHashCoordinatorReportsCurrentState(t *testing.T) {
	now := time.Unix(1_000, 0)
	coordinator := NewSourceHashCoordinator(1)
	coordinator.(*sourceHashCoordinator).now = func() time.Time { return now }
	provider, ok := coordinator.(SourceHashWorkStateProvider)
	if !ok {
		t.Fatal("Source Hash Work coordinator does not expose stable current state")
	}
	started := make(chan string, 2)
	activeRelease := make(chan struct{})
	queuedRelease := make(chan struct{})
	activeWork := newRetainedCoordinatorWork(newSingleQuantumCoordinatorWork(started, "active", activeRelease, 1))
	queuedWork := newRetainedCoordinatorWork(newSingleQuantumCoordinatorWork(started, "queued", queuedRelease, 1))

	activeResult := coordinator.Submit(t.Context(), coordinatorRequest("alpha", 0, 0, activeWork)).Completion
	if got := awaitCoordinatorStart(t, started); got != "active" {
		t.Fatalf("first admission = %q, want active", got)
	}
	queuedResult := coordinator.Submit(t.Context(), coordinatorRequest("alpha", 0, 0, queuedWork)).Completion
	now = now.Add(9 * time.Second)

	if got, want := provider.SourceHashWorkState("alpha"), (SourceHashWorkState{
		Queued:                      1,
		Active:                      1,
		OldestSchedulingWaitSeconds: 9,
		HashCapacity:                1,
		RetainedHandles:             1,
		RetainedHandleBudget:        4,
	}); got != want {
		t.Fatalf("active Source Hash Work state = %#v, want %#v", got, want)
	}

	close(activeRelease)
	if completed := awaitCoordinatorResult(t, activeResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "queued" {
		t.Fatalf("second admission = %q, want queued", got)
	}
	if got := provider.SourceHashWorkState("alpha"); got.Queued != 0 || got.Active != 1 || got.OldestSchedulingWaitSeconds != 0 || got.RetainedHandles != 1 {
		t.Fatalf("Source Hash Work state retained historical wait after admission: %#v", got)
	}

	close(queuedRelease)
	if completed := awaitCoordinatorResult(t, queuedResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := provider.SourceHashWorkState("alpha"); got.Queued != 0 || got.Active != 0 || got.OldestSchedulingWaitSeconds != 0 || got.RetainedHandles != 0 {
		t.Fatalf("Source Hash Work state retained completed state: %#v", got)
	}

	coordinator.Configure(3, map[string]int{"alpha": 0})
	if got := provider.SourceHashWorkState("alpha"); got.HashCapacity != 3 || got.RetainedHandleBudget != 6 {
		t.Fatalf("Source Hash Work state did not apply live Hash Capacity: %#v", got)
	}
}

func TestSourceHashCoordinatorReportsActualRetainedHandleUsage(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	provider := coordinator.(SourceHashWorkStateProvider)
	started := make(chan string, 1)
	firstRelease := make(chan struct{})
	results, _ := startBoundedSourceHashWalk(t, coordinator, new(sourceHashHandleObserver), "alpha", 0, 2*protocol.MinBlockSize, started,
		[]string{"source"}, []<-chan struct{}{firstRelease})

	if got := awaitCoordinatorStart(t, started); got != "source" {
		t.Fatalf("source open = %q, want source", got)
	}
	if got := provider.SourceHashWorkState("alpha").RetainedHandles; got != 1 {
		t.Fatalf("retained handles during first quantum = %d, want 1", got)
	}

	close(firstRelease)
	for result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if got := provider.SourceHashWorkState("alpha"); got.RetainedHandles != 0 || got.Active != 0 || got.Queued != 0 {
		t.Fatalf("Source Hash Work retained state after drain: %#v", got)
	}
}

func TestSourceHashCoordinatorReportsBoundedEnrollmentBackpressure(t *testing.T) {
	now := time.Unix(1_000, 0)
	coordinator := NewSourceHashCoordinator(1)
	coordinator.(*sourceHashCoordinator).now = func() time.Time { return now }
	provider := coordinator.(SourceHashWorkStateProvider)
	started := make(chan string, 4)
	releases := make([]chan struct{}, 4)
	results := make([]<-chan SourceHashCompletion, 4)
	for i := range 4 {
		releases[i] = make(chan struct{})
		results[i] = coordinator.Submit(t.Context(), coordinatorRequest("alpha", 0, 0,
			newSingleQuantumCoordinatorWork(started, string(rune('a'+i)), releases[i], 1),
		)).Completion
	}
	_ = awaitCoordinatorStart(t, started)

	blockedContext, cancelBlocked := context.WithCancel(t.Context())
	waiting := make(chan struct{})
	observedContext := &observedDoneContext{Context: blockedContext, waiting: waiting}
	returned := make(chan SourceHashSubmission, 1)
	go func() {
		returned <- coordinator.Submit(observedContext, coordinatorRequest("alpha", 0, 0,
			newSingleQuantumCoordinatorWork(started, "blocked", make(chan struct{}), 1),
		))
	}()
	awaitCoordinatorSignal(t, waiting, "bounded enrollment backpressure")
	now = now.Add(5 * time.Second)
	if got := provider.SourceHashWorkState("alpha"); got.Active != 1 || got.Queued != 4 || got.OldestSchedulingWaitSeconds != 5 {
		t.Fatalf("backpressured Source Hash Work state = %#v, want one active and four waiting for five seconds", got)
	}

	cancelBlocked()
	blocked := <-returned
	if completed := awaitCoordinatorResult(t, blocked.Completion); !errors.Is(completed.Err, context.Canceled) {
		t.Fatalf("backpressured completion = %+v, want cancellation", completed)
	}
	for _, release := range releases {
		close(release)
	}
	for _, result := range results {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatal(completed.Err)
		}
	}
	if got := provider.SourceHashWorkState("alpha"); got.Active != 0 || got.Queued != 0 || got.OldestSchedulingWaitSeconds != 0 {
		t.Fatalf("Source Hash Work state after backpressure cleanup = %#v", got)
	}
}

func TestSourceHashCoordinatorSerializesContinuationWithSlotReplacement(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 3)
	highFirst := make(chan struct{})
	highSecond := make(chan struct{})
	lowOnly := make(chan struct{})

	highResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "high-1", release: highFirst, bytes: 4},
				{label: "high-2", release: highSecond, bytes: 3, done: true},
			},
		},
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("first admission = %q, want high-1", got)
	}

	lowResult := coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "low-1", release: lowOnly, bytes: 2, done: true},
			},
		},
	)).Completion

	close(highFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-2" {
		t.Fatalf("replacement admission = %q, want ready high continuation", got)
	}

	close(highSecond)
	if result := awaitCoordinatorResult(t, highResult); result.Err != nil || result.Bytes != 7 {
		t.Fatalf("high result = %+v, want 7 consumed bytes and no error", result)
	}
	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("admission after high completion = %q, want low-1", got)
	}
	close(lowOnly)
	if result := awaitCoordinatorResult(t, lowResult); result.Err != nil || result.Bytes != 2 {
		t.Fatalf("low result = %+v, want 2 consumed bytes and no error", result)
	}
}

func TestSourceHashCoordinatorAdmitsNewHigherPriorityWorkAtQuantumBoundary(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 3)
	lowFirst := make(chan struct{})
	lowSecond := make(chan struct{})
	highOnly := make(chan struct{})

	lowResult := coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "low-1", release: lowFirst, bytes: 4},
				{label: "low-2", release: lowSecond, bytes: 3, done: true},
			},
		},
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("active admission = %q, want low-1", got)
	}
	highResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 0,
		newSingleQuantumCoordinatorWork(started, "high-1", highOnly, 2),
	)).Completion

	close(lowFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("boundary admission = %q, want newly runnable high-1", got)
	}
	close(highOnly)
	if completed := awaitCoordinatorResult(t, highResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "low-2" {
		t.Fatalf("admission after High drains = %q, want low-2", got)
	}
	close(lowSecond)
	if completed := awaitCoordinatorResult(t, lowResult); completed.Err != nil || completed.Bytes != 7 {
		t.Fatalf("Low completion = %+v, want 7 consumed bytes and no error", completed)
	}
}

func TestSourceHashCoordinatorUsesEveryCompatibleSlotByFolderPriority(t *testing.T) {
	t.Run("saturated pool replaces Low with High at every boundary", func(t *testing.T) {
		coordinator := NewSourceHashCoordinator(3)
		started := make(chan string, 12)
		lowFirst := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		lowSecond := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		highOnly := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		lowResults := make([]<-chan SourceHashCompletion, 3)
		highResults := make([]<-chan SourceHashCompletion, 3)

		for index := range lowResults {
			lowResults[index] = coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
				&controlledCoordinatorWork{
					started: started,
					quanta: []controlledCoordinatorQuantum{
						{label: fmt.Sprintf("low-%d-first", index), release: lowFirst[index], bytes: 4},
						{label: fmt.Sprintf("low-%d-second", index), release: lowSecond[index], bytes: 4, done: true},
					},
				},
			)).Completion
			if got, want := awaitCoordinatorStart(t, started), fmt.Sprintf("low-%d-first", index); got != want {
				t.Fatalf("initial saturated admission %d = %q, want %q", index, got, want)
			}
		}
		for index := range highResults {
			highResults[index] = coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 0,
				newSingleQuantumCoordinatorWork(started, fmt.Sprintf("high-%d", index), highOnly[index], 2),
			)).Completion
		}

		for index := range lowFirst {
			close(lowFirst[index])
			if got, want := awaitCoordinatorStart(t, started), fmt.Sprintf("high-%d", index); got != want {
				t.Fatalf("replacement admission %d = %q, want %q", index, got, want)
			}
		}
		for index := range highOnly {
			close(highOnly[index])
			if completed := awaitCoordinatorResult(t, highResults[index]); completed.Err != nil {
				t.Fatalf("High completion %d: %v", index, completed.Err)
			}
		}

		remainingLow := make(map[string]chan struct{}, len(lowSecond))
		for index, release := range lowSecond {
			remainingLow[fmt.Sprintf("low-%d-second", index)] = release
		}
		for range lowSecond {
			label := awaitCoordinatorStart(t, started)
			release, ok := remainingLow[label]
			if !ok {
				t.Fatalf("unexpected post-High admission %q", label)
			}
			close(release)
			delete(remainingLow, label)
		}
		for index, result := range lowResults {
			if completed := awaitCoordinatorResult(t, result); completed.Err != nil || completed.Bytes != 8 {
				t.Fatalf("Low completion %d = %+v, want eight consumed bytes and no error", index, completed)
			}
		}
	})

	t.Run("one sequential High file leaves spare slots to Low", func(t *testing.T) {
		coordinator := NewSourceHashCoordinator(3)
		started := make(chan string, 4)
		highFirst := make(chan struct{})
		highSecond := make(chan struct{})
		lowFirst := make(chan struct{})
		lowSecond := make(chan struct{})

		highResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 0,
			&controlledCoordinatorWork{
				started: started,
				quanta: []controlledCoordinatorQuantum{
					{label: "high-first", release: highFirst, bytes: 4},
					{label: "high-second", release: highSecond, bytes: 4, done: true},
				},
			},
		)).Completion
		if got := awaitCoordinatorStart(t, started); got != "high-first" {
			t.Fatalf("first admission = %q, want high-first", got)
		}
		lowFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
			newSingleQuantumCoordinatorWork(started, "low-first", lowFirst, 2),
		)).Completion
		if got := awaitCoordinatorStart(t, started); got != "low-first" {
			t.Fatalf("first spare-slot admission = %q, want low-first", got)
		}
		lowSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
			newSingleQuantumCoordinatorWork(started, "low-second", lowSecond, 2),
		)).Completion
		if got := awaitCoordinatorStart(t, started); got != "low-second" {
			t.Fatalf("second spare-slot admission = %q, want low-second", got)
		}

		close(highFirst)
		if got := awaitCoordinatorStart(t, started); got != "high-second" {
			t.Fatalf("High continuation admission = %q, want high-second", got)
		}
		close(highSecond)
		close(lowFirst)
		close(lowSecond)

		for description, result := range map[string]<-chan SourceHashCompletion{
			"high":       highResult,
			"first Low":  lowFirstResult,
			"second Low": lowSecondResult,
		} {
			if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
				t.Fatalf("%s completion: %v", description, completed.Err)
			}
		}
	})
}

func TestSourceHashCoordinatorDisplacesRetainedLowerPriorityWorkForHighPriorityAdmission(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 6)
	lowFirst := make(chan struct{})
	lowSecond := make(chan struct{})
	gateRelease := make(chan struct{})
	fillerRelease := make(chan struct{})
	secondFillerRelease := make(chan struct{})
	highRelease := make(chan struct{})
	lowReleased := make(chan struct{})

	lowWork := newRetainedCoordinatorWork(&controlledCoordinatorWork{
		started:  started,
		released: lowReleased,
		quanta: []controlledCoordinatorQuantum{
			{label: "low-1", release: lowFirst, bytes: 4},
			{label: "low-2", release: lowSecond, bytes: 3, done: true},
		},
	})
	lowResult := coordinator.Submit(t.Context(), coordinatorRequest("low", -100, 0, lowWork)).Completion
	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("first admission = %q, want low-1", got)
	}

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 50, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateRelease, 2),
	)).Completion
	close(lowFirst)
	if got := awaitCoordinatorStart(t, started); got != "gate-1" {
		t.Fatalf("boundary admission = %q, want gate-1", got)
	}
	provider := coordinator.(SourceHashWorkStateProvider)
	if got := provider.SourceHashWorkState("low").RetainedHandles; got != 1 {
		t.Fatalf("retained handles before eviction = %d, want 1", got)
	}

	fillerResult := coordinator.Submit(t.Context(), coordinatorRequest("filler", 0, 0,
		newSingleQuantumCoordinatorWork(started, "filler-1", fillerRelease, 2),
	)).Completion
	secondFillerResult := coordinator.Submit(t.Context(), coordinatorRequest("second-filler", 0, 0,
		newSingleQuantumCoordinatorWork(started, "filler-2", secondFillerRelease, 2),
	)).Completion
	highResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 0,
		newSingleQuantumCoordinatorWork(started, "high-1", highRelease, 2),
	)).Completion

	select {
	case completed := <-lowResult:
		if !errors.Is(completed.Err, errSourceHashWorkDisplaced) || completed.Bytes != 4 {
			t.Fatalf("displaced Low completion = %+v, want 4 charged bytes and displacement", completed)
		}
	default:
		t.Fatal("higher-priority admission returned without displacing retained Low work")
	}
	select {
	case <-lowReleased:
	default:
		t.Fatal("higher-priority admission did not release Low's retained source state")
	}
	if got := provider.SourceHashWorkState("low"); got.Queued != 0 || got.Active != 0 || got.RetainedHandles != 0 {
		t.Fatalf("evicted Source Hash Work retained observable state: %#v", got)
	}

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("admission after retained Low displacement = %q, want high-1", got)
	}
	close(highRelease)
	if completed := awaitCoordinatorResult(t, highResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "filler-1" {
		t.Fatalf("remaining admission = %q, want filler-1", got)
	}
	close(fillerRelease)
	if completed := awaitCoordinatorResult(t, fillerResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "filler-2" {
		t.Fatalf("final admission = %q, want filler-2", got)
	}
	close(secondFillerRelease)
	if completed := awaitCoordinatorResult(t, secondFillerResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorBoundsEnrolledFilesByHashCapacityAndLookahead(t *testing.T) {
	coordinator := NewSourceHashCoordinator(2)
	coordinator.Configure(2, map[string]int{"holder": 0, "blocked": 0})
	started := make(chan string, 5)
	releases := make([]chan struct{}, 5)
	results := make([]<-chan SourceHashCompletion, 5)
	for i := range 5 { // Hash Capacity 2 plus the documented three-file lookahead.
		releases[i] = make(chan struct{})
		results[i] = coordinator.Submit(t.Context(), coordinatorRequest("holder", 0, 0,
			newSingleQuantumCoordinatorWork(started, string(rune('a'+i)), releases[i], 1),
		)).Completion
	}
	for range 2 {
		_ = awaitCoordinatorStart(t, started)
	}

	waiting := make(chan struct{})
	blockedContext := &observedDoneContext{Context: t.Context(), waiting: waiting}
	blockedClosed := make(chan struct{})
	blockedWork := newSingleQuantumCoordinatorWork(started, "blocked", make(chan struct{}), 1)
	blockedWork.closed = blockedClosed
	returned := make(chan SourceHashSubmission, 1)
	go func() {
		returned <- coordinator.Submit(blockedContext, coordinatorRequest("blocked", 0, 0, blockedWork))
	}()
	awaitCoordinatorSignal(t, waiting, "bounded enrollment backpressure")
	coordinator.Configure(2, map[string]int{"holder": 0})
	blocked := <-returned
	if completed := awaitCoordinatorResult(t, blocked.Completion); !errors.Is(completed.Err, context.Canceled) {
		t.Fatalf("backpressured completion = %+v, want context cancellation", completed)
	}
	select {
	case <-blocked.Admitted:
		t.Fatal("backpressured file was admitted beyond the bounded window")
	default:
	}
	awaitCoordinatorSignal(t, blockedClosed, "backpressured source cleanup")

	for _, release := range releases {
		close(release)
	}
	for _, result := range results {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatal(completed.Err)
		}
	}
}

func TestSourceHashCoordinatorShrinkReleasesRetainedHandlesToNewBound(t *testing.T) {
	coordinator := NewSourceHashCoordinator(3)
	started := make(chan string, 9)
	lowFirst := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	lowSecond := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	lowReleased := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	lowResults := make([]<-chan SourceHashCompletion, 3)
	for i := range 3 {
		work := &controlledCoordinatorWork{
			started:  started,
			released: lowReleased[i],
			quanta: []controlledCoordinatorQuantum{
				{label: string(rune('a'+i)) + "-1", release: lowFirst[i], bytes: 4},
				{label: string(rune('a'+i)) + "-2", release: lowSecond[i], bytes: 4, done: true},
			},
		}
		lowResults[i] = coordinator.Submit(t.Context(), coordinatorRequest(string(rune('a'+i)), -100, 0, work)).Completion
	}
	for range 3 {
		_ = awaitCoordinatorStart(t, started)
	}

	gateReleases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	gateResults := make([]<-chan SourceHashCompletion, 3)
	for i := range 3 {
		label := string(rune('x' + i))
		gateResults[i] = coordinator.Submit(t.Context(), coordinatorRequest(label, 100, 0,
			newSingleQuantumCoordinatorWork(started, label, gateReleases[i], 1),
		)).Completion
	}
	for _, release := range lowFirst {
		close(release)
	}
	for range 3 {
		_ = awaitCoordinatorStart(t, started)
	}

	coordinator.Configure(1, map[string]int{
		"a": -100, "b": -100, "c": -100,
		"x": 100, "y": 100, "z": 100,
	})
	pendingReleases := append([]chan struct{}(nil), lowReleased...)
	displaced := make(map[int]struct{}, 2)
	for range 2 {
		var index int
		select {
		case <-pendingReleases[0]:
			index = 0
		case <-pendingReleases[1]:
			index = 1
		case <-pendingReleases[2]:
			index = 2
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for retained handle release after Hash Capacity shrink")
		}
		pendingReleases[index] = nil
		displaced[index] = struct{}{}
		if completed := awaitCoordinatorResult(t, lowResults[index]); !errors.Is(completed.Err, errSourceHashWorkDisplaced) || completed.Bytes != 4 {
			t.Fatalf("displaced retained completion %d = %+v, want four bytes and displacement", index, completed)
		}
	}
	remaining := 0
	for i := range 3 {
		if _, ok := displaced[i]; !ok {
			remaining = i
		}
	}
	select {
	case <-lowReleased[remaining]:
		t.Fatal("Hash Capacity shrink released a retained handle inside the new capacity-plus-lookahead bound")
	default:
	}

	for _, release := range gateReleases {
		close(release)
	}
	for _, result := range gateResults {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatal(completed.Err)
		}
	}
	wantContinuation := string(rune('a'+remaining)) + "-2"
	if got := awaitCoordinatorStart(t, started); got != wantContinuation {
		t.Fatalf("retained continuation after shrink = %q, want %s", got, wantContinuation)
	}
	close(lowSecond[remaining])
	if completed := awaitCoordinatorResult(t, lowResults[remaining]); completed.Err != nil || completed.Bytes != 8 {
		t.Fatalf("retained completion after shrink = %+v, want eight bytes and success", completed)
	}
}

func TestWalkRestartsDisplacedSourceHashWorkAfterFreshOpen(t *testing.T) {
	coordinator := &sourceHashEnrollmentObserver{
		SourceHashCoordinator: NewSourceHashCoordinator(1),
		enrolled:              make(chan string, 32),
	}
	started := make(chan string, 32)
	handles := new(sourceHashHandleObserver)
	lowInitialRelease := make(chan struct{})
	lowRestartRelease := make(chan struct{})
	gateRelease := make(chan struct{})
	fillerRelease := make(chan struct{})
	secondFillerRelease := make(chan struct{})
	highRelease := make(chan struct{})

	lowResults, lowFilesystem := startBoundedSourceHashWalk(t, coordinator, handles, "low", -100, 2*protocol.MinBlockSize, started,
		[]string{"low-initial", "low-restarted"}, []<-chan struct{}{lowInitialRelease, lowRestartRelease})
	if got := awaitCoordinatorStart(t, started); got != "low-initial" {
		t.Fatalf("first source open = %q, want low-initial", got)
	}
	if got := awaitCoordinatorStart(t, coordinator.enrolled); got != "low" {
		t.Fatalf("first enrollment = %q, want low", got)
	}

	gateResults, _ := startBoundedSourceHashWalk(t, coordinator, handles, "gate", 50, protocol.MinBlockSize, started,
		[]string{"gate"}, []<-chan struct{}{gateRelease})
	if got := awaitCoordinatorStart(t, coordinator.enrolled); got != "gate" {
		t.Fatalf("second enrollment = %q, want gate", got)
	}
	close(lowInitialRelease)
	if got := awaitCoordinatorStart(t, started); got != "gate" {
		t.Fatalf("boundary source open = %q, want gate", got)
	}

	fillerResults, _ := startBoundedSourceHashWalk(t, coordinator, handles, "filler", 0, protocol.MinBlockSize, started,
		[]string{"filler"}, []<-chan struct{}{fillerRelease})
	if got := awaitCoordinatorStart(t, coordinator.enrolled); got != "filler" {
		t.Fatalf("third enrollment = %q, want filler", got)
	}
	secondFillerResults, _ := startBoundedSourceHashWalk(t, coordinator, handles, "second-filler", 0, protocol.MinBlockSize, started,
		[]string{"second-filler"}, []<-chan struct{}{secondFillerRelease})
	if got := awaitCoordinatorStart(t, coordinator.enrolled); got != "second-filler" {
		t.Fatalf("fourth enrollment = %q, want second-filler", got)
	}

	backlogResults := make(map[string]<-chan ScanResult, 7)
	backlogReleases := make(map[string]chan struct{}, 7)
	for i := range 7 {
		folder := fmt.Sprintf("backlog-%d", i)
		release := make(chan struct{})
		backlogReleases[folder] = release
		results, _ := startBoundedSourceHashWalk(t, coordinator, handles, folder, -100, protocol.MinBlockSize, started,
			[]string{folder}, []<-chan struct{}{release})
		backlogResults[folder] = results
	}

	highResults, _ := startBoundedSourceHashWalk(t, coordinator, handles, "high", 100, protocol.MinBlockSize, started,
		[]string{"high"}, []<-chan struct{}{highRelease})
	if got := awaitCoordinatorStart(t, coordinator.enrolled); got != "high" {
		t.Fatalf("replacement enrollment = %q, want high", got)
	}
	if got := lowFilesystem.closes.Load(); got != 1 {
		t.Fatalf("Low source closes after displacement = %d, want 1", got)
	}

	close(gateRelease)
	if got := awaitCoordinatorStart(t, started); got != "high" {
		t.Fatalf("source open after gate = %q, want high", got)
	}
	close(highRelease)
	if got := awaitCoordinatorStart(t, started); got != "filler" {
		t.Fatalf("source open after High = %q, want filler", got)
	}
	close(fillerRelease)
	if got := awaitCoordinatorStart(t, started); got != "second-filler" {
		t.Fatalf("source open after filler = %q, want second-filler", got)
	}
	close(secondFillerRelease)

	remainingReleases := make(map[string]chan struct{}, len(backlogReleases)+1)
	remainingReleases["low-restarted"] = lowRestartRelease
	for folder, release := range backlogReleases {
		remainingReleases[folder] = release
	}
	for len(remainingReleases) > 0 {
		label := awaitCoordinatorStart(t, started)
		release, ok := remainingReleases[label]
		if !ok {
			t.Fatalf("unexpected bounded-window stress admission %q", label)
		}
		close(release)
		delete(remainingReleases, label)
	}

	reenrolled := make(map[string]struct{}, len(backlogResults)+1)
	for range len(backlogResults) + 1 {
		folder := awaitCoordinatorStart(t, coordinator.enrolled)
		if folder != "low" && !strings.HasPrefix(folder, "backlog-") {
			t.Fatalf("unexpected bounded-window stress enrollment %q", folder)
		}
		if _, ok := reenrolled[folder]; ok {
			t.Fatalf("duplicate bounded-window stress enrollment %q", folder)
		}
		reenrolled[folder] = struct{}{}
	}

	allResults := map[string]<-chan ScanResult{
		"low":           lowResults,
		"gate":          gateResults,
		"filler":        fillerResults,
		"second-filler": secondFillerResults,
		"high":          highResults,
	}
	for folder, results := range backlogResults {
		allResults[folder] = results
	}
	for folder, results := range allResults {
		var completed protocol.FileInfo
		for result := range results {
			if result.Err != nil {
				t.Fatalf("%s result error: %v", folder, result.Err)
			}
			if result.File.Name == "payload" {
				completed = result.File
			}
		}
		if len(completed.Blocks) == 0 || len(completed.BlocksHash) == 0 {
			t.Errorf("%s did not publish a complete file: %+v", folder, completed)
		}
	}
	if got := lowFilesystem.opens.Load(); got != 2 {
		t.Errorf("Low source opens = %d, want initial plus fresh reopen", got)
	}
	if got := lowFilesystem.closes.Load(); got != 2 {
		t.Errorf("Low source closes = %d, want displaced plus completed handles", got)
	}
	if got := handles.opens.Load(); got != 13 {
		t.Errorf("node-wide source opens = %d, want one per file plus Low's fresh reopen", got)
	}
	if got := handles.closes.Load(); got != 13 {
		t.Errorf("node-wide source closes = %d, want every opened handle closed", got)
	}
	if got := handles.current.Load(); got != 0 {
		t.Errorf("node-wide retained handles after completion = %d, want zero", got)
	}
	const handleBudget = 4 // Hash Capacity one plus the documented three-file lookahead.
	if got := handles.peak.Load(); got > handleBudget {
		t.Errorf("node-wide peak handles = %d, want at most Hash Capacity plus lookahead %d", got, handleBudget)
	} else if got < 2 {
		t.Errorf("node-wide peak handles = %d, want active plus retained overlap", got)
	}
}

func TestSourceHashCoordinatorReordersQueuedSourceHashWorkAfterLiveFolderPriorityChange(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{
		"active": 0,
		"bulk":   0,
		"focus":  -100,
	})
	started := make(chan string, 3)
	activeRelease := make(chan struct{})
	bulkRelease := make(chan struct{})
	focusRelease := make(chan struct{})

	activeResult := coordinator.Submit(t.Context(), coordinatorRequest("active", 0, 0,
		newSingleQuantumCoordinatorWork(started, "active-1", activeRelease, 4),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "active-1" {
		t.Fatalf("active admission = %q, want active-1", got)
	}
	bulkResult := coordinator.Submit(t.Context(), coordinatorRequest("bulk", 0, 0,
		newSingleQuantumCoordinatorWork(started, "bulk-1", bulkRelease, 2),
	)).Completion
	focusResult := coordinator.Submit(t.Context(), coordinatorRequest("focus", -100, 0,
		newSingleQuantumCoordinatorWork(started, "focus-1", focusRelease, 2),
	)).Completion

	coordinator.Configure(1, map[string]int{
		"active": 0,
		"bulk":   0,
		"focus":  100,
	})
	select {
	case got := <-started:
		t.Fatalf("priority change preempted active quantum with %q", got)
	default:
	}

	close(activeRelease)
	if completed := awaitCoordinatorResult(t, activeResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "focus-1" {
		t.Fatalf("first admission after reprioritization = %q, want focus-1", got)
	}
	close(focusRelease)
	if completed := awaitCoordinatorResult(t, focusResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "bulk-1" {
		t.Fatalf("remaining admission = %q, want bulk-1", got)
	}
	close(bulkRelease)
	if completed := awaitCoordinatorResult(t, bulkResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorReprioritizedActiveSourceHashWorkRejoinsEqualPriorityShare(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{"a": 0, "b": 0, "gate": 200})
	started := make(chan string, 8)
	aFirstRelease := make(chan struct{})
	aSecondRelease := make(chan struct{})
	bFirstRelease := make(chan struct{})
	aEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "a", Priority: 0})
	bEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "b", Priority: 0})
	t.Cleanup(aEpoch.Close)
	t.Cleanup(bEpoch.Close)

	aResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "a-1", release: aFirstRelease, bytes: 8},
				{label: "a-2", release: aSecondRelease, bytes: 8, done: true},
			},
		},
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("initial admission = %q, want a-1", got)
	}
	bFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-1", bFirstRelease, 4),
	)).Completion

	coordinator.Configure(1, map[string]int{"a": 100, "b": 0, "gate": 200})
	close(aFirstRelease)
	if got := awaitCoordinatorStart(t, started); got != "a-2" {
		t.Fatalf("continuation after reprioritization = %q, want a-2", got)
	}
	close(aSecondRelease)
	if completed := awaitCoordinatorResult(t, aResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("remaining original-share admission = %q, want b-1", got)
	}
	close(bFirstRelease)
	if completed := awaitCoordinatorResult(t, bFirstResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}

	gateRelease := make(chan struct{})
	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 200, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate-1" {
		t.Fatalf("gate admission = %q, want gate-1", got)
	}
	coordinator.Configure(1, map[string]int{"a": 0, "b": 0, "gate": 200})
	aFreshRelease := make(chan struct{})
	bFreshRelease := make(chan struct{})
	aFreshResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-fresh", aFreshRelease, 2),
	)).Completion
	bFreshResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-fresh", bFreshRelease, 2),
	)).Completion

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "a-fresh" {
		t.Fatalf("first admission after rejoining Equal-Priority Share = %q, want a-fresh", got)
	}
	close(aFreshRelease)
	if completed := awaitCoordinatorResult(t, aFreshResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-fresh" {
		t.Fatalf("remaining fresh admission = %q, want b-fresh", got)
	}
	close(bFreshRelease)
	if completed := awaitCoordinatorResult(t, bFreshResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorFillsLiveHashCapacityGrowthImmediately(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	priorities := map[string]int{"a": 0, "b": 0}
	coordinator.Configure(1, priorities)
	started := make(chan string, 2)
	aRelease := make(chan struct{})
	bRelease := make(chan struct{})

	aResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-1", aRelease, 2),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("first admission = %q, want a-1", got)
	}
	bSubmission := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-1", bRelease, 2),
	))

	coordinator.Configure(2, priorities)
	select {
	case <-bSubmission.Admitted:
	default:
		t.Fatal("Hash Capacity growth returned without filling the new compatible slot")
	}
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("growth admission = %q, want b-1", got)
	}

	close(aRelease)
	close(bRelease)
	for description, result := range map[string]<-chan SourceHashCompletion{
		"A": aResult,
		"B": bSubmission.Completion,
	} {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatalf("%s completion: %v", description, completed.Err)
		}
	}
}

func TestSourceHashCoordinatorCancelsQueuedSourceHashWorkForUnavailableFolder(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{"gate": 100, "victim": 0})
	started := make(chan string, 2)
	gateRelease := make(chan struct{})
	victimRelease := make(chan struct{})
	victimClosed := make(chan struct{})

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateRelease, 2),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate-1" {
		t.Fatalf("gate admission = %q, want gate-1", got)
	}
	victimWork := newSingleQuantumCoordinatorWork(started, "victim-1", victimRelease, 2)
	victimWork.closed = victimClosed
	victimSubmission := coordinator.Submit(t.Context(), coordinatorRequest("victim", 0, 0, victimWork))
	provider := coordinator.(SourceHashWorkStateProvider)
	if got := provider.SourceHashWorkState("victim"); got.Queued != 1 || got.Active != 0 {
		t.Fatalf("queued Source Hash Work state before lifecycle change = %#v", got)
	}

	coordinator.Configure(1, map[string]int{"gate": 100})
	select {
	case completed := <-victimSubmission.Completion:
		if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != 0 {
			t.Fatalf("queued cancellation = %+v, want zero bytes and context cancellation", completed)
		}
	default:
		t.Fatal("Folder lifecycle change returned without canceling queued Source Hash Work")
	}
	select {
	case <-victimSubmission.Admitted:
		t.Fatal("canceled queued Source Hash Work was admitted")
	default:
	}
	select {
	case <-victimClosed:
	default:
		t.Fatal("queued cancellation did not close its source owner")
	}
	if got := provider.SourceHashWorkState("victim"); got.Queued != 0 || got.Active != 0 || got.OldestSchedulingWaitSeconds != 0 {
		t.Fatalf("queued Source Hash Work state after lifecycle cleanup = %#v", got)
	}

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorRejectsNewSourceHashWorkForUnavailableFolder(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{})
	started := make(chan string, 1)
	release := make(chan struct{})
	closed := make(chan struct{})
	work := newSingleQuantumCoordinatorWork(started, "removed-1", release, 2)
	work.closed = closed

	submission := coordinator.Submit(t.Context(), coordinatorRequest("removed", 0, 0, work))
	select {
	case completed := <-submission.Completion:
		if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != 0 {
			t.Fatalf("unavailable Folder submission = %+v, want zero bytes and context cancellation", completed)
		}
	default:
		t.Fatal("unavailable Folder submission was not rejected synchronously")
	}
	select {
	case <-submission.Admitted:
		t.Fatal("unavailable Folder Source Hash Work was admitted")
	default:
	}
	select {
	case <-closed:
	default:
		t.Fatal("unavailable Folder source owner was not closed")
	}
	select {
	case got := <-started:
		t.Fatalf("unavailable Folder started Hashing Quantum %q", got)
	default:
	}
}

func TestSourceHashCoordinatorCleansUpActiveSourceHashWorkAtHashingQuantumBoundary(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{"victim": 0})
	started := make(chan string, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	closed := make(chan struct{})
	work := &controlledCoordinatorWork{
		started: started,
		quanta: []controlledCoordinatorQuantum{
			{label: "victim-1", release: firstRelease, bytes: 5},
			{label: "victim-2", release: secondRelease, bytes: 3, done: true},
		},
		closed: closed,
	}
	submission := coordinator.Submit(t.Context(), coordinatorRequest("victim", 0, 0, work))
	provider := coordinator.(SourceHashWorkStateProvider)
	if got := awaitCoordinatorStart(t, started); got != "victim-1" {
		t.Fatalf("active admission = %q, want victim-1", got)
	}

	coordinator.Configure(1, map[string]int{})
	if got := provider.SourceHashWorkState("victim"); got.Active != 1 || got.Queued != 0 || got.OldestSchedulingWaitSeconds != 0 {
		t.Fatalf("active Source Hash Work state during lifecycle drain = %#v", got)
	}
	select {
	case completed := <-submission.Completion:
		t.Fatalf("active quantum was preempted by lifecycle change: %+v", completed)
	default:
	}
	select {
	case <-closed:
		t.Fatal("active source owner closed before its quantum boundary")
	default:
	}

	close(firstRelease)
	select {
	case completed := <-submission.Completion:
		if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != 5 || completed.File.Name != "" {
			t.Fatalf("active cancellation = %+v, want five charged bytes, no file, and context cancellation", completed)
		}
	case got := <-started:
		t.Fatalf("canceled Source Hash Work continued with %q", got)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for active cancellation cleanup")
	}
	select {
	case <-closed:
	default:
		t.Fatal("active cancellation did not close its source owner")
	}
	if got := provider.SourceHashWorkState("victim"); got.Active != 0 || got.Queued != 0 || got.OldestSchedulingWaitSeconds != 0 {
		t.Fatalf("active Source Hash Work state after lifecycle cleanup = %#v", got)
	}
}

func TestSourceHashCoordinatorCleansUpCanceledOwnerBeforeReplacementAdmission(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	coordinator.Configure(1, map[string]int{"victim": 100, "replacement": 0})
	started := make(chan string, 2)
	victimRelease := make(chan struct{})
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	replacementRelease := make(chan struct{})
	victimWork := &controlledCoordinatorWork{
		started: started,
		quanta: []controlledCoordinatorQuantum{
			{label: "victim-1", release: victimRelease, bytes: 5},
		},
		closeStarted: closeStarted,
		closeRelease: closeRelease,
	}
	victimSubmission := coordinator.Submit(t.Context(), coordinatorRequest("victim", 100, 0, victimWork))
	if got := awaitCoordinatorStart(t, started); got != "victim-1" {
		t.Fatalf("active admission = %q, want victim-1", got)
	}
	replacementSubmission := coordinator.Submit(t.Context(), coordinatorRequest("replacement", 0, 0,
		newSingleQuantumCoordinatorWork(started, "replacement-1", replacementRelease, 2),
	))

	coordinator.Configure(1, map[string]int{"replacement": 0})
	close(victimRelease)
	awaitCoordinatorSignal(t, closeStarted, "canceled source owner cleanup")
	select {
	case <-replacementSubmission.Admitted:
		t.Fatal("replacement admitted before canceled source owner cleanup finished")
	default:
	}

	close(closeRelease)
	if completed := awaitCoordinatorResult(t, victimSubmission.Completion); !errors.Is(completed.Err, context.Canceled) || completed.Bytes != 5 {
		t.Fatalf("canceled completion = %+v, want five bytes and context cancellation", completed)
	}
	awaitCoordinatorSignal(t, replacementSubmission.Admitted, "replacement after canceled owner cleanup")
	if got := awaitCoordinatorStart(t, started); got != "replacement-1" {
		t.Fatalf("replacement admission = %q, want replacement-1", got)
	}
	close(replacementRelease)
	if completed := awaitCoordinatorResult(t, replacementSubmission.Completion); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorDrainsLiveHashCapacityShrinkBeforeReplacement(t *testing.T) {
	coordinator := NewSourceHashCoordinator(2)
	priorities := map[string]int{"a": 0, "b": 0, "c": 0}
	coordinator.Configure(2, priorities)
	started := make(chan string, 3)
	releases := map[string]chan struct{}{
		"a": make(chan struct{}),
		"b": make(chan struct{}),
		"c": make(chan struct{}),
	}

	aResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-1", releases["a"], 2),
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-1", releases["b"], 2),
	)).Completion
	initial := map[string]bool{"a-1": false, "b-1": false}
	for range 2 {
		got := awaitCoordinatorStart(t, started)
		if _, ok := initial[got]; !ok || initial[got] {
			t.Fatalf("unexpected initial admission %q", got)
		}
		initial[got] = true
	}
	cSubmission := coordinator.Submit(t.Context(), coordinatorRequest("c", 0, 0,
		newSingleQuantumCoordinatorWork(started, "c-1", releases["c"], 2),
	))

	coordinator.Configure(1, priorities)
	close(releases["a"])
	if completed := awaitCoordinatorResult(t, aResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	select {
	case <-cSubmission.Admitted:
		t.Fatal("replacement admitted while active usage equaled shrunken Hash Capacity")
	default:
	}

	close(releases["b"])
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	awaitCoordinatorSignal(t, cSubmission.Admitted, "replacement admission after shrink drain")
	if got := awaitCoordinatorStart(t, started); got != "c-1" {
		t.Fatalf("post-drain admission = %q, want c-1", got)
	}
	close(releases["c"])
	if completed := awaitCoordinatorResult(t, cSubmission.Completion); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorUsesSpareCapacityAroundFolderCeiling(t *testing.T) {
	coordinator := NewSourceHashCoordinator(2)
	started := make(chan string, 3)
	highFirst := make(chan struct{})
	highSecond := make(chan struct{})
	lowOnly := make(chan struct{})

	highFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 1,
		newSingleQuantumCoordinatorWork(started, "high-1", highFirst, 4),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "high-1" {
		t.Fatalf("first admission = %q, want high-1", got)
	}
	highSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("high", 100, 1,
		newSingleQuantumCoordinatorWork(started, "high-2", highSecond, 4),
	)).Completion
	lowResult := coordinator.Submit(t.Context(), coordinatorRequest("low", 0, 0,
		newSingleQuantumCoordinatorWork(started, "low-1", lowOnly, 2),
	)).Completion

	if got := awaitCoordinatorStart(t, started); got != "low-1" {
		t.Fatalf("spare-slot admission = %q, want low-1", got)
	}
	close(highFirst)
	if got := awaitCoordinatorStart(t, started); got != "high-2" {
		t.Fatalf("admission after ceiling allows work = %q, want high-2", got)
	}
	close(highSecond)
	close(lowOnly)

	for description, result := range map[string]<-chan SourceHashCompletion{
		"first high":  highFirstResult,
		"second high": highSecondResult,
		"low":         lowResult,
	} {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatalf("%s result: %v", description, completed.Err)
		}
	}
}

func TestSourceHashCoordinatorSharesUnequalQuantaByActualBytes(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 8)
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})
	bThird := make(chan struct{})
	bFourth := make(chan struct{})

	releaseGate := occupyCoordinatorSlot(t, coordinator, started, "gate")

	aResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "a-1", release: aFirst, bytes: 8},
				{label: "a-2", release: aSecond, bytes: 8, done: true},
			},
		},
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "b-1", release: bFirst, bytes: 3},
				{label: "b-2", release: bSecond, bytes: 3},
				{label: "b-3", release: bThird, bytes: 3},
				{label: "b-4", release: bFourth, bytes: 3, done: true},
			},
		},
	)).Completion

	releaseGate()
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("first equal-priority admission = %q, want a-1", got)
	}

	close(aFirst)
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("admission after A consumed 8 bytes = %q, want B", got)
	}
	close(bFirst)
	if got := awaitCoordinatorStart(t, started); got != "b-2" {
		t.Fatalf("admission while B had consumed 3 bytes = %q, want B", got)
	}
	close(bSecond)
	if got := awaitCoordinatorStart(t, started); got != "b-3" {
		t.Fatalf("admission while B had consumed 6 bytes = %q, want B", got)
	}
	close(bThird)
	if got := awaitCoordinatorStart(t, started); got != "a-2" {
		t.Fatalf("admission after B reached 9 bytes = %q, want A", got)
	}

	close(aSecond)
	if completed := awaitCoordinatorResult(t, aResult); completed.Err != nil || completed.Bytes != 16 {
		t.Fatalf("A completion = %+v, want 16 consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-4" {
		t.Fatalf("remaining admission = %q, want b-4", got)
	}
	close(bFourth)
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil || completed.Bytes != 12 {
		t.Fatalf("B completion = %+v, want 12 consumed bytes and no error", completed)
	}
}

func TestSourceHashCoordinatorIncludesActiveBytesInEqualPriorityShare(t *testing.T) {
	coordinator := NewSourceHashCoordinator(2)
	started := make(chan string, 8)
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bOnly := make(chan struct{})

	releaseFirstGate := occupyCoordinatorSlot(t, coordinator, started, "gate-1")
	releaseSecondGate := occupyCoordinatorSlot(t, coordinator, started, "gate-2")

	aFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-1", aFirst, 8),
	)).Completion
	aSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-2", aSecond, 8),
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-1", bOnly, 3),
	)).Completion

	releaseFirstGate()
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("first equal-priority admission = %q, want a-1", got)
	}

	releaseSecondGate()
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("admission while A had 8 active bytes = %q, want b-1", got)
	}

	close(bOnly)
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil || completed.Bytes != 3 {
		t.Fatalf("B completion = %+v, want 3 consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "a-2" {
		t.Fatalf("remaining admission = %q, want a-2", got)
	}
	close(aFirst)
	close(aSecond)
	for description, result := range map[string]<-chan SourceHashCompletion{
		"first A":  aFirstResult,
		"second A": aSecondResult,
	} {
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatalf("%s result: %v", description, completed.Err)
		}
	}
}

func TestSourceHashCoordinatorResetsFairnessOnlyAfterEpochDrain(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 10)
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})

	releaseGate := occupyCoordinatorSlot(t, coordinator, started, "gate-1")

	aEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "a", Priority: 0})
	bEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "b", Priority: 0})
	aFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-1", aFirst, 8),
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "b-1", release: bFirst, bytes: 3},
				{label: "b-2", release: bSecond, bytes: 3, done: true},
			},
		},
	)).Completion

	releaseGate()
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("first epoch admission = %q, want a-1", got)
	}
	close(aFirst)
	if completed := awaitCoordinatorResult(t, aFirstResult); completed.Err != nil || completed.Bytes != 8 {
		t.Fatalf("first A completion = %+v, want 8 consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("admission after first A work = %q, want b-1", got)
	}

	aSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-2", aSecond, 4),
	)).Completion
	close(bFirst)
	if got := awaitCoordinatorStart(t, started); got != "b-2" {
		t.Fatalf("admission while A retained 8 charged bytes = %q, want b-2", got)
	}
	close(bSecond)
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil || completed.Bytes != 6 {
		t.Fatalf("B completion = %+v, want 6 consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "a-2" {
		t.Fatalf("admission after B completion = %q, want a-2", got)
	}
	close(aSecond)
	if completed := awaitCoordinatorResult(t, aSecondResult); completed.Err != nil || completed.Bytes != 4 {
		t.Fatalf("second A completion = %+v, want 4 consumed bytes and no error", completed)
	}
	aEpoch.Close()
	bEpoch.Close()

	releaseSecondGate := occupyCoordinatorSlot(t, coordinator, started, "gate-2")
	aEpoch = coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "a", Priority: 0})
	cEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "c", Priority: 0})
	t.Cleanup(aEpoch.Close)
	t.Cleanup(cEpoch.Close)
	aFreshRelease := make(chan struct{})
	cRelease := make(chan struct{})
	aFreshResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-fresh", aFreshRelease, 2),
	)).Completion
	cResult := coordinator.Submit(t.Context(), coordinatorRequest("c", 0, 0,
		newSingleQuantumCoordinatorWork(started, "c-1", cRelease, 2),
	)).Completion

	releaseSecondGate()
	if got := awaitCoordinatorStart(t, started); got != "a-fresh" {
		t.Fatalf("first admission after drained epoch = %q, want fresh A without inherited debt", got)
	}
	close(aFreshRelease)
	if completed := awaitCoordinatorResult(t, aFreshResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "c-1" {
		t.Fatalf("remaining fresh-epoch admission = %q, want c-1", got)
	}
	close(cRelease)
	if completed := awaitCoordinatorResult(t, cResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorChargesActualShortReadBytes(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 8)
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})

	releaseGate := occupyCoordinatorSlot(t, coordinator, started, "gate")
	aResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "a-short", release: aFirst, expected: 8, bytes: 2},
				{label: "a-final", release: aSecond, expected: 8, bytes: 8, done: true},
			},
		},
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "b-1", release: bFirst, bytes: 4},
				{label: "b-2", release: bSecond, bytes: 4, done: true},
			},
		},
	)).Completion

	releaseGate()
	if got := awaitCoordinatorStart(t, started); got != "a-short" {
		t.Fatalf("first admission = %q, want a-short", got)
	}
	close(aFirst)
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("admission after A consumed 2 of 8 expected bytes = %q, want b-1", got)
	}
	close(bFirst)
	if got := awaitCoordinatorStart(t, started); got != "a-final" {
		t.Fatalf("admission with A charged 2 and B charged 4 bytes = %q, want a-final", got)
	}
	close(aSecond)
	if completed := awaitCoordinatorResult(t, aResult); completed.Err != nil || completed.Bytes != 10 {
		t.Fatalf("A completion = %+v, want 10 actual consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-2" {
		t.Fatalf("remaining admission = %q, want b-2", got)
	}
	close(bSecond)
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil || completed.Bytes != 8 {
		t.Fatalf("B completion = %+v, want 8 consumed bytes and no error", completed)
	}
}

func TestSourceHashCoordinatorChargesDiscardedWork(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 8)
	discardRelease := make(chan struct{})
	aNextRelease := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})
	errDiscarded := errors.New("discard Source Hash Work")

	releaseGate := occupyCoordinatorSlot(t, coordinator, started, "gate")
	discardedResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "a-discarded", release: discardRelease, expected: 8, bytes: 5, err: errDiscarded},
			},
		},
	)).Completion
	aNextResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-next", aNextRelease, 4),
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		&controlledCoordinatorWork{
			started: started,
			quanta: []controlledCoordinatorQuantum{
				{label: "b-1", release: bFirst, bytes: 3},
				{label: "b-2", release: bSecond, bytes: 3, done: true},
			},
		},
	)).Completion

	releaseGate()
	if got := awaitCoordinatorStart(t, started); got != "a-discarded" {
		t.Fatalf("first admission = %q, want a-discarded", got)
	}
	close(discardRelease)
	if completed := awaitCoordinatorResult(t, discardedResult); !errors.Is(completed.Err, errDiscarded) || completed.Bytes != 5 {
		t.Fatalf("discarded completion = %+v, want 5 consumed bytes and discard error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "b-1" {
		t.Fatalf("admission after A discarded 5 bytes = %q, want b-1", got)
	}
	close(bFirst)
	if got := awaitCoordinatorStart(t, started); got != "b-2" {
		t.Fatalf("admission while B had consumed 3 bytes = %q, want b-2", got)
	}
	close(bSecond)
	if completed := awaitCoordinatorResult(t, bResult); completed.Err != nil || completed.Bytes != 6 {
		t.Fatalf("B completion = %+v, want 6 consumed bytes and no error", completed)
	}
	if got := awaitCoordinatorStart(t, started); got != "a-next" {
		t.Fatalf("admission after B reached 6 bytes = %q, want a-next", got)
	}
	close(aNextRelease)
	if completed := awaitCoordinatorResult(t, aNextResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

func TestSourceHashCoordinatorNewParticipantJoinsItsPriorityShare(t *testing.T) {
	coordinator := NewSourceHashCoordinator(1)
	started := make(chan string, 10)
	highEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "high-share", Priority: 50})
	incumbentEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "incumbent", Priority: 0})
	t.Cleanup(highEpoch.Close)
	t.Cleanup(incumbentEpoch.Close)
	highRelease := make(chan struct{})
	incumbentFirst := make(chan struct{})
	incumbentNext := make(chan struct{})
	newcomerRelease := make(chan struct{})

	releaseGate := occupyCoordinatorSlot(t, coordinator, started, "gate-1")
	highResult := coordinator.Submit(t.Context(), coordinatorRequest("high-share", 50, 0,
		newSingleQuantumCoordinatorWork(started, "high-share-1", highRelease, 1),
	)).Completion
	incumbentFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("incumbent", 0, 0,
		newSingleQuantumCoordinatorWork(started, "incumbent-1", incumbentFirst, 8),
	)).Completion

	releaseGate()
	if got := awaitCoordinatorStart(t, started); got != "high-share-1" {
		t.Fatalf("higher-priority share admission = %q, want high-share-1", got)
	}
	close(highRelease)
	if completed := awaitCoordinatorResult(t, highResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "incumbent-1" {
		t.Fatalf("incumbent admission = %q, want incumbent-1", got)
	}

	close(incumbentFirst)
	if completed := awaitCoordinatorResult(t, incumbentFirstResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	releaseSecondGate := occupyCoordinatorSlot(t, coordinator, started, "gate-2")
	incumbentNextResult := coordinator.Submit(t.Context(), coordinatorRequest("incumbent", 0, 0,
		newSingleQuantumCoordinatorWork(started, "incumbent-2", incumbentNext, 2),
	)).Completion
	newcomerEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "newcomer", Priority: 0})
	t.Cleanup(newcomerEpoch.Close)
	newcomerResult := coordinator.Submit(t.Context(), coordinatorRequest("newcomer", 0, 0,
		newSingleQuantumCoordinatorWork(started, "newcomer-1", newcomerRelease, 2),
	)).Completion

	releaseSecondGate()
	if got := awaitCoordinatorStart(t, started); got != "incumbent-2" {
		t.Fatalf("first same-priority admission = %q, want incumbent without newcomer windfall", got)
	}
	close(incumbentNext)
	if completed := awaitCoordinatorResult(t, incumbentNextResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "newcomer-1" {
		t.Fatalf("remaining same-priority admission = %q, want newcomer", got)
	}
	close(newcomerRelease)
	if completed := awaitCoordinatorResult(t, newcomerResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
}

type controlledCoordinatorQuantum struct {
	label    string
	release  <-chan struct{}
	expected int64
	bytes    int64
	done     bool
	err      error
}

type controlledCoordinatorWork struct {
	started      chan<- string
	quanta       []controlledCoordinatorQuantum
	next         int
	released     chan struct{}
	releasedOnce sync.Once
	closed       chan struct{}
	closeStarted chan struct{}
	closeRelease <-chan struct{}
	close        sync.Once
}

type retainedCoordinatorWork struct {
	*controlledCoordinatorWork
	retained atomic.Bool
}

func newRetainedCoordinatorWork(work *controlledCoordinatorWork) *retainedCoordinatorWork {
	return &retainedCoordinatorWork{controlledCoordinatorWork: work}
}

func (w *retainedCoordinatorWork) HashNext(ctx context.Context) (HashingQuantumResult, error) {
	w.retained.Store(true)
	return w.controlledCoordinatorWork.HashNext(ctx)
}

func (w *retainedCoordinatorWork) RetainedHandle() bool {
	return w.retained.Load()
}

func (w *retainedCoordinatorWork) ReleaseRetainedHandle() {
	w.retained.Store(false)
	w.controlledCoordinatorWork.ReleaseRetainedHandle()
}

func (w *retainedCoordinatorWork) Close() {
	w.retained.Store(false)
	w.controlledCoordinatorWork.Close()
}

type sourceHashEnrollmentObserver struct {
	SourceHashCoordinator
	enrolled chan string
}

type observedDoneContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func (c *sourceHashEnrollmentObserver) Submit(ctx context.Context, request SourceHashRequest) SourceHashSubmission {
	submission := c.SourceHashCoordinator.Submit(ctx, request)
	c.enrolled <- request.Folder.ID
	return submission
}

type sourceHashReadGate struct {
	fs.File
	label   string
	started chan<- string
	release <-chan struct{}
	once    sync.Once
}

func (f *sourceHashReadGate) Read(buf []byte) (int, error) {
	f.once.Do(func() {
		f.started <- f.label
		<-f.release
	})
	return f.File.Read(buf)
}

func startBoundedSourceHashWalk(t *testing.T, coordinator SourceHashCoordinator, handles *sourceHashHandleObserver, folder string, priority int, size int, started chan<- string, labels []string, releases []<-chan struct{}) (<-chan ScanResult, *observedSourceHashFilesystem) {
	t.Helper()
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, "payload", make([]byte, size))
	filesystem := &observedSourceHashFilesystem{Filesystem: underlying, handles: handles}
	filesystem.wrap = func(file fs.File) fs.File {
		open := int(filesystem.opens.Load()) - 1
		return &sourceHashReadGate{
			File:    file,
			label:   labels[open],
			started: started,
			release: releases[open],
		}
	}
	result := Walk(t.Context(), Config{
		Folder:                folder,
		Filesystem:            filesystem,
		Hashers:               1,
		SourceHashFolder:      SourceHashFolder{ID: folder, Priority: priority},
		SourceHashCoordinator: coordinator,
		ProgressTickIntervalS: -1,
		EventLogger:           events.NoopLogger,
	})
	return result.Results, filesystem
}

func (w *controlledCoordinatorWork) ReleaseRetainedHandle() {
	if w.released != nil {
		w.releasedOnce.Do(func() { close(w.released) })
	}
	w.next = 0
}

func coordinatorRequest(folder string, priority, ceiling int, work HashingQuantumWork) SourceHashRequest {
	return SourceHashRequest{
		Folder: SourceHashFolder{
			ID:            folder,
			Priority:      priority,
			HasherCeiling: ceiling,
		},
		Work: work,
	}
}

func newSingleQuantumCoordinatorWork(started chan<- string, label string, release <-chan struct{}, bytes int64) *controlledCoordinatorWork {
	return &controlledCoordinatorWork{
		started: started,
		quanta: []controlledCoordinatorQuantum{
			{label: label, release: release, bytes: bytes, done: true},
		},
	}
}

func occupyCoordinatorSlot(t *testing.T, coordinator SourceHashCoordinator, started chan string, label string) func() {
	t.Helper()
	release := make(chan struct{})
	result := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, label, release, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != label {
		t.Fatalf("gate admission = %q, want %q", got, label)
	}
	return func() {
		t.Helper()
		close(release)
		if completed := awaitCoordinatorResult(t, result); completed.Err != nil {
			t.Fatal(completed.Err)
		}
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
	}, quantum.err
}

func (w *controlledCoordinatorWork) NextHashingQuantumBytes() int64 {
	if w.quanta[w.next].expected > 0 {
		return w.quanta[w.next].expected
	}
	return w.quanta[w.next].bytes
}

func (w *controlledCoordinatorWork) Close() {
	w.close.Do(func() {
		if w.closed != nil {
			close(w.closed)
		}
		if w.closeStarted != nil {
			close(w.closeStarted)
		}
		if w.closeRelease != nil {
			<-w.closeRelease
		}
	})
}

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

func awaitCoordinatorResult(t *testing.T, result <-chan SourceHashCompletion) SourceHashCompletion {
	t.Helper()
	select {
	case completed := <-result:
		return completed
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Source Hash Work completion")
		return SourceHashCompletion{}
	}
}

func awaitCoordinatorSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
