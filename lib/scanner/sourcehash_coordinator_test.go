// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"errors"
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
	gateRelease := make(chan struct{})
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})
	bThird := make(chan struct{})
	bFourth := make(chan struct{})

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate" {
		t.Fatalf("gate admission = %q, want gate", got)
	}

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

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	gateFirst := make(chan struct{})
	gateSecond := make(chan struct{})
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bOnly := make(chan struct{})

	gateFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateFirst, 1),
	)).Completion
	gateSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-2", gateSecond, 1),
	)).Completion
	gateAdmissions := map[string]bool{
		awaitCoordinatorStart(t, started): true,
		awaitCoordinatorStart(t, started): true,
	}
	if !gateAdmissions["gate-1"] || !gateAdmissions["gate-2"] {
		t.Fatalf("gate admissions = %v, want gate-1 and gate-2", gateAdmissions)
	}

	aFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-1", aFirst, 8),
	)).Completion
	aSecondResult := coordinator.Submit(t.Context(), coordinatorRequest("a", 0, 0,
		newSingleQuantumCoordinatorWork(started, "a-2", aSecond, 8),
	)).Completion
	bResult := coordinator.Submit(t.Context(), coordinatorRequest("b", 0, 0,
		newSingleQuantumCoordinatorWork(started, "b-1", bOnly, 3),
	)).Completion

	close(gateFirst)
	if completed := awaitCoordinatorResult(t, gateFirstResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "a-1" {
		t.Fatalf("first equal-priority admission = %q, want a-1", got)
	}

	close(gateSecond)
	if completed := awaitCoordinatorResult(t, gateSecondResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	gateRelease := make(chan struct{})
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate-1" {
		t.Fatalf("gate admission = %q, want gate-1", got)
	}

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

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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

	secondGateRelease := make(chan struct{})
	secondGateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-2", secondGateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate-2" {
		t.Fatalf("second gate admission = %q, want gate-2", got)
	}
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

	close(secondGateRelease)
	if completed := awaitCoordinatorResult(t, secondGateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	gateRelease := make(chan struct{})
	aFirst := make(chan struct{})
	aSecond := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate" {
		t.Fatalf("gate admission = %q, want gate", got)
	}
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

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	gateRelease := make(chan struct{})
	discardRelease := make(chan struct{})
	aNextRelease := make(chan struct{})
	bFirst := make(chan struct{})
	bSecond := make(chan struct{})
	errDiscarded := errors.New("discard Source Hash Work")

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate" {
		t.Fatalf("gate admission = %q, want gate", got)
	}
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

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	highEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "high-account", Priority: 50})
	incumbentEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "incumbent", Priority: 0})
	t.Cleanup(highEpoch.Close)
	t.Cleanup(incumbentEpoch.Close)
	gateRelease := make(chan struct{})
	highRelease := make(chan struct{})
	incumbentFirst := make(chan struct{})
	secondGateRelease := make(chan struct{})
	incumbentNext := make(chan struct{})
	newcomerRelease := make(chan struct{})

	gateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-1", gateRelease, 1),
	)).Completion
	if got := awaitCoordinatorStart(t, started); got != "gate-1" {
		t.Fatalf("gate admission = %q, want gate-1", got)
	}
	highResult := coordinator.Submit(t.Context(), coordinatorRequest("high-account", 50, 0,
		newSingleQuantumCoordinatorWork(started, "high-account-1", highRelease, 1),
	)).Completion
	incumbentFirstResult := coordinator.Submit(t.Context(), coordinatorRequest("incumbent", 0, 0,
		newSingleQuantumCoordinatorWork(started, "incumbent-1", incumbentFirst, 8),
	)).Completion

	close(gateRelease)
	if completed := awaitCoordinatorResult(t, gateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "high-account-1" {
		t.Fatalf("higher-priority account admission = %q, want high-account-1", got)
	}
	close(highRelease)
	if completed := awaitCoordinatorResult(t, highResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "incumbent-1" {
		t.Fatalf("incumbent admission = %q, want incumbent-1", got)
	}

	secondGateResult := coordinator.Submit(t.Context(), coordinatorRequest("gate", 100, 0,
		newSingleQuantumCoordinatorWork(started, "gate-2", secondGateRelease, 1),
	)).Completion
	close(incumbentFirst)
	if completed := awaitCoordinatorResult(t, incumbentFirstResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
	if got := awaitCoordinatorStart(t, started); got != "gate-2" {
		t.Fatalf("second gate admission = %q, want gate-2", got)
	}
	incumbentNextResult := coordinator.Submit(t.Context(), coordinatorRequest("incumbent", 0, 0,
		newSingleQuantumCoordinatorWork(started, "incumbent-2", incumbentNext, 2),
	)).Completion
	newcomerEpoch := coordinator.BeginSourceHashEpoch(SourceHashFolder{ID: "newcomer", Priority: 0})
	t.Cleanup(newcomerEpoch.Close)
	newcomerResult := coordinator.Submit(t.Context(), coordinatorRequest("newcomer", 0, 0,
		newSingleQuantumCoordinatorWork(started, "newcomer-1", newcomerRelease, 2),
	)).Completion

	close(secondGateRelease)
	if completed := awaitCoordinatorResult(t, secondGateResult); completed.Err != nil {
		t.Fatal(completed.Err)
	}
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
	started chan<- string
	quanta  []controlledCoordinatorQuantum
	next    int
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
