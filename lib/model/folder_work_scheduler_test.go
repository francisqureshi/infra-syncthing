// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"
	"time"
)

func TestFolderWorkSchedulerKeepsMaintenanceInArrivalOrder(t *testing.T) {
	scheduler := configuredFolderWorkScheduler(map[string]int{
		"low":  -100,
		"high": 100,
	})
	if err := scheduler.takeWithContext(t.Context(), "gate", folderWorkNetwork); err != nil {
		t.Fatal(err)
	}

	started := make(chan string, 2)
	lowRelease := enqueueFolderWork(t, scheduler, started, "low", folderWorkMaintenance)
	awaitFolderWorkQueued(t, scheduler, 1)
	highRelease := enqueueFolderWork(t, scheduler, started, "high", folderWorkMaintenance)
	awaitFolderWorkQueued(t, scheduler, 2)

	scheduler.give()
	if folder := awaitFolderWorkStart(t, started); folder != "low" {
		t.Fatalf("first maintenance work is for folder %q, want low", folder)
	}
	assertNoFolderWorkStarted(t, started)
	close(lowRelease)
	if folder := awaitFolderWorkStart(t, started); folder != "high" {
		t.Fatalf("second maintenance work is for folder %q, want high", folder)
	}
	close(highRelease)
}

func TestFolderWorkSchedulerUsesNetworkPriorityUniversally(t *testing.T) {
	scheduler := configuredFolderWorkScheduler(map[string]int{
		"low":  -100,
		"high": 100,
	})
	if err := scheduler.takeWithContext(t.Context(), "gate", folderWorkNetwork); err != nil {
		t.Fatal(err)
	}

	started := make(chan string, 2)
	lowRelease := enqueueFolderWork(t, scheduler, started, "low", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 1)
	highRelease := enqueueFolderWork(t, scheduler, started, "high", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 2)

	scheduler.give()
	if folder := awaitFolderWorkStart(t, started); folder != "high" {
		t.Fatalf("first folder work is for folder %q, want high", folder)
	}
	close(highRelease)
	if folder := awaitFolderWorkStart(t, started); folder != "low" {
		t.Fatalf("second folder work is for folder %q, want low", folder)
	}
	close(lowRelease)
}

func TestFolderWorkSchedulerReprioritizesQueuedWork(t *testing.T) {
	scheduler := configuredFolderWorkScheduler(map[string]int{
		"first":  0,
		"second": 0,
	})
	if err := scheduler.takeWithContext(t.Context(), "gate", folderWorkNetwork); err != nil {
		t.Fatal(err)
	}

	started := make(chan string, 2)
	firstRelease := enqueueFolderWork(t, scheduler, started, "first", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 1)
	secondRelease := enqueueFolderWork(t, scheduler, started, "second", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 2)
	scheduler.configure(1, map[string]int{
		"first":  -100,
		"second": 100,
	})

	scheduler.give()
	if folder := awaitFolderWorkStart(t, started); folder != "second" {
		t.Fatalf("first reprioritized folder work is for folder %q, want second", folder)
	}
	close(secondRelease)
	if folder := awaitFolderWorkStart(t, started); folder != "first" {
		t.Fatalf("second reprioritized folder work is for folder %q, want first", folder)
	}
	close(firstRelease)
}

func TestFolderWorkSchedulerMaintenanceDoesNotInvertNetworkPriority(t *testing.T) {
	scheduler := configuredFolderWorkScheduler(map[string]int{
		"low":  -100,
		"high": 100,
	})
	if err := scheduler.takeWithContext(t.Context(), "gate", folderWorkNetwork); err != nil {
		t.Fatal(err)
	}

	started := make(chan string, 3)
	lowRelease := enqueueFolderWork(t, scheduler, started, "low", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 1)
	maintenanceRelease := enqueueFolderWork(t, scheduler, started, "maintenance", folderWorkMaintenance)
	awaitFolderWorkQueued(t, scheduler, 2)
	highRelease := enqueueFolderWork(t, scheduler, started, "high", folderWorkNetwork)
	awaitFolderWorkQueued(t, scheduler, 3)

	scheduler.give()
	if folder := awaitFolderWorkStart(t, started); folder != "high" {
		t.Fatalf("first network work around maintenance is for folder %q, want high", folder)
	}
	close(highRelease)
	if folder := awaitFolderWorkStart(t, started); folder != "low" {
		t.Fatalf("second network work around maintenance is for folder %q, want low", folder)
	}
	close(lowRelease)
	if folder := awaitFolderWorkStart(t, started); folder != "maintenance" {
		t.Fatalf("work after prioritized folders is %q, want maintenance", folder)
	}
	close(maintenanceRelease)
}

func configuredFolderWorkScheduler(priorities map[string]int) *folderWorkScheduler {
	scheduler := newFolderWorkScheduler()
	scheduler.configure(1, priorities)
	return scheduler
}

func enqueueFolderWork(t *testing.T, scheduler *folderWorkScheduler, started chan<- string, folder string, class folderWorkClass) chan struct{} {
	t.Helper()
	release := make(chan struct{})
	go func() {
		if err := scheduler.takeWithContext(t.Context(), folder, class); err != nil {
			return
		}
		started <- folder
		<-release
		scheduler.give()
	}()
	return release
}

func awaitFolderWorkQueued(t *testing.T, scheduler *folderWorkScheduler, expected int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		scheduler.mut.Lock()
		queued := len(scheduler.waiters)
		scheduler.mut.Unlock()
		if queued == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued folder work items", expected)
}

func awaitFolderWorkStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case folder := <-started:
		return folder
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for folder work to start")
		return ""
	}
}

func assertNoFolderWorkStarted(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case folder := <-started:
		t.Fatalf("folder %q work exceeded maximum folder concurrency", folder)
	case <-time.After(50 * time.Millisecond):
	}
}
