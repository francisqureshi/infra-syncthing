// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"runtime"
	"sync"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

func TestNetworkPrioritySchedulerControlledLoad(t *testing.T) {
	const (
		transfersPerFolder = 256
		smallBlock         = 1 << 20
		largeBlock         = 4 << 20
		maxQueueHeap       = 32 << 20
	)
	if smallBlock > protocol.MaxBlockSize || largeBlock > protocol.MaxBlockSize {
		t.Fatal("controlled load contains a block larger than the protocol limit")
	}
	folders := []string{"folder-a", "folder-b", "folder-c"}
	scheduler := configuredBlockTransferScheduler(1, nil, map[string]int{
		"gate":     100,
		"folder-a": 0,
		"folder-b": 0,
		"folder-c": 0,
	})
	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 1)))

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	waiters := make([]*blockTransferWaiter, 0, transfersPerFolder*len(folders))
	for transfer := range transfersPerFolder {
		bytes := smallBlock
		if transfer%2 != 0 {
			bytes = largeBlock
		}
		for _, folder := range folders {
			waiters = append(waiters, scheduler.enqueue(blockTransferDescriptor{
				folder: folder,
				bytes:  bytes,
				sources: []blockTransferSource{
					{device: device1, connections: []string{"device-1"}},
					{device: device2, connections: []string{"device-2"}},
				},
			}))
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if delta := max(after.Alloc, before.Alloc) - before.Alloc; delta > maxQueueHeap {
		t.Fatalf("queuing lightweight descriptors grew the heap by %d bytes, limit %d", delta, maxQueueHeap)
	}

	scheduler.mut.Lock()
	if got, want := len(scheduler.queued), len(waiters); got != want {
		scheduler.mut.Unlock()
		t.Fatalf("queued transfers = %d, want %d", got, want)
	}
	if scheduler.globalInFlight > scheduler.globalLimit {
		scheduler.mut.Unlock()
		t.Fatalf("global In-Flight accounting = %d, limit %d", scheduler.globalInFlight, scheduler.globalLimit)
	}
	scheduler.mut.Unlock()

	gate.close()
	admitted := make(chan blockTransferResult)
	var waiting sync.WaitGroup
	waiting.Add(len(waiters))
	for _, waiter := range waiters {
		go func() {
			defer waiting.Done()
			admitted <- <-waiter.result
		}()
	}
	folderBytes := make(map[string]int64)
	deviceBytes := make(map[string]map[protocol.DeviceID]int64, len(folders))
	for _, folder := range folders {
		deviceBytes[folder] = map[protocol.DeviceID]int64{device1: 0, device2: 0}
	}
	for range waiters {
		result := <-admitted
		if result.err != nil {
			t.Fatal(result.err)
		}
		admission := result.admission
		folderBytes[admission.folder] += admission.transferBytes
		deviceBytes[admission.folder][admission.device] += admission.transferBytes
		assertByteFairConvergence(t, "folders", folderBytes, largeBlock)
		assertByteFairConvergence(t, "devices in "+admission.folder, deviceBytes[admission.folder], largeBlock)

		scheduler.mut.Lock()
		if scheduler.globalInFlight > scheduler.globalLimit {
			scheduler.mut.Unlock()
			t.Fatalf("global In-Flight accounting = %d, limit %d", scheduler.globalInFlight, scheduler.globalLimit)
		}
		scheduler.mut.Unlock()
		admission.close()
	}
	waiting.Wait()

	wantFolderBytes := int64(transfersPerFolder/2) * (smallBlock + largeBlock)
	for _, folder := range folders {
		if got := folderBytes[folder]; got != wantFolderBytes {
			t.Fatalf("folder %q admitted %d bytes, want %d", folder, got, wantFolderBytes)
		}
	}
}

func assertByteFairConvergence[K comparable](t *testing.T, scope string, values map[K]int64, maximumDifference int64) {
	t.Helper()
	var minimum, maximum int64
	first := true
	for _, value := range values {
		if first || value < minimum {
			minimum = value
		}
		if first || value > maximum {
			maximum = value
		}
		first = false
	}
	if maximum-minimum > maximumDifference {
		t.Fatalf("%s diverged by %d bytes, maximum expected difference %d: %#v", scope, maximum-minimum, maximumDifference, values)
	}
}
