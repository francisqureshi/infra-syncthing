// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestBlockTransferSchedulerStrictPriorityAccumulatesCapacity(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(6, nil, map[string]int{
		"active": 0,
		"high":   100,
		"low":    -100,
	})

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("active", device1, 6)))
	high := scheduler.enqueue(blockTransferDescriptorForDevice("high", device1, 6))
	low := scheduler.enqueue(blockTransferDescriptorForDevice("low", device1, 2))

	assertBlockTransferWaiting(t, high)
	assertBlockTransferWaiting(t, low)
	active.close()

	highAdmission := awaitBlockTransferAdmission(t, high)
	assertBlockTransferWaiting(t, low)
	highAdmission.close()

	awaitBlockTransferAdmission(t, low).close()
	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  6,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders: map[string]blockTransferFolder{
			"active": {priority: 0, runnable: true},
			"high":   {priority: 100, runnable: true},
			"low":    {priority: -100, runnable: false},
		},
	})
	result := <-scheduler.enqueue(blockTransferDescriptorForDevice("low", device1, 2)).result
	if !errors.Is(result.err, protocol.ErrGeneric) {
		t.Fatalf("new request for non-runnable folder returned %v, expected protocol error", result.err)
	}
}

func TestBlockTransferSchedulerAllowsOnlyGenuineLeftoverCapacity(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(10, map[protocol.DeviceID]int{device1: 4}, map[string]int{
		"active": 0,
		"high":   100,
		"low":    -100,
	})

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("active", device1, 4)))
	high := scheduler.enqueue(blockTransferDescriptorForDevice("high", device1, 4))
	leftover := scheduler.enqueue(blockTransferDescriptorForDevice("low", device2, 2))
	excess := scheduler.enqueue(blockTransferDescriptorForDevice("low", device2, 1))

	assertBlockTransferWaiting(t, high)
	leftoverAdmission := awaitBlockTransferAdmission(t, leftover)
	assertBlockTransferWaiting(t, excess)

	active.close()
	highAdmission := awaitBlockTransferAdmission(t, high)
	awaitBlockTransferAdmission(t, excess).close()
	leftoverAdmission.close()
	highAdmission.close()
}

func TestBlockTransferSchedulerProtectsOnlyNextHighPriorityBlock(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(10, map[protocol.DeviceID]int{device1: 4}, map[string]int{
		"active": 0,
		"high":   100,
		"low":    -100,
	})

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("active", device1, 4)))
	nextHigh := scheduler.enqueue(blockTransferDescriptorForDevice("high", device1, 4))
	laterHigh := scheduler.enqueue(blockTransferDescriptorForDevice("high", device2, 10))
	leftover := scheduler.enqueue(blockTransferDescriptorForDevice("low", device2, 2))

	assertBlockTransferWaiting(t, nextHigh)
	assertBlockTransferWaiting(t, laterHigh)
	leftoverAdmission := awaitBlockTransferAdmission(t, leftover)
	leftoverAdmission.close()

	active.close()
	awaitBlockTransferAdmission(t, nextHigh).close()
	awaitBlockTransferAdmission(t, laterHigh).close()
}

func TestBlockTransferSchedulerSharesEqualPriorityBytesBetweenFolders(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":     100,
		"folder-a": 0,
		"folder-b": 0,
	})

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	a1 := scheduler.enqueue(blockTransferDescriptorForDevice("folder-a", device1, 4))
	a2 := scheduler.enqueue(blockTransferDescriptorForDevice("folder-a", device1, 4))
	b1 := scheduler.enqueue(blockTransferDescriptorForDevice("folder-b", device1, 2))
	b2 := scheduler.enqueue(blockTransferDescriptorForDevice("folder-b", device1, 2))

	gate.close()
	awaitBlockTransferAdmission(t, a1).close()
	awaitBlockTransferAdmission(t, b1).close()
	awaitBlockTransferAdmission(t, b2).close()
	awaitBlockTransferAdmission(t, a2).close()
}

func TestBlockTransferSchedulerSharesFolderBytesBetweenDevices(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":   100,
		"shared": 0,
	})

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	device1First := scheduler.enqueue(blockTransferDescriptorForDevice("shared", device1, 4))
	device1Second := scheduler.enqueue(blockTransferDescriptorForDevice("shared", device1, 4))
	device2First := scheduler.enqueue(blockTransferDescriptorForDevice("shared", device2, 2))
	device2Second := scheduler.enqueue(blockTransferDescriptorForDevice("shared", device2, 2))

	gate.close()
	awaitBlockTransferAdmission(t, device1First).close()
	awaitBlockTransferAdmission(t, device2First).close()
	awaitBlockTransferAdmission(t, device2Second).close()
	awaitBlockTransferAdmission(t, device1Second).close()
}

func TestBlockTransferSchedulerSharesCandidateSourcesBetweenDevices(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":   100,
		"shared": 0,
	})

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	sources := []blockTransferSource{
		{device: device1, connections: []string{"device-1"}},
		{device: device2, connections: []string{"device-2"}},
	}
	first := scheduler.enqueue(blockTransferDescriptor{folder: "shared", sources: sources, bytes: 4})
	second := scheduler.enqueue(blockTransferDescriptor{folder: "shared", sources: sources, bytes: 4})

	gate.close()
	firstAdmission := awaitBlockTransferAdmission(t, first)
	if firstAdmission.device != device1 {
		firstAdmission.close()
		t.Fatalf("first source device = %s, want %s", firstAdmission.device, device1)
	}
	firstAdmission.close()
	secondAdmission := awaitBlockTransferAdmission(t, second)
	if secondAdmission.device != device2 {
		secondAdmission.close()
		t.Fatalf("second source device = %s, want %s", secondAdmission.device, device2)
	}
	secondAdmission.close()
}

func TestBlockTransferSchedulerDoesNotCarryFairnessDebtAcrossIdlePeriods(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":     100,
		"folder-a": 0,
		"folder-b": 0,
	})

	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder-a", device1, 4))).close()
	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder-a", device1, 4))).close()

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	a := scheduler.enqueue(blockTransferDescriptorForDevice("folder-a", device1, 4))
	b := scheduler.enqueue(blockTransferDescriptorForDevice("folder-b", device1, 4))

	gate.close()
	awaitBlockTransferAdmission(t, a).close()
	awaitBlockTransferAdmission(t, b).close()
}

func TestBlockTransferSchedulerReprioritizesAndCancelsQueuedFolders(t *testing.T) {
	scheduler := newBlockTransferScheduler()
	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  4,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders: map[string]blockTransferFolder{
			"gate":   {priority: 100, runnable: true},
			"first":  {priority: -100, runnable: true},
			"second": {priority: 50, runnable: true},
		},
	})

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	first := scheduler.enqueue(blockTransferDescriptorForDevice("first", device1, 4))
	second := scheduler.enqueue(blockTransferDescriptorForDevice("second", device1, 4))

	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  4,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders: map[string]blockTransferFolder{
			"gate":   {priority: 100, runnable: true},
			"first":  {priority: 100, runnable: true},
			"second": {priority: 50, runnable: true},
		},
	})
	gate.close()
	firstAdmission := awaitBlockTransferAdmission(t, first)
	assertBlockTransferWaiting(t, second)

	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  4,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders: map[string]blockTransferFolder{
			"gate":  {priority: 100, runnable: true},
			"first": {priority: 100, runnable: true},
		},
	})
	select {
	case result := <-second.result:
		if !errors.Is(result.err, protocol.ErrGeneric) {
			t.Fatalf("paused folder returned %v, expected protocol error", result.err)
		}
		if result.admission != nil {
			result.admission.close()
			t.Fatal("paused folder received an admission")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for paused folder request cancellation")
	}
	firstAdmission.close()
}

func TestBlockTransferSchedulerReprioritizationJoinsCurrentFairnessRound(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(8, nil, map[string]int{
		"gate":      100,
		"incumbent": 50,
		"moving-a":  0,
		"moving-b":  0,
	})

	incumbentActive := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("incumbent", device1, 4)))
	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("gate", device1, 4)))
	incumbentNext := scheduler.enqueue(blockTransferDescriptorForDevice("incumbent", device1, 4))
	movingA := scheduler.enqueue(blockTransferDescriptorForDevice("moving-a", device1, 4))
	movingB := scheduler.enqueue(blockTransferDescriptorForDevice("moving-b", device1, 4))

	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  8,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders: map[string]blockTransferFolder{
			"gate":      {priority: 100, runnable: true},
			"incumbent": {priority: 50, runnable: true},
			"moving-a":  {priority: 50, runnable: true},
			"moving-b":  {priority: 50, runnable: true},
		},
	})
	gate.close()

	incumbentNextAdmission := awaitBlockTransferAdmission(t, incumbentNext)
	assertBlockTransferWaiting(t, movingA)
	assertBlockTransferWaiting(t, movingB)
	incumbentNextAdmission.close()
	movingAAdmission := awaitBlockTransferAdmission(t, movingA)
	assertBlockTransferWaiting(t, movingB)
	movingAAdmission.close()
	awaitBlockTransferAdmission(t, movingB).close()
	incumbentActive.close()
}

func TestBlockTransferAdmissionsPreservePerConnectionResponseOrder(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(8, nil, map[string]int{"folder": 0})

	first := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 4, "connection")))
	second := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 4, "connection")))

	first.waitForResponse()
	select {
	case <-second.previousResponse:
		t.Fatal("second admitted response overtook the first response")
	default:
	}

	first.close()
	second.waitForResponse()
	second.close()
}

func TestBlockTransferSchedulerSelectsLeastLoadedCompatibleConnectionDeterministically(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(12, nil, map[string]int{"folder": 0})

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 4, "connection-a")))
	leastLoaded := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 4, "connection-a", "connection-b")))
	if leastLoaded.connection != "connection-b" {
		t.Fatalf("least-loaded connection = %q, want connection-b", leastLoaded.connection)
	}

	deterministicTie := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 4, "connection-b", "connection-a")))
	if deterministicTie.connection != "connection-a" {
		t.Fatalf("equal-load connection = %q, want deterministic connection-a", deterministicTie.connection)
	}

	active.close()
	leastLoaded.close()
	deterministicTie.close()
}

func TestConfigureBlockTransferSchedulerKeepsLegacyPathBehindFeatureFlag(t *testing.T) {
	scheduler := newBlockTransferScheduler()
	cfg := config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "folder"}},
		Devices: []config.DeviceConfiguration{{DeviceID: device1, MaxRequestKiB: 1}},
	}
	configureUploadBlockTransferScheduler(scheduler, cfg)

	legacyResult := <-scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 1024)).result
	if legacyResult.err != nil || legacyResult.admission != nil {
		t.Fatalf("inactive feature flag did not use legacy upload path: %#v", legacyResult)
	}

	cfg.Options.FeatureFlags = []string{config.FeatureFlagNetworkPriority}
	configureUploadBlockTransferScheduler(scheduler, cfg)
	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 1024))).close()

	cfg.Options.FeatureFlags = nil
	configureUploadBlockTransferScheduler(scheduler, cfg)
	disabledResult := <-scheduler.enqueue(blockTransferDescriptorForDevice("folder", device1, 1024)).result
	if disabledResult.err != nil || disabledResult.admission != nil {
		t.Fatalf("disabled scheduler did not return to legacy upload path: %#v", disabledResult)
	}
}

func configuredBlockTransferScheduler(globalLimit int, deviceLimits map[protocol.DeviceID]int, priorities map[string]int) *blockTransferScheduler {
	if deviceLimits == nil {
		deviceLimits = make(map[protocol.DeviceID]int)
	}
	folders := make(map[string]blockTransferFolder, len(priorities))
	for folder, priority := range priorities {
		folders[folder] = blockTransferFolder{priority: priority, runnable: true}
	}
	scheduler := newBlockTransferScheduler()
	scheduler.configure(blockTransferSchedulerConfiguration{
		enabled:      true,
		globalLimit:  globalLimit,
		deviceLimits: deviceLimits,
		folders:      folders,
	})
	return scheduler
}

func blockTransferDescriptorForDevice(folder string, device protocol.DeviceID, bytes int, connections ...string) blockTransferDescriptor {
	return blockTransferDescriptor{
		folder: folder,
		sources: []blockTransferSource{{
			device:      device,
			connections: connections,
		}},
		bytes: bytes,
	}
}

func awaitBlockTransferAdmission(t *testing.T, waiter *blockTransferWaiter) *blockTransferAdmission {
	t.Helper()
	select {
	case result := <-waiter.result:
		if result.err != nil {
			t.Fatalf("Block Transfer admission failed: %v", result.err)
		}
		if result.admission == nil {
			t.Fatal("Block Transfer admission was bypassed")
		}
		return result.admission
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Block Transfer admission")
		return nil
	}
}

func assertBlockTransferWaiting(t *testing.T, waiter *blockTransferWaiter) {
	t.Helper()
	select {
	case result := <-waiter.result:
		if result.err != nil {
			t.Fatalf("queued Block Transfer failed: %v", result.err)
		}
		result.admission.close()
		t.Fatal("Block Transfer was admitted before capacity became available")
	default:
	}
}
