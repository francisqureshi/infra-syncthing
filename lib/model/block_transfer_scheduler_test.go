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

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "active",
		device: device1,
		bytes:  6,
	}))
	high := scheduler.enqueue(blockTransferDescriptor{
		folder: "high",
		device: device1,
		bytes:  6,
	})
	low := scheduler.enqueue(blockTransferDescriptor{
		folder: "low",
		device: device1,
		bytes:  2,
	})

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
	result := <-scheduler.enqueue(blockTransferDescriptor{folder: "low", device: device1, bytes: 2}).result
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

	active := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "active",
		device: device1,
		bytes:  4,
	}))
	high := scheduler.enqueue(blockTransferDescriptor{
		folder: "high",
		device: device1,
		bytes:  4,
	})
	leftover := scheduler.enqueue(blockTransferDescriptor{
		folder: "low",
		device: device2,
		bytes:  2,
	})
	excess := scheduler.enqueue(blockTransferDescriptor{
		folder: "low",
		device: device2,
		bytes:  1,
	})

	assertBlockTransferWaiting(t, high)
	leftoverAdmission := awaitBlockTransferAdmission(t, leftover)
	assertBlockTransferWaiting(t, excess)

	active.close()
	highAdmission := awaitBlockTransferAdmission(t, high)
	awaitBlockTransferAdmission(t, excess).close()
	leftoverAdmission.close()
	highAdmission.close()
}

func TestBlockTransferSchedulerSharesEqualPriorityBytesBetweenFolders(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":     100,
		"folder-a": 0,
		"folder-b": 0,
	})

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "gate",
		device: device1,
		bytes:  4,
	}))
	a1 := scheduler.enqueue(blockTransferDescriptor{folder: "folder-a", device: device1, bytes: 4})
	a2 := scheduler.enqueue(blockTransferDescriptor{folder: "folder-a", device: device1, bytes: 4})
	b1 := scheduler.enqueue(blockTransferDescriptor{folder: "folder-b", device: device1, bytes: 2})
	b2 := scheduler.enqueue(blockTransferDescriptor{folder: "folder-b", device: device1, bytes: 2})

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

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "gate",
		device: device1,
		bytes:  4,
	}))
	device1First := scheduler.enqueue(blockTransferDescriptor{folder: "shared", device: device1, bytes: 4})
	device1Second := scheduler.enqueue(blockTransferDescriptor{folder: "shared", device: device1, bytes: 4})
	device2First := scheduler.enqueue(blockTransferDescriptor{folder: "shared", device: device2, bytes: 2})
	device2Second := scheduler.enqueue(blockTransferDescriptor{folder: "shared", device: device2, bytes: 2})

	gate.close()
	awaitBlockTransferAdmission(t, device1First).close()
	awaitBlockTransferAdmission(t, device2First).close()
	awaitBlockTransferAdmission(t, device2Second).close()
	awaitBlockTransferAdmission(t, device1Second).close()
}

func TestBlockTransferSchedulerDoesNotCarryFairnessDebtAcrossIdlePeriods(t *testing.T) {
	scheduler := configuredBlockTransferScheduler(4, nil, map[string]int{
		"gate":     100,
		"folder-a": 0,
		"folder-b": 0,
	})

	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{folder: "folder-a", device: device1, bytes: 4})).close()
	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{folder: "folder-a", device: device1, bytes: 4})).close()

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{folder: "gate", device: device1, bytes: 4}))
	a := scheduler.enqueue(blockTransferDescriptor{folder: "folder-a", device: device1, bytes: 4})
	b := scheduler.enqueue(blockTransferDescriptor{folder: "folder-b", device: device1, bytes: 4})

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

	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "gate",
		device: device1,
		bytes:  4,
	}))
	first := scheduler.enqueue(blockTransferDescriptor{folder: "first", device: device1, bytes: 4})
	second := scheduler.enqueue(blockTransferDescriptor{folder: "second", device: device1, bytes: 4})

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

	incumbentActive := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "incumbent",
		device: device1,
		bytes:  4,
	}))
	gate := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "gate",
		device: device1,
		bytes:  4,
	}))
	incumbentNext := scheduler.enqueue(blockTransferDescriptor{folder: "incumbent", device: device1, bytes: 4})
	movingA := scheduler.enqueue(blockTransferDescriptor{folder: "moving-a", device: device1, bytes: 4})
	movingB := scheduler.enqueue(blockTransferDescriptor{folder: "moving-b", device: device1, bytes: 4})

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

	first := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder:     "folder",
		device:     device1,
		connection: "connection",
		bytes:      4,
	}))
	second := awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder:     "folder",
		device:     device1,
		connection: "connection",
		bytes:      4,
	}))

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

func TestConfigureBlockTransferSchedulerKeepsLegacyPathBehindFeatureFlag(t *testing.T) {
	scheduler := newBlockTransferScheduler()
	cfg := config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "folder"}},
		Devices: []config.DeviceConfiguration{{DeviceID: device1, MaxRequestKiB: 1}},
	}
	configureBlockTransferScheduler(scheduler, cfg)

	legacyResult := <-scheduler.enqueue(blockTransferDescriptor{
		folder: "folder",
		device: device1,
		bytes:  1024,
	}).result
	if legacyResult.err != nil || legacyResult.admission != nil {
		t.Fatalf("inactive feature flag did not use legacy upload path: %#v", legacyResult)
	}

	cfg.Options.FeatureFlags = []string{config.FeatureFlagNetworkPriority}
	configureBlockTransferScheduler(scheduler, cfg)
	awaitBlockTransferAdmission(t, scheduler.enqueue(blockTransferDescriptor{
		folder: "folder",
		device: device1,
		bytes:  1024,
	})).close()

	cfg.Options.FeatureFlags = nil
	configureBlockTransferScheduler(scheduler, cfg)
	disabledResult := <-scheduler.enqueue(blockTransferDescriptor{folder: "folder", device: device1, bytes: 1024}).result
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
