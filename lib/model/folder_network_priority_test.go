// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/timeutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
	"github.com/syncthing/syncthing/lib/scanner"
)

func TestAutomaticPullsUseNetworkPriorityWithinFolderConcurrency(t *testing.T) {
	m := newFolderNetworkPriorityModel(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	})
	conn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "high")

	started := make(chan folderPullStart, 3)
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		start := folderPullStart{
			folder:  req.Folder,
			release: make(chan struct{}),
		}
		select {
		case started <- start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-start.release:
			return bytes.Repeat([]byte(req.Folder), req.Size/len(req.Folder)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	sendFolderNetworkPriorityFile(t, m, conn, "active")
	active := awaitFolderPullStart(t, started)
	if active.folder != "active" {
		t.Fatalf("first active pull is for folder %q, want active", active.folder)
	}

	// The active pull occupies the only folder-I/O slot, ensuring both queued
	// folders are considered together when it completes.
	sendFolderNetworkPriorityFile(t, m, conn, "low")
	awaitFolderState(t, m, "low", FolderSyncWaiting)
	sendFolderNetworkPriorityFile(t, m, conn, "high")
	awaitFolderState(t, m, "high", FolderSyncWaiting)
	assertNoFolderPullStarted(t, started)

	close(active.release)
	high := awaitFolderPullStart(t, started)
	if high.folder != "high" {
		close(high.release)
		t.Fatalf("first queued pull is for folder %q, want high", high.folder)
	}
	assertNoFolderPullStarted(t, started)

	close(high.release)
	low := awaitFolderPullStart(t, started)
	if low.folder != "low" {
		close(low.release)
		t.Fatalf("second queued pull is for folder %q, want low", low.folder)
	}
	close(low.release)
}

func TestPrerequisiteScansUseNetworkPriorityWithinFolderConcurrency(t *testing.T) {
	m := newFolderNetworkPriorityModel(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	})
	conn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "high")
	started := make(chan folderPullStart, 1)
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		start := folderPullStart{folder: req.Folder, release: make(chan struct{})}
		select {
		case started <- start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-start.release:
			return bytes.Repeat([]byte(req.Folder), req.Size/len(req.Folder)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	sendFolderNetworkPriorityFile(t, m, conn, "active")
	active := awaitFolderPullStart(t, started)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	if err := m.SetIgnores("low", []string{"ignored-low"}); err != nil {
		t.Fatal(err)
	}
	awaitFolderState(t, m, "low", FolderScanWaiting)
	if err := m.SetIgnores("high", []string{"ignored-high"}); err != nil {
		t.Fatal(err)
	}
	awaitFolderState(t, m, "high", FolderScanWaiting)

	close(active.release)
	if folder := awaitFirstScanningFolder(t, sub, "low", "high"); folder != "high" {
		t.Fatalf("first queued prerequisite scan is for folder %q, want high", folder)
	}
	awaitFolderStateEvent(t, sub, "high", FolderIdle)
	awaitFolderStateEvent(t, sub, "low", FolderScanning)
	awaitFolderStateEvent(t, sub, "low", FolderIdle)
}

func TestExplicitScansKeepLegacyFolderConcurrencyOrder(t *testing.T) {
	m := newFolderNetworkPriorityModel(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	})
	conn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "high")
	started := make(chan folderPullStart, 1)
	observeFolderPullStarts(conn, started)
	sendFolderNetworkPriorityFile(t, m, conn, "active")
	active := awaitFolderPullStart(t, started)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	awaitFolderState(t, m, "low", FolderScanWaiting)
	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderState(t, m, "high", FolderScanWaiting)

	close(active.release)
	if folder := awaitFirstScanningFolder(t, sub, "low", "high"); folder != "low" {
		t.Fatalf("first queued explicit scan is for folder %q, want low", folder)
	}
	if err := <-lowResult; err != nil {
		t.Fatal(err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func TestUnavailablePullYieldsAndReentersNetworkPriorityOrdering(t *testing.T) {
	m := newFolderNetworkPriorityModelForDevices(t, map[string]int{
		"active": 0,
		"low":    -50,
		"lowest": -100,
		"high":   100,
	}, map[string]protocol.DeviceID{
		"active": device1,
		"low":    device1,
		"lowest": device1,
		"high":   device2,
	})
	started := make(chan folderPullStart, 4)
	lowConn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "lowest")
	observeFolderPullStarts(lowConn, started)

	sendFolderNetworkPriorityFile(t, m, lowConn, "active")
	active := awaitFolderPullStart(t, started)

	// The high-priority folder has needed source data in the index, but its
	// only peer is not connected yet.
	highFile := newFolderNetworkPriorityFile(t, "high", device2)
	if err := m.sdb.Update("high", device2, []protocol.FileInfo{highFile}); err != nil {
		t.Fatal(err)
	}
	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	runner, ok := m.folderRunners.Get("high")
	if !ok {
		t.Fatal("high-priority folder is not running")
	}
	runner.SchedulePull()
	awaitFolderStateEvent(t, sub, "high", FolderSyncWaiting)
	awaitFolderStateEvent(t, sub, "high", FolderIdle)

	sendFolderNetworkPriorityFile(t, m, lowConn, "low")
	awaitFolderState(t, m, "low", FolderSyncWaiting)
	close(active.release)
	low := awaitFolderPullStart(t, started)
	if low.folder != "low" {
		close(low.release)
		t.Fatalf("runnable work after unavailable high-priority folder is %q, want low", low.folder)
	}

	// A new connection makes the high-priority folder runnable while the low
	// pull remains active. A lower-priority peer is queued at the same time.
	highConn := addFolderNetworkPriorityConnection(t, m, device2, "high")
	observeFolderPullStarts(highConn, started)
	if err := m.IndexUpdate(highConn, &protocol.IndexUpdate{Folder: "high", Files: []protocol.FileInfo{highFile}}); err != nil {
		t.Fatal(err)
	}
	awaitFolderState(t, m, "high", FolderSyncWaiting)
	sendFolderNetworkPriorityFile(t, m, lowConn, "lowest")
	awaitFolderState(t, m, "lowest", FolderSyncWaiting)

	close(low.release)
	high := awaitFolderPullStart(t, started)
	if high.folder != "high" {
		close(high.release)
		t.Fatalf("recovered high-priority pull is %q, want high", high.folder)
	}
	close(high.release)
	lowest := awaitFolderPullStart(t, started)
	if lowest.folder != "lowest" {
		close(lowest.release)
		t.Fatalf("pull after recovered high-priority folder is %q, want lowest", lowest.folder)
	}
	close(lowest.release)
}

func TestPullRetryBackoffYieldsToRunnableWork(t *testing.T) {
	m := newFolderNetworkPriorityModel(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	})
	conn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "high")
	started := make(chan folderPullStart, 2)
	var highAttempts atomic.Int32
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		if req.Folder == "high" {
			highAttempts.Add(1)
			return nil, errors.New("test pull failure")
		}
		start := folderPullStart{folder: req.Folder, name: req.Name, release: make(chan struct{})}
		select {
		case started <- start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-start.release:
			return bytes.Repeat([]byte(req.Folder), req.Size/len(req.Folder)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	sendFolderNetworkPriorityFile(t, m, conn, "high")
	awaitAtomicValue(t, &highAttempts, 1)
	awaitFolderState(t, m, "high", FolderIdle)
	if attempts := highAttempts.Load(); attempts != 1 {
		t.Fatalf("initial high-priority pull attempts = %d, want 1", attempts)
	}

	sendFolderNetworkPriorityFile(t, m, conn, "active")
	active := awaitFolderPullStart(t, started)
	highRunner, ok := m.folderRunners.Get("high")
	if !ok {
		t.Fatal("high-priority folder is not running")
	}
	highFolder, ok := highRunner.(*sendReceiveFolder)
	if !ok {
		t.Fatalf("high-priority folder runner has type %T, want *sendReceiveFolder", highRunner)
	}
	highRunner.SchedulePull()
	awaitPullScheduleConsumed(t, highFolder.folder)
	if attempts := highAttempts.Load(); attempts != 1 {
		t.Fatalf("high-priority pull retried during backoff; attempts = %d, want 1", attempts)
	}

	sendFolderNetworkPriorityFile(t, m, conn, "low")
	awaitFolderState(t, m, "low", FolderSyncWaiting)
	close(active.release)
	low := awaitFolderPullStart(t, started)
	if low.folder != "low" {
		close(low.release)
		t.Fatalf("work admitted while high priority is in backoff = %q, want low", low.folder)
	}
	close(low.release)
}

func TestFilePriorityRemainsFolderLocalAfterNetworkPriorityAdmission(t *testing.T) {
	m := newFolderNetworkPriorityModelWithOrders(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	}, map[string]config.PullOrder{
		"high": config.PullOrderNewestFirst,
	})
	conn := addFolderNetworkPriorityConnection(t, m, device1, "active", "low", "high")
	started := make(chan folderPullStart, 4)
	observeFolderPullStarts(conn, started)

	sendFolderNetworkPriorityFile(t, m, conn, "active")
	active := awaitFolderPullStart(t, started)
	sendFolderNetworkPriorityFile(t, m, conn, "low")
	awaitFolderState(t, m, "low", FolderSyncWaiting)

	old := newNamedFolderNetworkPriorityFile(t, "high", "old.txt", device1, time.Now().Add(-time.Hour))
	newer := newNamedFolderNetworkPriorityFile(t, "high", "new.txt", device1, time.Now())
	if err := m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: "high", Files: []protocol.FileInfo{old, newer}}); err != nil {
		t.Fatal(err)
	}
	awaitFolderState(t, m, "high", FolderSyncWaiting)
	highRunner, ok := m.folderRunners.Get("high")
	if !ok {
		t.Fatal("high-priority folder is not running")
	}
	highFolder, ok := highRunner.(*sendReceiveFolder)
	if !ok {
		t.Fatalf("high-priority folder runner has type %T, want *sendReceiveFolder", highRunner)
	}
	if highFolder.Order != config.PullOrderNewestFirst {
		t.Fatalf("high folder File Priority = %v, want newest first", highFolder.Order)
	}
	needed, errFn := m.sdb.AllNeededGlobalFiles("high", protocol.LocalDeviceID, config.PullOrderNewestFirst, 0, 0)
	var neededNames []string
	for file := range needed {
		neededNames = append(neededNames, file.Name)
	}
	if err := errFn(); err != nil {
		t.Fatal(err)
	}
	if len(neededNames) != 2 || neededNames[0] != "new.txt" {
		t.Fatalf("needed files in File Priority order = %v, want [new.txt old.txt]", neededNames)
	}

	close(active.release)
	firstHigh := awaitFolderPullStart(t, started)
	if firstHigh.folder != "high" {
		close(firstHigh.release)
		t.Fatalf("first admitted file is for folder %q, want high", firstHigh.folder)
	}
	close(firstHigh.release)
	secondHigh := awaitFolderPullStart(t, started)
	if secondHigh.folder != "high" {
		close(secondHigh.release)
		t.Fatalf("second admitted file is for folder %q, want high", secondHigh.folder)
	}
	if firstHigh.name == secondHigh.name || (firstHigh.name != "new.txt" && firstHigh.name != "old.txt") || (secondHigh.name != "new.txt" && secondHigh.name != "old.txt") {
		close(secondHigh.release)
		t.Fatalf("admitted high-priority files = [%q %q], want new.txt and old.txt", firstHigh.name, secondHigh.name)
	}
	close(secondHigh.release)
	low := awaitFolderPullStart(t, started)
	if low.folder != "low" {
		close(low.release)
		t.Fatalf("folder after high-priority files is %q, want low", low.folder)
	}
	close(low.release)
}

type folderPullStart struct {
	folder  string
	name    string
	release chan struct{}
}

func newFolderNetworkPriorityModel(t *testing.T, priorities map[string]int) *testModel {
	t.Helper()
	return newFolderNetworkPriorityModelWithOrders(t, priorities, nil)
}

func newFolderNetworkPriorityModelWithOrders(t *testing.T, priorities map[string]int, orders map[string]config.PullOrder) *testModel {
	t.Helper()
	devices := make(map[string]protocol.DeviceID, len(priorities))
	for folder := range priorities {
		devices[folder] = device1
	}
	return newFolderNetworkPriorityModelWithConfiguration(t, priorities, devices, orders)
}

func newFolderNetworkPriorityModelForDevices(t *testing.T, priorities map[string]int, devices map[string]protocol.DeviceID) *testModel {
	t.Helper()
	return newFolderNetworkPriorityModelWithConfiguration(t, priorities, devices, nil)
}

func newFolderNetworkPriorityModelWithConfiguration(t *testing.T, priorities map[string]int, devices map[string]protocol.DeviceID, orders map[string]config.PullOrder) *testModel {
	t.Helper()
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawMaxFolderConcurrency = 1
	configuredDevices := make(map[protocol.DeviceID]struct{})
	for _, deviceID := range devices {
		if _, ok := configuredDevices[deviceID]; ok {
			continue
		}
		device := cfg.Defaults.Device.Copy()
		device.DeviceID = deviceID
		cfg.SetDevice(device)
		configuredDevices[deviceID] = struct{}{}
	}
	for folderID, priority := range priorities {
		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = rand.String(32) + "?content=true"
		folder.FilesystemType = config.FilesystemTypeFake
		folder.Devices = []config.FolderDeviceConfiguration{{DeviceID: devices[folderID]}}
		folder.NetworkPriority = priority
		folder.Order = orders[folderID]
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		folder.PullerDelayS = 0
		folder.Copiers = 1
		cfg.SetFolder(folder)
	}
	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	m := setupModel(t, wrapper)
	t.Cleanup(func() { cleanupModel(m) })
	return m
}

func addFolderNetworkPriorityConnection(t *testing.T, m *testModel, device protocol.DeviceID, folders ...string) *fakeConnection {
	t.Helper()
	conn := newFakeConnection(device, m)
	m.AddConnection(conn, protocol.Hello{})
	clusterFolders := make([]protocol.Folder, 0, len(folders))
	for _, folder := range folders {
		clusterFolders = append(clusterFolders, protocol.Folder{
			ID: folder,
			Devices: []protocol.Device{
				{ID: myID},
				{ID: device},
			},
		})
	}
	if err := m.ClusterConfig(conn, &protocol.ClusterConfig{Folders: clusterFolders}); err != nil {
		t.Fatal(err)
	}
	return conn
}

func sendFolderNetworkPriorityFile(t *testing.T, m *testModel, conn *fakeConnection, folder string) {
	t.Helper()
	file := newFolderNetworkPriorityFile(t, folder, conn.id)
	if err := m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: folder, Files: []protocol.FileInfo{file}}); err != nil {
		t.Fatal(err)
	}
}

func newFolderNetworkPriorityFile(t *testing.T, folder string, device protocol.DeviceID) protocol.FileInfo {
	t.Helper()
	return newNamedFolderNetworkPriorityFile(t, folder, folder+".txt", device, time.Now())
}

func newNamedFolderNetworkPriorityFile(t *testing.T, folder, name string, device protocol.DeviceID, modified time.Time) protocol.FileInfo {
	t.Helper()
	data := bytes.Repeat([]byte(folder), 128)
	blockSize := protocol.BlockSize(int64(len(data)))
	blocks, err := scanner.Blocks(context.Background(), bytes.NewReader(data), blockSize, int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.FileInfo{
		Name:         name,
		Type:         protocol.FileInfoTypeFile,
		Size:         int64(len(data)),
		RawBlockSize: int32(blockSize),
		Blocks:       blocks,
		ModifiedS:    modified.Unix(),
		Permissions:  0o644,
		Sequence:     timeutil.StrictlyMonotonicNanos(),
		Version:      protocol.Vector{}.Update(device.Short()),
	}
}

func observeFolderPullStarts(conn *fakeConnection, started chan<- folderPullStart) {
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		start := folderPullStart{folder: req.Folder, name: req.Name, release: make(chan struct{})}
		select {
		case started <- start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-start.release:
			return bytes.Repeat([]byte(req.Folder), req.Size/len(req.Folder)), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
}

func awaitFolderState(t *testing.T, m *testModel, folder string, expected folderState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, _, err := m.State(folder)
		if err != nil {
			t.Fatal(err)
		}
		if state == expected.String() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	state, _, err := m.State(folder)
	t.Fatalf("folder %q state = %q (%v), want %q", folder, state, err, expected)
}

func awaitFolderPullStart(t *testing.T, started <-chan folderPullStart) folderPullStart {
	t.Helper()
	select {
	case start := <-started:
		return start
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for folder pull to start")
		return folderPullStart{}
	}
}

func assertNoFolderPullStarted(t *testing.T, started <-chan folderPullStart) {
	t.Helper()
	select {
	case start := <-started:
		close(start.release)
		t.Fatalf("folder %q pull started before folder concurrency became available", start.folder)
	case <-time.After(50 * time.Millisecond):
	}
}

func awaitFirstScanningFolder(t *testing.T, sub events.Subscription, folders ...string) string {
	t.Helper()
	wanted := make(map[string]struct{}, len(folders))
	for _, folder := range folders {
		wanted[folder] = struct{}{}
	}
	for {
		select {
		case ev := <-sub.C():
			data := ev.Data.(map[string]any)
			folder := data["folder"].(string)
			if _, ok := wanted[folder]; ok && data["to"] == FolderScanning.String() {
				return folder
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a prerequisite scan to start")
			return ""
		}
	}
}

func awaitFolderStateEvent(t *testing.T, sub events.Subscription, folder string, expected folderState) {
	t.Helper()
	for {
		select {
		case ev := <-sub.C():
			data := ev.Data.(map[string]any)
			if data["folder"] == folder && data["to"] == expected.String() {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for folder %q to enter %q", folder, expected)
		}
	}
}

func awaitPullScheduleConsumed(t *testing.T, folder *folder) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(folder.pullScheduled) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for scheduled pull to be consumed")
}

func awaitAtomicValue(t *testing.T, value *atomic.Int32, expected int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if value.Load() == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for atomic value %d; current value = %d", expected, value.Load())
}
