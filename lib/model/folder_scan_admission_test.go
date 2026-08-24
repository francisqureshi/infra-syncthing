// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/rand"
)

const scanAdmissionFilesystemType fs.FilesystemType = "scan-admission"

var scanAdmissionFilesystems sync.Map

func init() {
	fs.RegisterFilesystemType(scanAdmissionFilesystemType, func(root string, _ ...fs.Option) (fs.Filesystem, error) {
		control, ok := scanAdmissionFilesystems.Load(root)
		if !ok {
			return nil, errors.New("missing scan-admission filesystem")
		}
		return &scanAdmissionFilesystem{
			Filesystem: control.(*scanAdmissionFilesystemControl).filesystem,
			control:    control.(*scanAdmissionFilesystemControl),
		}, nil
	})
}

type scanAdmissionFilesystemControl struct {
	filesystem       fs.Filesystem
	walked           chan struct{}
	read             chan struct{}
	release          chan struct{}
	reconciliation   chan struct{}
	releaseReconcile chan struct{}
	traversalFailure chan struct{}
	releaseFailure   chan struct{}
	traversalBlocked chan struct{}
	releaseTraversal chan struct{}
	armed            atomic.Bool
	blockRead        atomic.Bool
	blockReconcile   atomic.Bool
	failTraversal    atomic.Bool
	blockTraversal   atomic.Bool
}

type scanAdmissionFilesystem struct {
	fs.Filesystem
	control *scanAdmissionFilesystemControl
}

func (f *scanAdmissionFilesystem) DirNames(name string) ([]string, error) {
	if f.control.armed.Load() && name == "blocked" && f.control.blockTraversal.CompareAndSwap(true, false) {
		close(f.control.traversalBlocked)
		<-f.control.releaseTraversal
	}
	if f.control.armed.Load() && name == "failure" && f.control.failTraversal.CompareAndSwap(true, false) {
		close(f.control.traversalFailure)
		<-f.control.releaseFailure
		return nil, errors.New("controlled traversal failure")
	}
	if f.control.armed.Load() && name == "traversal" {
		select {
		case f.control.walked <- struct{}{}:
		default:
		}
	}
	return f.Filesystem.DirNames(name)
}

func (f *scanAdmissionFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil || name != "payload" || !f.control.armed.Load() || !f.control.blockRead.CompareAndSwap(true, false) {
		return file, err
	}
	return &scanAdmissionFile{
		File:    file,
		control: f.control,
	}, nil
}

func (f *scanAdmissionFilesystem) Lstat(name string) (fs.FileInfo, error) {
	if f.control.armed.Load() && name == "deleted" && f.control.blockReconcile.CompareAndSwap(true, false) {
		close(f.control.reconciliation)
		<-f.control.releaseReconcile
	}
	return f.Filesystem.Lstat(name)
}

type scanAdmissionFile struct {
	fs.File
	control *scanAdmissionFilesystemControl
	once    sync.Once
}

func (f *scanAdmissionFile) Read(buf []byte) (int, error) {
	f.once.Do(func() {
		close(f.control.read)
		<-f.control.release
	})
	return f.File.Read(buf)
}

func TestBufferedScanReleasesScanAdmissionAtTraversalCompletion(t *testing.T) {
	testScanReleasesScanAdmissionAtTraversalCompletion(t, 0)
}

func TestStreamingScanReleasesScanAdmissionAtTraversalCompletion(t *testing.T) {
	testScanReleasesScanAdmissionAtTraversalCompletion(t, -1)
}

func TestDeletedFileReconciliationReacquiresFolderConcurrency(t *testing.T) {
	m, controls := newScanAdmissionModel(t, 0)
	writeFile(t, controls["high"].filesystem, "deleted", []byte("deleted"))
	if err := m.ScanFolder("high"); err != nil {
		t.Fatal(err)
	}
	if err := controls["high"].filesystem.Remove("deleted"); err != nil {
		t.Fatal(err)
	}
	for _, control := range controls {
		if err := control.filesystem.Mkdir("traversal", 0o755); err != nil {
			t.Fatal(err)
		}
		control.armed.Store(true)
	}
	controls["high"].blockReconcile.Store(true)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitScanAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal")
	awaitScanAdmissionSignalOrResult(t, controls["high"].reconciliation, highResult, "high deleted-file reconciliation")

	nextResult := make(chan error, 1)
	go func() { nextResult <- m.ScanFolder("next") }()
	awaitFolderStateEvent(t, sub, "next", FolderScanWaiting)
	select {
	case <-controls["next"].walked:
		t.Fatal("next traversal started while deleted-file reconciliation held folder concurrency")
	default:
	}

	close(controls["high"].releaseReconcile)
	awaitScanAdmissionSignalOrResult(t, controls["next"].walked, nextResult, "next traversal after reconciliation")
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
	if err := <-nextResult; err != nil {
		t.Fatal(err)
	}
}

func TestTraversalFailureReleasesFolderConcurrency(t *testing.T) {
	m, controls := newScanAdmissionModel(t, 0)
	if err := controls["low"].filesystem.Mkdir("failure", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := controls["high"].filesystem.Mkdir("traversal", 0o755); err != nil {
		t.Fatal(err)
	}
	controls["low"].failTraversal.Store(true)
	controls["low"].armed.Store(true)
	controls["high"].armed.Store(true)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	awaitScanAdmissionSignalOrResult(t, controls["low"].traversalFailure, lowResult, "controlled traversal failure")

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	close(controls["low"].releaseFailure)
	awaitScanAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal after low traversal failure")
	if err := <-lowResult; err != nil {
		t.Fatal(err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func TestCancelledScanDoesNotLeakScanAdmission(t *testing.T) {
	m, controls := newScanAdmissionModel(t, 0)
	if err := controls["low"].filesystem.Mkdir("blocked", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := controls["high"].filesystem.Mkdir("traversal", 0o755); err != nil {
		t.Fatal(err)
	}
	controls["low"].blockTraversal.Store(true)
	controls["low"].armed.Store(true)
	controls["high"].armed.Store(true)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	runner, ok := m.folderRunners.Get("low")
	if !ok {
		t.Fatal("low folder is not running")
	}
	lowFolder := runner.(*sendReceiveFolder).folder
	scanCtx, cancelScan := context.WithCancel(t.Context())
	lowResult := make(chan error, 1)
	go func() { lowResult <- lowFolder.scanSubdirsWithClass(scanCtx, nil, folderWorkMaintenance) }()
	awaitScanAdmissionSignal(t, controls["low"].traversalBlocked, "blocked low traversal")

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	cancelScan()
	close(controls["low"].releaseTraversal)
	awaitScanAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal after low scan cancellation")
	if err := <-lowResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("low scan returned %v, expected context cancellation", err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func TestResultConsumerFailureDoesNotLeakScanAdmission(t *testing.T) {
	m, controls := newScanAdmissionModel(t, 0)
	writeFile(t, controls["low"].filesystem, "payload", make([]byte, 128<<10))
	for _, control := range controls {
		if err := control.filesystem.Mkdir("traversal", 0o755); err != nil {
			t.Fatal(err)
		}
		control.armed.Store(true)
	}
	controls["low"].blockRead.Store(true)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	awaitScanAdmissionSignalOrResult(t, controls["low"].walked, lowResult, "low traversal")
	awaitScanAdmissionSignal(t, controls["low"].read, "low hashing")

	// The scan batch rechecks folder health before publishing completed-file
	// results. Removing the marker after traversal makes that consumer fail.
	if err := controls["low"].filesystem.Remove(config.DefaultMarkerName); err != nil {
		t.Fatal(err)
	}
	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	awaitScanAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal while low result consumption remains pending")

	close(controls["low"].release)
	if err := <-lowResult; err == nil {
		t.Fatal("low scan succeeded after its result consumer lost folder health")
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}

	nextResult := make(chan error, 1)
	go func() { nextResult <- m.ScanFolder("next") }()
	awaitScanAdmissionSignalOrResult(t, controls["next"].walked, nextResult, "next traversal after result-consumer failure")
	if err := <-nextResult; err != nil {
		t.Fatal(err)
	}
}

func testScanReleasesScanAdmissionAtTraversalCompletion(t *testing.T, scanProgressInterval int) {
	t.Helper()
	m, controls := newScanAdmissionModel(t, scanProgressInterval)
	writeFile(t, controls["low"].filesystem, "payload", make([]byte, 128<<10))
	for _, control := range controls {
		if err := control.filesystem.Mkdir("traversal", 0o755); err != nil {
			t.Fatal(err)
		}
	}
	controls["low"].blockRead.Store(true)
	controls["low"].armed.Store(true)
	controls["high"].armed.Store(true)

	sub := m.evLogger.Subscribe(events.StateChanged)
	defer sub.Unsubscribe()
	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	awaitFolderStateEvent(t, sub, "low", FolderScanWaiting)
	awaitFolderStateEvent(t, sub, "low", FolderScanning)
	awaitScanAdmissionSignalOrResult(t, controls["low"].walked, lowResult, "low traversal")
	awaitScanAdmissionSignal(t, controls["low"].read, "low hashing")

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	awaitScanAdmissionSignal(t, controls["high"].walked, "high traversal while low hashing remains active")

	close(controls["low"].release)
	if err := <-lowResult; err != nil {
		t.Fatal(err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func newScanAdmissionModel(t *testing.T, scanProgressInterval int) (*testModel, map[string]*scanAdmissionFilesystemControl) {
	t.Helper()
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawMaxFolderConcurrency = 1
	controls := make(map[string]*scanAdmissionFilesystemControl, 3)
	for _, folderID := range []string{"low", "high", "next"} {
		root := rand.String(32)
		control := &scanAdmissionFilesystemControl{
			filesystem:       fs.NewFilesystem(fs.FilesystemTypeFake, root+"?content=true"),
			walked:           make(chan struct{}, 1),
			read:             make(chan struct{}),
			release:          make(chan struct{}),
			reconciliation:   make(chan struct{}),
			releaseReconcile: make(chan struct{}),
			traversalFailure: make(chan struct{}),
			releaseFailure:   make(chan struct{}),
			traversalBlocked: make(chan struct{}),
			releaseTraversal: make(chan struct{}),
		}
		scanAdmissionFilesystems.Store(root, control)
		t.Cleanup(func() {
			scanAdmissionFilesystems.Delete(root)
			closeScanAdmissionSignal(control.release)
			closeScanAdmissionSignal(control.releaseReconcile)
			closeScanAdmissionSignal(control.releaseFailure)
			closeScanAdmissionSignal(control.releaseTraversal)
		})
		controls[folderID] = control

		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = root
		folder.FilesystemType = config.FilesystemType(scanAdmissionFilesystemType)
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		folder.ScanProgressIntervalS = scanProgressInterval
		cfg.SetFolder(folder)
	}
	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	m := setupModel(t, wrapper)
	t.Cleanup(func() { cleanupModel(m) })
	return m, controls
}

func awaitScanAdmissionSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitScanAdmissionSignalOrResult(t *testing.T, signal <-chan struct{}, result <-chan error, description string) {
	t.Helper()
	select {
	case <-signal:
	case err := <-result:
		t.Fatalf("scan returned before %s: %v", description, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func closeScanAdmissionSignal(signal chan struct{}) {
	select {
	case <-signal:
	default:
		close(signal)
	}
}
