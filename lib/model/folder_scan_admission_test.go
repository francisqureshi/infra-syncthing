// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
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

const traversalAdmissionFilesystemType fs.FilesystemType = "traversal-admission"

var traversalAdmissionFilesystems sync.Map

func init() {
	fs.RegisterFilesystemType(traversalAdmissionFilesystemType, func(root string, _ ...fs.Option) (fs.Filesystem, error) {
		control, ok := traversalAdmissionFilesystems.Load(root)
		if !ok {
			return nil, errors.New("missing traversal-admission filesystem")
		}
		return &traversalAdmissionFilesystem{
			Filesystem: control.(*traversalAdmissionFilesystemControl).filesystem,
			control:    control.(*traversalAdmissionFilesystemControl),
		}, nil
	})
}

type traversalAdmissionFilesystemControl struct {
	filesystem       fs.Filesystem
	walked           chan struct{}
	read             chan struct{}
	release          chan struct{}
	reconciliation   chan struct{}
	releaseReconcile chan struct{}
	traversalFailure chan struct{}
	releaseFailure   chan struct{}
	armed            atomic.Bool
	blockRead        atomic.Bool
	blockReconcile   atomic.Bool
	failTraversal    atomic.Bool
}

type traversalAdmissionFilesystem struct {
	fs.Filesystem
	control *traversalAdmissionFilesystemControl
}

func (f *traversalAdmissionFilesystem) DirNames(name string) ([]string, error) {
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

func (f *traversalAdmissionFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil || name != "payload" || !f.control.armed.Load() || !f.control.blockRead.CompareAndSwap(true, false) {
		return file, err
	}
	return &traversalAdmissionFile{
		File:    file,
		control: f.control,
	}, nil
}

func (f *traversalAdmissionFilesystem) Lstat(name string) (fs.FileInfo, error) {
	if f.control.armed.Load() && name == "deleted" && f.control.blockReconcile.CompareAndSwap(true, false) {
		close(f.control.reconciliation)
		<-f.control.releaseReconcile
	}
	return f.Filesystem.Lstat(name)
}

type traversalAdmissionFile struct {
	fs.File
	control *traversalAdmissionFilesystemControl
	once    sync.Once
}

func (f *traversalAdmissionFile) Read(buf []byte) (int, error) {
	f.once.Do(func() {
		close(f.control.read)
		<-f.control.release
	})
	return f.File.Read(buf)
}

func TestBufferedScanReleasesTraversalAdmissionBeforeHashingCompletes(t *testing.T) {
	testScanReleasesTraversalAdmissionBeforeHashingCompletes(t, 0)
}

func TestStreamingScanReleasesTraversalAdmissionBeforeHashingCompletes(t *testing.T) {
	testScanReleasesTraversalAdmissionBeforeHashingCompletes(t, -1)
}

func TestDeletedFileReconciliationReacquiresFolderConcurrency(t *testing.T) {
	m, controls := newTraversalAdmissionModel(t, 0)
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
	awaitTraversalAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal")
	awaitTraversalAdmissionSignalOrResult(t, controls["high"].reconciliation, highResult, "high deleted-file reconciliation")

	nextResult := make(chan error, 1)
	go func() { nextResult <- m.ScanFolder("next") }()
	awaitFolderStateEvent(t, sub, "next", FolderScanWaiting)
	select {
	case <-controls["next"].walked:
		t.Fatal("next traversal started while deleted-file reconciliation held folder concurrency")
	default:
	}

	close(controls["high"].releaseReconcile)
	awaitTraversalAdmissionSignalOrResult(t, controls["next"].walked, nextResult, "next traversal after reconciliation")
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
	if err := <-nextResult; err != nil {
		t.Fatal(err)
	}
}

func TestTraversalFailureReleasesFolderConcurrency(t *testing.T) {
	m, controls := newTraversalAdmissionModel(t, 0)
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
	awaitTraversalAdmissionSignalOrResult(t, controls["low"].traversalFailure, lowResult, "controlled traversal failure")

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	close(controls["low"].releaseFailure)
	awaitTraversalAdmissionSignalOrResult(t, controls["high"].walked, highResult, "high traversal after low traversal failure")
	if err := <-lowResult; err != nil {
		t.Fatal(err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func testScanReleasesTraversalAdmissionBeforeHashingCompletes(t *testing.T, scanProgressInterval int) {
	t.Helper()
	m, controls := newTraversalAdmissionModel(t, scanProgressInterval)
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
	awaitTraversalAdmissionSignalOrResult(t, controls["low"].walked, lowResult, "low traversal")
	awaitTraversalAdmissionSignal(t, controls["low"].read, "low hashing")

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitFolderStateEvent(t, sub, "high", FolderScanWaiting)
	awaitTraversalAdmissionSignal(t, controls["high"].walked, "high traversal while low hashing remains active")

	close(controls["low"].release)
	if err := <-lowResult; err != nil {
		t.Fatal(err)
	}
	if err := <-highResult; err != nil {
		t.Fatal(err)
	}
}

func newTraversalAdmissionModel(t *testing.T, scanProgressInterval int) (*testModel, map[string]*traversalAdmissionFilesystemControl) {
	t.Helper()
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawMaxFolderConcurrency = 1
	controls := make(map[string]*traversalAdmissionFilesystemControl, 3)
	for _, folderID := range []string{"low", "high", "next"} {
		root := rand.String(32)
		control := &traversalAdmissionFilesystemControl{
			filesystem:       fs.NewFilesystem(fs.FilesystemTypeFake, root+"?content=true"),
			walked:           make(chan struct{}, 1),
			read:             make(chan struct{}),
			release:          make(chan struct{}),
			reconciliation:   make(chan struct{}),
			releaseReconcile: make(chan struct{}),
			traversalFailure: make(chan struct{}),
			releaseFailure:   make(chan struct{}),
		}
		traversalAdmissionFilesystems.Store(root, control)
		t.Cleanup(func() {
			traversalAdmissionFilesystems.Delete(root)
			select {
			case <-control.release:
			default:
				close(control.release)
			}
			select {
			case <-control.releaseReconcile:
			default:
				close(control.releaseReconcile)
			}
			select {
			case <-control.releaseFailure:
			default:
				close(control.releaseFailure)
			}
		})
		controls[folderID] = control

		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = root
		folder.FilesystemType = config.FilesystemType(traversalAdmissionFilesystemType)
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

func awaitTraversalAdmissionSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitTraversalAdmissionSignalOrResult(t *testing.T, signal <-chan struct{}, result <-chan error, description string) {
	t.Helper()
	select {
	case <-signal:
	case err := <-result:
		t.Fatalf("scan returned before %s: %v", description, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
