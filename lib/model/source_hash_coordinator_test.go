// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

const sourceHashCoordinatorFilesystemType fs.FilesystemType = "source-hash-coordinator"

var sourceHashCoordinatorFilesystems sync.Map

func init() {
	fs.RegisterFilesystemType(sourceHashCoordinatorFilesystemType, func(root string, _ ...fs.Option) (fs.Filesystem, error) {
		control, ok := sourceHashCoordinatorFilesystems.Load(root)
		if !ok {
			return nil, errors.New("missing Source Hash Work filesystem")
		}
		return &sourceHashCoordinatorFilesystem{
			Filesystem: control.(*sourceHashCoordinatorFilesystemControl).filesystem,
			control:    control.(*sourceHashCoordinatorFilesystemControl),
		}, nil
	})
}

type sourceHashCoordinatorFilesystemControl struct {
	folder     string
	filesystem fs.Filesystem
	started    chan<- string
	release    <-chan struct{}
	traversed  chan struct{}
	opened     chan struct{}
	armed      atomic.Bool

	traversedOnce sync.Once
	openedOnce    sync.Once
}

type sourceHashCoordinatorFilesystem struct {
	fs.Filesystem
	control *sourceHashCoordinatorFilesystemControl
}

func (f *sourceHashCoordinatorFilesystem) DirNames(name string) ([]string, error) {
	names, err := f.Filesystem.DirNames(name)
	if f.control.armed.Load() {
		f.control.traversedOnce.Do(func() { close(f.control.traversed) })
	}
	return names, err
}

func (f *sourceHashCoordinatorFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil || name != "payload" || !f.control.armed.Load() {
		return file, err
	}
	f.control.openedOnce.Do(func() { close(f.control.opened) })
	return &sourceHashCoordinatorFile{
		File:    file,
		control: f.control,
	}, nil
}

type sourceHashCoordinatorFile struct {
	fs.File
	control *sourceHashCoordinatorFilesystemControl
	once    sync.Once
}

func (f *sourceHashCoordinatorFile) Read(buf []byte) (int, error) {
	f.once.Do(func() {
		f.control.started <- f.control.folder
		<-f.control.release
	})
	return f.File.Read(buf)
}

func TestModelSchedulesSourceHashWorkAfterTraversalRelease(t *testing.T) {
	started := make(chan string, 2)
	lowRelease := make(chan struct{})
	highRelease := make(chan struct{})
	controls := map[string]*sourceHashCoordinatorFilesystemControl{
		"low": {
			folder:     "low",
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			started:    started,
			release:    lowRelease,
			traversed:  make(chan struct{}),
			opened:     make(chan struct{}),
		},
		"high": {
			folder:     "high",
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			started:    started,
			release:    highRelease,
			traversed:  make(chan struct{}),
			opened:     make(chan struct{}),
		},
	}
	t.Cleanup(func() {
		closeSourceHashCoordinatorSignal(lowRelease)
		closeSourceHashCoordinatorSignal(highRelease)
	})

	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawMaxFolderConcurrency = 1
	cfg.Options.RawHashCapacity = 1
	for folderID, priority := range map[string]int{"low": -100, "high": 100} {
		root := rand.String(32)
		control := controls[folderID]
		sourceHashCoordinatorFilesystems.Store(root, control)
		t.Cleanup(func() { sourceHashCoordinatorFilesystems.Delete(root) })

		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = root
		folder.FilesystemType = config.FilesystemType(sourceHashCoordinatorFilesystemType)
		folder.FolderPriority = priority
		folder.Hashers = 1
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		folder.ScanProgressIntervalS = 0
		cfg.SetFolder(folder)
	}

	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	m := setupModel(t, wrapper)
	t.Cleanup(func() { cleanupModel(m) })
	for _, control := range controls {
		writeFile(t, control.filesystem, "payload", make([]byte, protocol.MinBlockSize))
		control.armed.Store(true)
	}

	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	if got := awaitSourceHashCoordinatorStart(t, started); got != "low" {
		t.Fatalf("first Hashing Quantum = %q, want low", got)
	}
	if file, ok := m.testCurrentFolderFile("low", "payload"); ok {
		t.Fatalf("Low published while its Hashing Quantum was active: %+v", file)
	}

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitSourceHashCoordinatorSignal(t, controls["high"].traversed, "High traversal while Low hashing remains active")
	awaitSourceHashCoordinatorSignal(t, controls["high"].opened, "High Source Hash Work enrollment")

	close(lowRelease)
	if got := awaitSourceHashCoordinatorStart(t, started); got != "high" {
		t.Fatalf("next Hashing Quantum = %q, want high", got)
	}
	if file, ok := m.testCurrentFolderFile("high", "payload"); ok {
		t.Fatalf("High published while its Hashing Quantum was active: %+v", file)
	}
	close(highRelease)

	for folder, result := range map[string]<-chan error{"low": lowResult, "high": highResult} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s scan: %v", folder, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s scan", folder)
		}
		file, ok := m.testCurrentFolderFile(folder, "payload")
		if !ok || len(file.Blocks) != 1 || len(file.BlocksHash) == 0 {
			t.Fatalf("%s complete publication = %+v, present=%v", folder, file, ok)
		}
	}
}

func awaitSourceHashCoordinatorStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case folder := <-started:
		return folder
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Hashing Quantum")
		return ""
	}
}

func awaitSourceHashCoordinatorSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(fmt.Sprintf("timed out waiting for %s", description))
	}
}

func closeSourceHashCoordinatorSignal(signal chan struct{}) {
	select {
	case <-signal:
	default:
		close(signal)
	}
}
