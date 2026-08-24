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
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
	"github.com/syncthing/syncthing/lib/scanner"
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
	folder         string
	filesystem     fs.Filesystem
	started        chan<- string
	release        <-chan struct{}
	quantumStarted chan<- string
	quantumRelease <-chan struct{}
	traversed      chan struct{}
	armed          atomic.Bool

	traversedOnce sync.Once
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
	if err != nil || !f.control.armed.Load() {
		return file, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &sourceHashCoordinatorFile{
		File:             file,
		control:          f.control,
		blockSize:        int64(protocol.FileInfo{Size: info.Size()}.BlockSize()),
		remainingInBlock: 0,
	}, nil
}

type sourceHashCoordinatorFile struct {
	fs.File
	control          *sourceHashCoordinatorFilesystemControl
	once             sync.Once
	blockSize        int64
	remainingInBlock int64
}

func (f *sourceHashCoordinatorFile) Read(buf []byte) (int, error) {
	if f.control.quantumStarted != nil {
		if f.remainingInBlock == 0 {
			f.control.quantumStarted <- f.control.folder
			<-f.control.quantumRelease
			f.remainingInBlock = f.blockSize
		}
		n, err := f.File.Read(buf)
		f.remainingInBlock -= int64(n)
		return n, err
	}
	f.once.Do(func() {
		f.control.started <- f.control.folder
		<-f.control.release
	})
	return f.File.Read(buf)
}

type observedSourceHashSubmission struct {
	folder     string
	submission scanner.SourceHashSubmission
}

type sourceHashCoordinatorObserver struct {
	scanner.SourceHashCoordinator
	submitted chan<- observedSourceHashSubmission
}

func (c sourceHashCoordinatorObserver) Submit(ctx context.Context, request scanner.SourceHashRequest) scanner.SourceHashSubmission {
	submission := c.SourceHashCoordinator.Submit(ctx, request)
	c.submitted <- observedSourceHashSubmission{
		folder:     request.Folder.ID,
		submission: submission,
	}
	return submission
}

func TestModelSchedulesSourceHashWorkAfterTraversalRelease(t *testing.T) {
	started := make(chan string, 3)
	blockerRelease := make(chan struct{})
	lowRelease := make(chan struct{})
	highRelease := make(chan struct{})
	controls := map[string]*sourceHashCoordinatorFilesystemControl{
		"blocker": {
			folder:     "blocker",
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			started:    started,
			release:    blockerRelease,
			traversed:  make(chan struct{}),
		},
		"low": {
			folder:     "low",
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			started:    started,
			release:    lowRelease,
			traversed:  make(chan struct{}),
		},
		"high": {
			folder:     "high",
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			started:    started,
			release:    highRelease,
			traversed:  make(chan struct{}),
		},
	}
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawMaxFolderConcurrency = 1
	cfg.Options.RawHashCapacity = 1
	for folderID, priority := range map[string]int{"blocker": 0, "low": -100, "high": 100} {
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
	t.Cleanup(func() {
		closeSourceHashCoordinatorSignal(blockerRelease)
		closeSourceHashCoordinatorSignal(lowRelease)
		closeSourceHashCoordinatorSignal(highRelease)
	})
	submitted := make(chan observedSourceHashSubmission, 3)
	m.sourceHashCoordinator = sourceHashCoordinatorObserver{
		SourceHashCoordinator: m.sourceHashCoordinator,
		submitted:             submitted,
	}
	for _, control := range controls {
		writeFile(t, control.filesystem, "payload", make([]byte, protocol.MinBlockSize))
		control.armed.Store(true)
	}

	blockerResult := make(chan error, 1)
	go func() { blockerResult <- m.ScanFolder("blocker") }()
	if got := awaitSourceHashCoordinatorStart(t, started); got != "blocker" {
		t.Fatalf("first Hashing Quantum = %q, want blocker", got)
	}
	blockerSubmission := awaitSourceHashSubmission(t, submitted)
	if blockerSubmission.folder != "blocker" {
		t.Fatalf("first Source Hash Work submission = %q, want blocker", blockerSubmission.folder)
	}
	awaitSourceHashCoordinatorSignal(t, blockerSubmission.submission.Admitted, "blocker admission")
	if file, ok := m.testCurrentFolderFile("blocker", "payload"); ok {
		t.Fatalf("Blocker published while its Hashing Quantum was active: %+v", file)
	}

	lowResult := make(chan error, 1)
	go func() { lowResult <- m.ScanFolder("low") }()
	awaitSourceHashCoordinatorSignal(t, controls["low"].traversed, "Low traversal while blocker hashing remains active")
	lowSubmission := awaitSourceHashSubmission(t, submitted)
	if lowSubmission.folder != "low" {
		t.Fatalf("second Source Hash Work submission = %q, want low", lowSubmission.folder)
	}
	select {
	case <-lowSubmission.submission.Admitted:
		t.Fatal("Low Hashing Quantum was admitted while blocker occupied the only Hash Capacity slot")
	default:
	}

	highResult := make(chan error, 1)
	go func() { highResult <- m.ScanFolder("high") }()
	awaitSourceHashCoordinatorSignal(t, controls["high"].traversed, "High traversal while blocker hashing remains active")
	highSubmission := awaitSourceHashSubmission(t, submitted)
	if highSubmission.folder != "high" {
		t.Fatalf("third Source Hash Work submission = %q, want high", highSubmission.folder)
	}
	select {
	case <-highSubmission.submission.Admitted:
		t.Fatal("High Hashing Quantum was admitted while blocker occupied the only Hash Capacity slot")
	default:
	}

	close(blockerRelease)
	if got := awaitSourceHashCoordinatorStart(t, started); got != "high" {
		t.Fatalf("first queued admission = %q, want high before earlier Low", got)
	}
	awaitSourceHashCoordinatorSignal(t, highSubmission.submission.Admitted, "High strict-priority admission")
	select {
	case <-lowSubmission.submission.Admitted:
		t.Fatal("Low Hashing Quantum was admitted while High occupied the only Hash Capacity slot")
	default:
	}
	if file, ok := m.testCurrentFolderFile("high", "payload"); ok {
		t.Fatalf("High published while its Hashing Quantum was active: %+v", file)
	}
	close(highRelease)
	awaitSourceHashCoordinatorSignal(t, lowSubmission.submission.Admitted, "Low admission after High completion")
	if got := awaitSourceHashCoordinatorStart(t, started); got != "low" {
		t.Fatalf("admission after High completion = %q, want Low", got)
	}
	close(lowRelease)

	results := map[string]<-chan error{
		"blocker": blockerResult,
		"low":     lowResult,
		"high":    highResult,
	}
	for folder, result := range results {
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

func TestModelSharesEqualPrioritySourceHashWorkByActualBytes(t *testing.T) {
	started := make(chan string, 8)
	releases := map[string]chan struct{}{
		"gate": make(chan struct{}),
		"a":    make(chan struct{}),
		"b":    make(chan struct{}),
	}
	controls := map[string]*sourceHashCoordinatorFilesystemControl{
		"gate": {
			folder:         "gate",
			filesystem:     fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			quantumStarted: started,
			quantumRelease: releases["gate"],
			traversed:      make(chan struct{}),
		},
		"a": {
			folder:         "a",
			filesystem:     fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			quantumStarted: started,
			quantumRelease: releases["a"],
			traversed:      make(chan struct{}),
		},
		"b": {
			folder:         "b",
			filesystem:     fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			quantumStarted: started,
			quantumRelease: releases["b"],
			traversed:      make(chan struct{}),
		},
	}
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawHashCapacity = 2
	for folderID, priority := range map[string]int{"gate": 100, "a": 0, "b": 0} {
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
		folder.Hashers = 2
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		folder.ScanProgressIntervalS = 0
		cfg.SetFolder(folder)
	}

	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	m := setupModel(t, wrapper)
	t.Cleanup(func() { cleanupModel(m) })
	for _, release := range releases {
		release := release
		t.Cleanup(func() { closeSourceHashCoordinatorSignal(release) })
	}
	submitted := make(chan observedSourceHashSubmission, 4)
	m.sourceHashCoordinator = sourceHashCoordinatorObserver{
		SourceHashCoordinator: m.sourceHashCoordinator,
		submitted:             submitted,
	}

	writeFile(t, controls["gate"].filesystem, "payload-1", make([]byte, protocol.MinBlockSize))
	writeFile(t, controls["gate"].filesystem, "payload-2", make([]byte, protocol.MinBlockSize))
	writeFile(t, controls["a"].filesystem, "payload-1", make([]byte, protocol.MinBlockSize))
	writeFile(t, controls["a"].filesystem, "payload-2", make([]byte, protocol.MinBlockSize))
	writeFile(t, controls["b"].filesystem, "payload-1", make([]byte, protocol.MinBlockSize/2))
	writeFile(t, controls["b"].filesystem, "payload-2", make([]byte, protocol.MinBlockSize/2))
	for _, control := range controls {
		control.armed.Store(true)
	}

	scanResults := make(map[string]<-chan error)
	for _, folder := range []string{"gate", "a", "b"} {
		result := make(chan error, 1)
		scanResults[folder] = result
		go func() { result <- m.ScanFolder(folder) }()
		awaitSourceHashCoordinatorSignal(t, controls[folder].traversed, folder+" traversal")
		for range 2 {
			submission := awaitSourceHashSubmission(t, submitted)
			if submission.folder != folder {
				t.Fatalf("Source Hash Work submission = %q, want %q", submission.folder, folder)
			}
		}
		if folder == "gate" {
			for range 2 {
				if got := awaitSourceHashCoordinatorStart(t, started); got != "gate" {
					t.Fatalf("gate Hashing Quantum = %q, want gate", got)
				}
			}
		}
	}

	releases["gate"] <- struct{}{}
	if got := awaitSourceHashCoordinatorStart(t, started); got != "a" {
		t.Fatalf("first equal-priority admission = %q, want a", got)
	}
	releases["gate"] <- struct{}{}
	if got := awaitSourceHashCoordinatorStart(t, started); got != "b" {
		t.Fatalf("admission while A had one active block = %q, want b", got)
	}
	for index, want := range []string{"b", "a"} {
		releases[map[string]string{"b": "a", "a": "b"}[want]] <- struct{}{}
		if got := awaitSourceHashCoordinatorStart(t, started); got != want {
			t.Fatalf("byte-fair replacement %d = %q, want %q", index+1, got, want)
		}
	}
	releases["a"] <- struct{}{}
	releases["b"] <- struct{}{}

	for folder, result := range scanResults {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s scan: %v", folder, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s scan", folder)
		}
	}
	for folder, files := range map[string]map[string]int64{
		"gate": {
			"payload-1": protocol.MinBlockSize,
			"payload-2": protocol.MinBlockSize,
		},
		"a": {
			"payload-1": protocol.MinBlockSize,
			"payload-2": protocol.MinBlockSize,
		},
		"b": {
			"payload-1": protocol.MinBlockSize / 2,
			"payload-2": protocol.MinBlockSize / 2,
		},
	} {
		for name, size := range files {
			file, ok := m.testCurrentFolderFile(folder, name)
			if !ok || file.Size != size || len(file.Blocks) == 0 || len(file.BlocksHash) == 0 {
				t.Fatalf("%s/%s complete publication = %+v, present=%v", folder, name, file, ok)
			}
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
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitSourceHashSubmission(t *testing.T, submitted <-chan observedSourceHashSubmission) observedSourceHashSubmission {
	t.Helper()
	select {
	case submission := <-submitted:
		return submission
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Source Hash Work submission")
		return observedSourceHashSubmission{}
	}
}

func closeSourceHashCoordinatorSignal(signal chan struct{}) {
	select {
	case <-signal:
	default:
		close(signal)
	}
}
