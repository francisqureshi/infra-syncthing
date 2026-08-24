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
	handleClosed   chan struct{}
	armed          atomic.Bool
	opens          atomic.Int64

	traversedOnce    sync.Once
	handleClosedOnce sync.Once
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
	f.control.opens.Add(1)
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
	closeOnce        sync.Once
}

func (f *sourceHashCoordinatorFile) Close() error {
	var err error
	f.closeOnce.Do(func() {
		err = f.File.Close()
		if f.control.handleClosed != nil {
			f.control.handleClosedOnce.Do(func() { close(f.control.handleClosed) })
		}
	})
	return err
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
	folder      string
	submission  scanner.SourceHashSubmission
	contextDone <-chan struct{}
}

type sourceHashCoordinatorObserver struct {
	scanner.SourceHashCoordinator
	submitted  chan<- observedSourceHashSubmission
	configured chan<- observedSourceHashConfiguration
}

type observedSourceHashConfiguration struct {
	capacity   int
	priorities map[string]int
}

func (c sourceHashCoordinatorObserver) Configure(capacity int, priorities map[string]int) {
	c.SourceHashCoordinator.Configure(capacity, priorities)
	if c.configured != nil {
		c.configured <- observedSourceHashConfiguration{
			capacity:   capacity,
			priorities: priorities,
		}
	}
}

func (c sourceHashCoordinatorObserver) Submit(ctx context.Context, request scanner.SourceHashRequest) scanner.SourceHashSubmission {
	submission := c.SourceHashCoordinator.Submit(ctx, request)
	callerCompletion := make(chan scanner.SourceHashCompletion, 1)
	observedCompletion := make(chan scanner.SourceHashCompletion, 1)
	go func() {
		completion, ok := <-submission.Completion
		if ok {
			callerCompletion <- completion
			observedCompletion <- completion
		}
		close(callerCompletion)
		close(observedCompletion)
	}()
	callerSubmission := scanner.SourceHashSubmission{
		Admitted:   submission.Admitted,
		Completion: callerCompletion,
	}
	c.submitted <- observedSourceHashSubmission{
		folder:      request.Folder.ID,
		contextDone: ctx.Done(),
		submission: scanner.SourceHashSubmission{
			Admitted:   submission.Admitted,
			Completion: observedCompletion,
		},
	}
	return callerSubmission
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

func TestModelAppliesLiveFolderPriorityToQueuedSourceHashWork(t *testing.T) {
	harness := newSourceHashModelTestHarness(t, 1, map[string]sourceHashModelTestFolder{
		"gate":  {priority: 100},
		"bulk":  {priority: 0},
		"focus": {priority: -100},
	})

	scanResults := make(map[string]<-chan error, len(harness.controls))
	for _, folder := range []string{"gate", "bulk", "focus"} {
		result := make(chan error, 1)
		scanResults[folder] = result
		go func() { result <- harness.model.ScanFolder(folder) }()
		awaitSourceHashCoordinatorSignal(t, harness.controls[folder].traversed, folder+" traversal")
		submission := awaitSourceHashSubmission(t, harness.submitted)
		if submission.folder != folder {
			t.Fatalf("Source Hash Work submission = %q, want %q", submission.folder, folder)
		}
		if folder == "gate" {
			if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "gate" {
				t.Fatalf("active Hashing Quantum = %q, want gate", got)
			}
		}
	}

	found := false
	waiter, err := harness.wrapper.Modify(func(cfg *config.Configuration) {
		folder, index, ok := cfg.Folder("focus")
		if !ok {
			return
		}
		found = true
		folder.FolderPriority = 100
		cfg.Folders[index] = folder
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
	if !found {
		t.Fatal("focus Folder disappeared before reprioritization")
	}
	select {
	case got := <-harness.started:
		t.Fatalf("live priority change preempted active Hashing Quantum with %q", got)
	default:
	}

	close(harness.releases["gate"])
	if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "focus" {
		t.Fatalf("first admission after live reprioritization = %q, want focus", got)
	}
	close(harness.releases["focus"])
	if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "bulk" {
		t.Fatalf("remaining admission = %q, want bulk", got)
	}
	close(harness.releases["bulk"])

	for folder, result := range scanResults {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s scan: %v", folder, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s scan", folder)
		}
		file, ok := harness.model.testCurrentFolderFile(folder, "payload")
		if !ok || len(file.Blocks) != 1 || len(file.BlocksHash) == 0 {
			t.Fatalf("%s complete publication = %+v, present=%v", folder, file, ok)
		}
	}
}

func TestModelAppliesValidHashCapacityGrowthLiveAfterAtomicValidationFailure(t *testing.T) {
	harness := newSourceHashModelTestHarness(t, 1, map[string]sourceHashModelTestFolder{
		"a": {},
		"b": {},
	})

	results := make(map[string]<-chan error, 2)
	var bSubmission scanner.SourceHashSubmission
	for _, folder := range []string{"a", "b"} {
		result := make(chan error, 1)
		results[folder] = result
		go func() { result <- harness.model.ScanFolder(folder) }()
		awaitSourceHashCoordinatorSignal(t, harness.controls[folder].traversed, folder+" traversal")
		submission := awaitSourceHashSubmission(t, harness.submitted)
		if submission.folder != folder {
			t.Fatalf("Source Hash Work submission = %q, want %q", submission.folder, folder)
		}
		if folder == "a" {
			if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "a" {
				t.Fatalf("initial Hashing Quantum = %q, want a", got)
			}
		} else {
			bSubmission = submission.submission
		}
	}

	if _, err := harness.wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.RawHashCapacity = -1
	}); err == nil {
		t.Fatal("negative Hash Capacity was accepted")
	}
	if got := harness.wrapper.Options().RawHashCapacity; got != 1 {
		t.Fatalf("stored Hash Capacity = %d after rejection, want 1", got)
	}
	select {
	case <-bSubmission.Admitted:
		t.Fatal("rejected Hash Capacity change altered active admission capacity")
	default:
	}

	waiter, err := harness.wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.RawHashCapacity = 2
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
	if harness.wrapper.RequiresRestart() {
		t.Fatal("live Hash Capacity growth unexpectedly requires restart")
	}
	awaitSourceHashCoordinatorSignal(t, bSubmission.Admitted, "live Hash Capacity growth")
	if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "b" {
		t.Fatalf("growth admission = %q, want b", got)
	}

	close(harness.releases["a"])
	close(harness.releases["b"])
	for folder, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s scan: %v", folder, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s scan", folder)
		}
	}
}

func TestModelDrainsLiveHashCapacityShrinkBeforeReplacement(t *testing.T) {
	harness := newSourceHashModelTestHarness(t, 2, map[string]sourceHashModelTestFolder{
		"a": {},
		"b": {},
		"c": {},
	})

	results := make(map[string]<-chan error, 3)
	submissions := make(map[string]scanner.SourceHashSubmission, 3)
	for _, folder := range []string{"a", "b", "c"} {
		result := make(chan error, 1)
		results[folder] = result
		go func() { result <- harness.model.ScanFolder(folder) }()
		awaitSourceHashCoordinatorSignal(t, harness.controls[folder].traversed, folder+" traversal")
		submission := awaitSourceHashSubmission(t, harness.submitted)
		if submission.folder != folder {
			t.Fatalf("Source Hash Work submission = %q, want %q", submission.folder, folder)
		}
		submissions[folder] = submission.submission
	}
	initial := map[string]bool{"a": false, "b": false}
	for range 2 {
		got := awaitSourceHashCoordinatorStart(t, harness.started)
		if _, ok := initial[got]; !ok || initial[got] {
			t.Fatalf("unexpected initial Hashing Quantum %q", got)
		}
		initial[got] = true
	}
	select {
	case <-submissions["c"].Admitted:
		t.Fatal("third Folder admitted before Hash Capacity shrink")
	default:
	}

	waiter, err := harness.wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.RawHashCapacity = 1
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
	observation := awaitSourceHashConfiguration(t, harness.configured, "live Hash Capacity shrink")
	if observation.capacity != 1 {
		t.Fatalf("configured Hash Capacity = %d, want 1", observation.capacity)
	}
	if harness.wrapper.RequiresRestart() {
		t.Fatal("live Hash Capacity shrink unexpectedly requires restart")
	}

	close(harness.releases["a"])
	completion := awaitSourceHashCompletion(t, submissions["a"].Completion, "first grandfathered completion")
	if completion.Err != nil {
		t.Fatal(completion.Err)
	}
	select {
	case <-submissions["c"].Admitted:
		t.Fatal("replacement admitted while usage equaled shrunken Hash Capacity")
	default:
	}

	close(harness.releases["b"])
	completion = awaitSourceHashCompletion(t, submissions["b"].Completion, "second grandfathered completion")
	if completion.Err != nil {
		t.Fatal(completion.Err)
	}
	awaitSourceHashCoordinatorSignal(t, submissions["c"].Admitted, "post-drain replacement")
	if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "c" {
		t.Fatalf("post-drain Hashing Quantum = %q, want c", got)
	}
	close(harness.releases["c"])
	completion = awaitSourceHashCompletion(t, submissions["c"].Completion, "replacement completion")
	if completion.Err != nil {
		t.Fatal(completion.Err)
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
	}
}

func TestModelFolderLifecycleCleansUpActiveSourceHashWorkAtHashingQuantumBoundary(t *testing.T) {
	for _, lifecycle := range []string{"pause", "remove"} {
		t.Run(lifecycle, func(t *testing.T) {
			harness := newSourceHashModelTestHarness(t, 1, map[string]sourceHashModelTestFolder{
				"victim": {
					size:                   2 * protocol.MinBlockSize,
					gateEachHashingQuantum: true,
					observeHandleClose:     true,
				},
			})
			control := harness.controls["victim"]
			handleClosed := control.handleClosed

			scanResult := make(chan error, 1)
			go func() { scanResult <- harness.model.ScanFolder("victim") }()
			awaitSourceHashCoordinatorSignal(t, control.traversed, "victim traversal")
			submission := awaitSourceHashSubmission(t, harness.submitted)
			if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "victim" {
				t.Fatalf("active Hashing Quantum = %q, want victim", got)
			}

			var waiter config.Waiter
			var err error
			switch lifecycle {
			case "pause":
				waiter, err = harness.wrapper.Modify(func(cfg *config.Configuration) {
					folder, index, ok := cfg.Folder("victim")
					if !ok {
						return
					}
					folder.Paused = true
					cfg.Folders[index] = folder
				})
			case "remove":
				waiter, err = harness.wrapper.RemoveFolder("victim")
			}
			if err != nil {
				t.Fatal(err)
			}
			committed := make(chan struct{})
			go func() {
				waiter.Wait()
				close(committed)
			}()
			observation := awaitSourceHashConfiguration(t, harness.configured, lifecycle+" configuration")
			if _, ok := observation.priorities["victim"]; ok {
				t.Fatalf("%s kept victim runnable in Source Hash Work configuration", lifecycle)
			}
			select {
			case completed := <-submission.submission.Completion:
				t.Fatalf("%s preempted active Hashing Quantum: %+v", lifecycle, completed)
			default:
			}
			select {
			case <-handleClosed:
				t.Fatalf("%s closed source handle before active block boundary", lifecycle)
			default:
			}

			close(harness.releases["victim"])
			completed := awaitSourceHashCompletion(t, submission.submission.Completion, lifecycle+" active cleanup")
			if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != protocol.MinBlockSize || completed.File.Name != "" {
				t.Fatalf("%s completion = %+v, want one charged block, no file, and context cancellation", lifecycle, completed)
			}
			awaitSourceHashCoordinatorSignal(t, handleClosed, lifecycle+" source handle cleanup")
			awaitSourceHashCoordinatorSignal(t, committed, lifecycle+" configuration commit")
			select {
			case got := <-harness.started:
				t.Fatalf("%s admitted another Hashing Quantum after cleanup: %q", lifecycle, got)
			default:
			}
			select {
			case <-scanResult:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for scan after Folder %s", lifecycle)
			}
			if lifecycle == "pause" {
				if file, ok := harness.model.testCurrentFolderFile("victim", "payload"); ok {
					t.Fatalf("pause published incomplete Source Hash Work: %+v", file)
				}
			}
		})
	}
}

func TestModelFolderLifecycleCancelsQueuedSourceHashWork(t *testing.T) {
	for _, lifecycle := range []string{"pause", "remove"} {
		t.Run(lifecycle, func(t *testing.T) {
			harness := newSourceHashModelTestHarness(t, 1, map[string]sourceHashModelTestFolder{
				"gate": {},
				"victim": {
					observeHandleClose: true,
				},
			})
			victimClosed := harness.controls["victim"].handleClosed

			gateScan := make(chan error, 1)
			go func() { gateScan <- harness.model.ScanFolder("gate") }()
			awaitSourceHashCoordinatorSignal(t, harness.controls["gate"].traversed, "gate traversal")
			gateSubmission := awaitSourceHashSubmission(t, harness.submitted)
			if gateSubmission.folder != "gate" {
				t.Fatalf("first Source Hash Work submission = %q, want gate", gateSubmission.folder)
			}
			if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "gate" {
				t.Fatalf("active Hashing Quantum = %q, want gate", got)
			}

			victimScan := make(chan error, 1)
			go func() { victimScan <- harness.model.ScanFolder("victim") }()
			awaitSourceHashCoordinatorSignal(t, harness.controls["victim"].traversed, "victim traversal")
			victimSubmission := awaitSourceHashSubmission(t, harness.submitted)
			if victimSubmission.folder != "victim" {
				t.Fatalf("queued Source Hash Work submission = %q, want victim", victimSubmission.folder)
			}
			select {
			case <-victimSubmission.submission.Admitted:
				t.Fatal("victim admitted while gate occupied the only Hash Capacity slot")
			default:
			}

			var waiter config.Waiter
			var err error
			switch lifecycle {
			case "pause":
				waiter, err = harness.wrapper.Modify(func(cfg *config.Configuration) {
					folder, index, ok := cfg.Folder("victim")
					if !ok {
						return
					}
					folder.Paused = true
					cfg.Folders[index] = folder
				})
			case "remove":
				waiter, err = harness.wrapper.RemoveFolder("victim")
			}
			if err != nil {
				t.Fatal(err)
			}
			committed := make(chan struct{})
			go func() {
				waiter.Wait()
				close(committed)
			}()
			observation := awaitSourceHashConfiguration(t, harness.configured, lifecycle+" queued cancellation")
			if _, ok := observation.priorities["victim"]; ok {
				t.Fatalf("%s kept queued victim runnable", lifecycle)
			}
			completed := awaitSourceHashCompletion(t, victimSubmission.submission.Completion, lifecycle+" queued completion")
			if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != 0 || completed.File.Name != "" {
				t.Fatalf("%s queued completion = %+v, want zero bytes, no file, and context cancellation", lifecycle, completed)
			}
			if got := harness.controls["victim"].opens.Load(); got != 0 {
				t.Fatalf("%s queued Source Hash Work opened %d source handles, want zero", lifecycle, got)
			}
			select {
			case <-victimClosed:
				t.Fatalf("%s queued Source Hash Work closed a handle that should never have opened", lifecycle)
			default:
			}
			select {
			case <-victimSubmission.submission.Admitted:
				t.Fatalf("%s admitted canceled queued Source Hash Work", lifecycle)
			default:
			}
			select {
			case got := <-harness.started:
				t.Fatalf("%s started unexpected Hashing Quantum %q", lifecycle, got)
			default:
			}
			awaitSourceHashCoordinatorSignal(t, committed, lifecycle+" queued configuration commit")
			select {
			case <-victimScan:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for victim scan after %s", lifecycle)
			}
			if lifecycle == "pause" {
				if file, ok := harness.model.testCurrentFolderFile("victim", "payload"); ok {
					t.Fatalf("pause published canceled queued Source Hash Work: %+v", file)
				}
			}

			close(harness.releases["gate"])
			completion := awaitSourceHashCompletion(t, gateSubmission.submission.Completion, "gate completion")
			if completion.Err != nil {
				t.Fatal(completion.Err)
			}
			select {
			case err := <-gateScan:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for gate scan")
			}
		})
	}
}

func TestModelPositiveFolderHashersChangeRestartsActiveSourceHashWork(t *testing.T) {
	harness := newSourceHashModelTestHarness(t, 2, map[string]sourceHashModelTestFolder{
		"folder": {
			size:                   2 * protocol.MinBlockSize,
			gateEachHashingQuantum: true,
			observeHandleClose:     true,
		},
	})
	control := harness.controls["folder"]
	handleClosed := control.handleClosed

	scanResult := make(chan error, 1)
	go func() { scanResult <- harness.model.ScanFolder("folder") }()
	awaitSourceHashCoordinatorSignal(t, control.traversed, "Folder traversal")
	submission := awaitSourceHashSubmission(t, harness.submitted)
	if got := awaitSourceHashCoordinatorStart(t, harness.started); got != "folder" {
		t.Fatalf("active Hashing Quantum = %q, want folder", got)
	}

	waiter, err := harness.wrapper.Modify(func(cfg *config.Configuration) {
		folder, index, ok := cfg.Folder("folder")
		if !ok {
			return
		}
		folder.Hashers = 2
		cfg.Folders[index] = folder
	})
	if err != nil {
		t.Fatal(err)
	}
	committed := make(chan struct{})
	go func() {
		waiter.Wait()
		close(committed)
	}()
	observation := awaitSourceHashConfiguration(t, harness.configured, "positive per-Folder hashers change")
	if _, ok := observation.priorities["folder"]; !ok {
		t.Fatal("positive per-Folder hashers change made Folder unavailable")
	}
	awaitSourceHashCoordinatorSignal(t, submission.contextDone, "Folder restart cancellation")
	select {
	case completed := <-submission.submission.Completion:
		t.Fatalf("Folder restart preempted active Hashing Quantum: %+v", completed)
	default:
	}
	select {
	case <-handleClosed:
		t.Fatal("Folder restart closed source handle before active Hashing Quantum boundary")
	default:
	}

	close(harness.releases["folder"])
	completed := awaitSourceHashCompletion(t, submission.submission.Completion, "restarted Source Hash Work cleanup")
	if !errors.Is(completed.Err, context.Canceled) || completed.Bytes != protocol.MinBlockSize || completed.File.Name != "" {
		t.Fatalf("restart completion = %+v, want one charged block, no file, and context cancellation", completed)
	}
	awaitSourceHashCoordinatorSignal(t, handleClosed, "restarted source handle cleanup")
	awaitSourceHashCoordinatorSignal(t, committed, "positive per-Folder hashers restart commit")
	if got := harness.model.numHashers("folder"); got != 2 {
		t.Fatalf("post-restart per-Folder hasher ceiling = %d, want 2", got)
	}
	select {
	case got := <-harness.started:
		t.Fatalf("restarted Source Hash Work continued with %q", got)
	default:
	}
	select {
	case <-scanResult:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scan canceled by per-Folder hashers restart")
	}
	if file, ok := harness.model.testCurrentFolderFile("folder", "payload"); ok {
		t.Fatalf("per-Folder hashers restart published incomplete Source Hash Work: %+v", file)
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
		submissions := 2
		if folder == "b" {
			submissions = 1
		}
		for range submissions {
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
	if submission := awaitSourceHashSubmission(t, submitted); submission.folder != "b" {
		t.Fatalf("Source Hash Work submission after bounded-window release = %q, want b", submission.folder)
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

type sourceHashModelTestFolder struct {
	priority               int
	size                   int
	gateEachHashingQuantum bool
	observeHandleClose     bool
}

type sourceHashModelTestHarness struct {
	wrapper    config.Wrapper
	model      *testModel
	started    chan string
	releases   map[string]chan struct{}
	controls   map[string]*sourceHashCoordinatorFilesystemControl
	submitted  chan observedSourceHashSubmission
	configured chan observedSourceHashConfiguration
}

func newSourceHashModelTestHarness(t *testing.T, capacity int, folders map[string]sourceHashModelTestFolder) *sourceHashModelTestHarness {
	t.Helper()
	harness := &sourceHashModelTestHarness{
		started:    make(chan string, 2*len(folders)),
		releases:   make(map[string]chan struct{}, len(folders)),
		controls:   make(map[string]*sourceHashCoordinatorFilesystemControl, len(folders)),
		submitted:  make(chan observedSourceHashSubmission, len(folders)),
		configured: make(chan observedSourceHashConfiguration, 1),
	}
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.RawHashCapacity = capacity

	for folderID, spec := range folders {
		release := make(chan struct{})
		control := &sourceHashCoordinatorFilesystemControl{
			folder:     folderID,
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			traversed:  make(chan struct{}),
		}
		if spec.gateEachHashingQuantum {
			control.quantumStarted = harness.started
			control.quantumRelease = release
		} else {
			control.started = harness.started
			control.release = release
		}
		if spec.observeHandleClose {
			control.handleClosed = make(chan struct{})
		}
		harness.releases[folderID] = release
		harness.controls[folderID] = control

		root := rand.String(32)
		sourceHashCoordinatorFilesystems.Store(root, control)
		t.Cleanup(func() { sourceHashCoordinatorFilesystems.Delete(root) })
		t.Cleanup(func() { closeSourceHashCoordinatorSignal(release) })

		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = root
		folder.FilesystemType = config.FilesystemType(sourceHashCoordinatorFilesystemType)
		folder.FolderPriority = spec.priority
		folder.Hashers = 1
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		folder.ScanProgressIntervalS = 0
		cfg.SetFolder(folder)
	}

	var cancel context.CancelFunc
	harness.wrapper, cancel = newConfigWrapper(cfg)
	t.Cleanup(cancel)
	harness.model = setupModel(t, harness.wrapper)
	t.Cleanup(func() { cleanupModel(harness.model) })
	harness.model.sourceHashCoordinator = sourceHashCoordinatorObserver{
		SourceHashCoordinator: harness.model.sourceHashCoordinator,
		submitted:             harness.submitted,
		configured:            harness.configured,
	}
	for folderID, spec := range folders {
		size := spec.size
		if size == 0 {
			size = protocol.MinBlockSize
		}
		control := harness.controls[folderID]
		writeFile(t, control.filesystem, "payload", make([]byte, size))
		control.armed.Store(true)
	}
	return harness
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

func awaitSourceHashConfiguration(t *testing.T, configured <-chan observedSourceHashConfiguration, description string) observedSourceHashConfiguration {
	t.Helper()
	select {
	case configuration := <-configured:
		return configuration
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return observedSourceHashConfiguration{}
	}
}

func awaitSourceHashCompletion(t *testing.T, completion <-chan scanner.SourceHashCompletion, description string) scanner.SourceHashCompletion {
	t.Helper()
	select {
	case result := <-completion:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return scanner.SourceHashCompletion{}
	}
}

func closeSourceHashCoordinatorSignal(signal chan struct{}) {
	select {
	case <-signal:
	default:
		close(signal)
	}
}
