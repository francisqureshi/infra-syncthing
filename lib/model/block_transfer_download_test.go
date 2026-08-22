// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
	protocolmocks "github.com/syncthing/syncthing/lib/protocol/mocks"
)

func TestModelRequestGlobalPrioritizesQueuedDownloadBlockTransfers(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{
		"active": 0,
		"low":    -100,
		"high":   100,
	})
	configureDownloadBlockTransferSchedulerForTest(m.model, 6, map[string]int{"active": 0, "low": -100, "high": 100})
	started := make(chan outgoingBlockTransferStart, 3)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
	addOutgoingBlockTransferConnection(m.model, device2, "connection-device-2", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)

	activeResult := requestOutgoingBlockTransfer(t, m.model, device1, "active", 4)
	awaitEnqueuedBlockTransfer(t, enqueued, "active")
	active := awaitOutgoingBlockTransferStart(t, started)

	highResult := requestOutgoingBlockTransfer(t, m.model, device2, "high", 6)
	awaitEnqueuedBlockTransfer(t, enqueued, "high")
	lowResult := requestOutgoingBlockTransfer(t, m.model, device1, "low", 2)
	awaitEnqueuedBlockTransfer(t, enqueued, "low")
	assertNoOutgoingBlockTransferStarted(t, started)

	close(active.release)
	awaitOutgoingBlockTransferResult(t, activeResult)
	high := awaitOutgoingBlockTransferStart(t, started)
	if high.folder != "high" {
		t.Fatalf("first queued download Block Transfer is for folder %q, want high", high.folder)
	}
	if high.device != device2 {
		t.Fatalf("first queued download Block Transfer uses device %s, want %s", high.device, device2)
	}
	close(high.release)
	awaitOutgoingBlockTransferResult(t, highResult)

	low := awaitOutgoingBlockTransferStart(t, started)
	if low.folder != "low" {
		t.Fatalf("second queued download Block Transfer is for folder %q, want low", low.folder)
	}
	close(low.release)
	awaitOutgoingBlockTransferResult(t, lowResult)
}

func TestNetworkPriorityLiveReprioritizationThroughModelRequestGlobal(t *testing.T) {
	const activeSize = protocol.MaxRequestSize
	wrapper, _ := newBlockTransferRequestConfigWithLimits(t, map[string]int{
		"active": 0,
		"bulk":   0,
		"focus":  -100,
	}, 0, -1, device1)
	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.RawMaxConcurrentOutgoingRequestKiB = activeSize / 1024
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
	m := setupModel(t, wrapper)
	defer cleanupModel(m)
	started := make(chan outgoingBlockTransferStart, 3)
	connection := addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
	requestCtx, cancelRequests := context.WithCancel(t.Context())
	defer func() {
		cancelRequests()
		m.Closed(connection, protocol.ErrClosed)
	}()
	enqueued := observeEnqueuedBlockTransfers(m.model)

	activeResult := requestOutgoingBlockTransferContext(requestCtx, m.model, device1, "active", activeSize)
	awaitEnqueuedBlockTransfer(t, enqueued, "active")
	active := awaitOutgoingBlockTransferStart(t, started)
	bulkResult := requestOutgoingBlockTransferContext(requestCtx, m.model, device1, "bulk", activeSize)
	awaitEnqueuedBlockTransfer(t, enqueued, "bulk")
	focusResult := requestOutgoingBlockTransferContext(requestCtx, m.model, device1, "focus", activeSize)
	awaitEnqueuedBlockTransfer(t, enqueued, "focus")

	found := false
	waiter, err = wrapper.Modify(func(cfg *config.Configuration) {
		folder, index, ok := cfg.Folder("focus")
		if !ok {
			return
		}
		found = true
		folder.NetworkPriority = 100
		cfg.Folders[index] = folder
	})
	if err != nil {
		close(active.release)
		t.Fatal(err)
	}
	waiter.Wait()
	if !found {
		close(active.release)
		t.Fatal("focus folder disappeared before reprioritization")
	}
	m.downloadScheduler.mut.Lock()
	focusPriority := m.downloadScheduler.folders["focus"].priority
	globalLimit := m.downloadScheduler.globalLimit
	globalInFlight := m.downloadScheduler.globalInFlight
	m.downloadScheduler.mut.Unlock()
	if focusPriority != 100 {
		close(active.release)
		t.Fatalf("download scheduler retained focus priority %d, want 100", focusPriority)
	}
	if globalLimit != activeSize || globalInFlight != activeSize {
		close(active.release)
		t.Fatalf("download capacity after reprioritization is %d/%d, want %d/%d", globalInFlight, globalLimit, activeSize, activeSize)
	}
	assertNoOutgoingBlockTransferStarted(t, started)

	close(active.release)
	awaitOutgoingBlockTransferResult(t, activeResult)
	focus := awaitOutgoingBlockTransferStart(t, started)
	if focus.folder != "focus" {
		close(focus.release)
		t.Fatalf("first download after reprioritization is %q, want focus", focus.folder)
	}
	close(focus.release)
	awaitOutgoingBlockTransferResult(t, focusResult)
	bulk := awaitOutgoingBlockTransferStart(t, started)
	if bulk.folder != "bulk" {
		close(bulk.release)
		t.Fatalf("second download after reprioritization is %q, want bulk", bulk.folder)
	}
	close(bulk.release)
	awaitOutgoingBlockTransferResult(t, bulkResult)
}

func TestNetworkPriorityRateLimitIndependenceThroughModelRequestGlobal(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{
		"high": 100,
		"low":  0,
	})
	configureDownloadBlockTransferSchedulerForTest(m.model, 2, map[string]int{"high": 100, "low": 0})
	started := make(chan outgoingBlockTransferStart, 4)
	sharedRate := newObservedSharedRateLimiter(64*1024, 8)
	addOutgoingBlockTransferConnectionWithCompletion(m.model, device1, "connection", started, func(ctx context.Context, req *protocol.Request) error {
		return sharedRate.take(ctx, req.Folder, req.Size)
	})

	activeLowResult1 := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	activeLow1 := awaitOutgoingBlockTransferStart(t, started)
	activeLowResult2 := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	activeLow2 := awaitOutgoingBlockTransferStart(t, started)
	enqueued := observeEnqueuedBlockTransfers(m.model)
	highResult := requestOutgoingBlockTransfer(t, m.model, device1, "high", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "high")
	queuedLowResult := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "low")
	assertNoOutgoingBlockTransferStarted(t, started)

	limitBefore := sharedRate.limiter.Limit()
	close(activeLow1.release)
	awaitOutgoingBlockTransferResult(t, activeLowResult1)
	high := awaitOutgoingBlockTransferStart(t, started)
	if high.folder != "high" {
		close(high.release)
		t.Fatalf("first admission after a low-priority transfer completed is %q, want high", high.folder)
	}
	assertNoOutgoingBlockTransferStarted(t, started)

	close(activeLow2.release)
	awaitOutgoingBlockTransferResult(t, activeLowResult2)
	queuedLow := awaitOutgoingBlockTransferStart(t, started)
	if queuedLow.folder != "low" {
		close(high.release)
		close(queuedLow.release)
		t.Fatalf("admission sharing the limiter with active high-priority work is %q, want low", queuedLow.folder)
	}
	close(high.release)
	close(queuedLow.release)
	awaitOutgoingBlockTransferResult(t, highResult)
	awaitOutgoingBlockTransferResult(t, queuedLowResult)

	if got := sharedRate.limiter.Limit(); got != limitBefore {
		t.Fatalf("shared raw rate limit changed from %v to %v", limitBefore, got)
	}
	if got := sharedRate.bytesFor("high"); got != 1 {
		t.Fatalf("high-priority bytes through shared rate limiter = %d, want 1", got)
	}
	if got := sharedRate.bytesFor("low"); got != 3 {
		t.Fatalf("low-priority bytes through shared rate limiter = %d, want 3", got)
	}
}

func TestModelRequestGlobalSelectsAvailableSourceAcrossDevices(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 1)
	addOutgoingBlockTransferConnection(m.model, device2, "connection-device-2", started)

	result := requestOutgoingBlockTransferFromAny(t, m.model, []Availability{
		{ID: device1},
		{ID: device2},
	}, "folder", 1)
	transfer := awaitOutgoingBlockTransferStart(t, started)
	if transfer.device != device2 {
		close(transfer.release)
		t.Fatalf("download source = %s, want available device %s", transfer.device, device2)
	}
	close(transfer.release)
	completed := awaitOutgoingBlockTransferResultValue(t, result)
	if completed.selected.ID != device2 {
		t.Fatalf("reported download source = %s, want %s", completed.selected.ID, device2)
	}
}

func TestModelRequestGlobalKeepsUnavailableSourceDataOutOfRunnableQueue(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{
		"high": 100,
		"low":  -100,
	})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"high": 100, "low": -100})
	started := make(chan outgoingBlockTransferStart, 1)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)

	_, _, err := m.requestGlobalFromAny(t.Context(), nil, &protocol.Request{Folder: "high", Name: "payload", Size: 1})
	if err == nil {
		t.Fatal("download with unavailable source data entered the runnable queue")
	}
	select {
	case descriptor := <-enqueued:
		t.Fatalf("unavailable source data queued folder %q", descriptor.folder)
	default:
	}

	lowResult := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "low")
	low := awaitOutgoingBlockTransferStart(t, started)
	close(low.release)
	awaitOutgoingBlockTransferResult(t, lowResult)
}

func TestModelRequestGlobalStopsWaitingWhenSourceDataBecomesUnavailable(t *testing.T) {
	wrapper, _ := newBlockTransferRequestConfig(t, map[string]int{
		"active": 0,
		"high":   100,
		"low":    -100,
	})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"active": 0, "high": 100, "low": -100})
	started := make(chan outgoingBlockTransferStart, 3)
	lowConnection := addOutgoingBlockTransferConnection(m.model, device2, "connection-device-2", started)
	fc := addFakeConn(m, device1, "high")
	if err := m.ScanFolder("high"); err != nil {
		t.Fatal(err)
	}
	highStarted := make(chan struct{}, 1)
	payload := []byte("source data")
	fc.RequestCalls(func(_ context.Context, _ *protocol.Request) ([]byte, error) {
		highStarted <- struct{}{}
		return payload, nil
	})
	enqueued := observeEnqueuedBlockTransfers(m.model)

	activeResult := requestOutgoingBlockTransfer(t, m.model, device2, "active", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "active")
	active := awaitOutgoingBlockTransferStart(t, started)

	fc.addFile("payload", 0o644, protocol.FileInfoTypeFile, payload)
	fc.sendIndexUpdate()
	select {
	case descriptor := <-enqueued:
		if descriptor.folder != "high" {
			t.Fatalf("enqueued Block Transfer is for folder %q, expected high", descriptor.folder)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for high-priority Block Transfer to enqueue")
	}
	lowResult := requestOutgoingBlockTransfer(t, m.model, device2, "low", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "low")

	fc.deleteFile("payload")
	fc.sendIndexUpdate()
	assertNoOutgoingBlockTransferStarted(t, started)

	close(active.release)
	awaitOutgoingBlockTransferResult(t, activeResult)
	var low outgoingBlockTransferStart
	select {
	case <-highStarted:
		t.Fatal("high-priority Block Transfer started after its source data became unavailable")
	case low = <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for healthy low-priority Block Transfer")
	}
	if low.folder != "low" {
		close(low.release)
		t.Fatalf("download after source loss is for folder %q, want low", low.folder)
	}
	close(low.release)
	awaitOutgoingBlockTransferResult(t, lowResult)
	m.Closed(lowConnection, protocol.ErrClosed)
	pauseFolder(t, wrapper, "high", true)
}

func TestModelRequestGlobalRevalidatesSourceDataWhenEnteringSchedulingWait(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 1)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)

	var calls atomic.Int32
	result := requestOutgoingBlockTransferWithAvailability(t, m.model, func() []Availability {
		if calls.Add(1) == 1 {
			return []Availability{{ID: device1}}
		}
		return nil
	}, "folder", 1)
	select {
	case result := <-result:
		if !errors.Is(result.err, protocol.ErrGeneric) {
			t.Fatalf("source lost before Scheduling Wait returned %v, want protocol error", result.err)
		}
	case transfer := <-started:
		close(transfer.release)
		awaitOutgoingBlockTransferResult(t, result)
		t.Fatal("Block Transfer was admitted without revalidating source data")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for source revalidation")
	}
}

func TestModelUploadAndDownloadBlockTransfersUseIndependentInFlightLimits(t *testing.T) {
	wrapper, controls := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)
	m.uploadScheduler.configure(blockTransferSchedulerConfiguration{
		globalLimit:  1,
		deviceLimits: map[protocol.DeviceID]int{device1: 1},
		folders:      map[string]blockTransferFolder{"folder": {priority: 0, runnable: true}},
	})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})

	payload := []byte{1}
	hash := sha256.Sum256(payload)
	writeFile(t, controls["folder"].filesystem, "payload", payload)
	upload, err := m.Request(device1Conn, &protocol.Request{
		Folder: "folder",
		Name:   "payload",
		Size:   len(payload),
		Hash:   hash[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	defer upload.Close()

	started := make(chan outgoingBlockTransferStart, 1)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)
	downloadResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", len(payload))
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")
	download := awaitOutgoingBlockTransferStart(t, started)
	close(download.release)
	awaitOutgoingBlockTransferResult(t, downloadResult)
}

func TestModelRequestGlobalCancelsQueuedDownloadWithFolderLifecycleContext(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 1)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)

	activeResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")
	active := awaitOutgoingBlockTransferStart(t, started)

	folderCtx, cancelFolder := context.WithCancel(t.Context())
	queuedResult := requestOutgoingBlockTransferContext(folderCtx, m.model, device1, "folder", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")
	cancelFolder()
	select {
	case result := <-queuedResult:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("queued download cancellation returned %v, want context cancellation", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued download cancellation")
	}
	assertNoOutgoingBlockTransferStarted(t, started)

	close(active.release)
	awaitOutgoingBlockTransferResult(t, activeResult)
}

func TestModelRequestGlobalCancelsQueuedDownloadWhenFolderStops(t *testing.T) {
	tests := map[string]func(*testing.T, config.Wrapper){
		"paused": func(t *testing.T, wrapper config.Wrapper) {
			pauseFolder(t, wrapper, "folder", true)
		},
		"removed": func(t *testing.T, wrapper config.Wrapper) {
			waiter, err := wrapper.RemoveFolder("folder")
			if err != nil {
				t.Fatal(err)
			}
			waiter.Wait()
		},
	}
	for name, stopFolder := range tests {
		t.Run(name, func(t *testing.T) {
			wrapper, _ := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
			m := setupModel(t, wrapper)
			defer cleanupModel(m)
			configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
			started := make(chan outgoingBlockTransferStart, 1)
			addOutgoingBlockTransferConnection(m.model, device1, "connection", started)
			enqueued := observeEnqueuedBlockTransfers(m.model)

			activeResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
			awaitEnqueuedBlockTransfer(t, enqueued, "folder")
			active := awaitOutgoingBlockTransferStart(t, started)
			queuedResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
			awaitEnqueuedBlockTransfer(t, enqueued, "folder")

			stopFolder(t, wrapper)
			select {
			case result := <-queuedResult:
				if !errors.Is(result.err, protocol.ErrGeneric) {
					t.Fatalf("queued download returned %v, want protocol error", result.err)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for queued download cancellation")
			}
			assertNoOutgoingBlockTransferStarted(t, started)
			close(active.release)
			awaitOutgoingBlockTransferResult(t, activeResult)
		})
	}
}

func TestModelRequestGlobalReleasesActiveDownloadOnFolderLifecycleCancellation(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 2)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)

	folderCtx, cancelFolder := context.WithCancel(t.Context())
	activeResult := requestOutgoingBlockTransferContext(folderCtx, m.model, device1, "folder", 1)
	awaitOutgoingBlockTransferStart(t, started)
	cancelFolder()
	select {
	case result := <-activeResult:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("active download cancellation returned %v, want context cancellation", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for active download cancellation")
	}

	nextResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	next := awaitOutgoingBlockTransferStart(t, started)
	close(next.release)
	awaitOutgoingBlockTransferResult(t, nextResult)
}

func TestModelRequestGlobalSelectsLeastLoadedCompatibleConnectionDeterministically(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 3, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 3)
	addOutgoingBlockTransferConnection(m.model, device1, "primary", started)
	addOutgoingBlockTransferConnection(m.model, device1, "connection-a", started)

	firstResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	first := awaitOutgoingBlockTransferStart(t, started)
	if first.connection != "connection-a" {
		close(first.release)
		t.Fatalf("first download connection = %q, want connection-a", first.connection)
	}
	addOutgoingBlockTransferConnection(m.model, device1, "connection-b", started)

	secondResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	second := awaitOutgoingBlockTransferStart(t, started)
	if second.connection != "connection-b" {
		close(first.release)
		close(second.release)
		t.Fatalf("least-loaded download connection = %q, want connection-b", second.connection)
	}
	thirdResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	third := awaitOutgoingBlockTransferStart(t, started)
	if third.connection != "connection-a" {
		close(first.release)
		close(second.release)
		close(third.release)
		t.Fatalf("equal-load download connection = %q, want deterministic connection-a", third.connection)
	}

	close(first.release)
	close(second.release)
	close(third.release)
	awaitOutgoingBlockTransferResult(t, firstResult)
	awaitOutgoingBlockTransferResult(t, secondResult)
	awaitOutgoingBlockTransferResult(t, thirdResult)
}

func TestModelRequestGlobalUsesRemainingCompatibleConnectionAfterConnectionCloses(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 2)
	addOutgoingBlockTransferConnection(m.model, device1, "primary", started)
	connectionA := addOutgoingBlockTransferConnection(m.model, device1, "connection-a", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)

	activeResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")
	active := awaitOutgoingBlockTransferStart(t, started)
	if active.connection != "connection-a" {
		close(active.release)
		t.Fatalf("active download connection = %q, want connection-a", active.connection)
	}
	addOutgoingBlockTransferConnection(m.model, device1, "connection-b", started)
	queuedResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", 1)
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")

	m.Closed(connectionA, protocol.ErrClosed)
	close(active.release)
	awaitOutgoingBlockTransferResult(t, activeResult)
	queued := awaitOutgoingBlockTransferStart(t, started)
	if queued.connection != "connection-b" {
		close(queued.release)
		t.Fatalf("download connection after close = %q, want connection-b", queued.connection)
	}
	close(queued.release)
	awaitOutgoingBlockTransferResult(t, queuedResult)
}

func TestDefaultNetworkPriorityLimitsConcurrentDownloads(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{"folder": 0})
	started := make(chan outgoingBlockTransferStart, 2)
	addOutgoingBlockTransferConnection(m.model, device1, "connection", started)

	firstResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", protocol.MaxRequestSize)
	first := awaitOutgoingBlockTransferStart(t, started)
	secondResult := requestOutgoingBlockTransfer(t, m.model, device1, "folder", protocol.MaxRequestSize)
	assertNoOutgoingBlockTransferStarted(t, started)

	close(first.release)
	awaitOutgoingBlockTransferResult(t, firstResult)
	second := awaitOutgoingBlockTransferStart(t, started)
	close(second.release)
	awaitOutgoingBlockTransferResult(t, secondResult)
}

func TestModelRequestGlobalBackoffRetryReturnsRestoredPeerToStrictPriority(t *testing.T) {
	m := newDownloadBlockTransferModel(t, map[string]int{
		"high": 100,
		"low":  -100,
	})
	configureDownloadBlockTransferSchedulerForTest(m.model, 1, map[string]int{"high": 100, "low": -100})
	started := make(chan outgoingBlockTransferStart, 3)
	addOutgoingBlockTransferConnection(m.model, device1, "connection-device-1", started)

	// With no runnable source, high-priority work fails before admission and
	// remains outside the scheduler during the folder's retry backoff.
	_, err := m.RequestGlobal(t.Context(), device2, "high", "payload", 0, 0, 1, nil, false)
	if err == nil {
		t.Fatal("unavailable high-priority peer accepted a download Block Transfer")
	}
	activeLowResult := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	activeLow := awaitOutgoingBlockTransferStart(t, started)

	// Restored availability at the next retry makes the transfer runnable
	// again, so it resumes strict priority over already queued healthy work.
	addOutgoingBlockTransferConnection(m.model, device2, "connection-device-2", started)
	enqueued := observeEnqueuedBlockTransfers(m.model)

	queuedLowResult := requestOutgoingBlockTransfer(t, m.model, device1, "low", 1)
	restoredHighResult := requestOutgoingBlockTransfer(t, m.model, device2, "high", 1)
	queuedFolders := make(map[string]bool)
	for range 2 {
		select {
		case descriptor := <-enqueued:
			queuedFolders[descriptor.folder] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for restored and healthy Block Transfers to enqueue")
		}
	}
	if !queuedFolders["high"] || !queuedFolders["low"] {
		t.Fatalf("queued folders = %v, want high and low", queuedFolders)
	}
	close(activeLow.release)
	awaitOutgoingBlockTransferResult(t, activeLowResult)
	restoredHigh := awaitOutgoingBlockTransferStart(t, started)
	if restoredHigh.folder != "high" {
		close(restoredHigh.release)
		t.Fatalf("first download after availability restoration is %q, want high", restoredHigh.folder)
	}
	close(restoredHigh.release)
	awaitOutgoingBlockTransferResult(t, restoredHighResult)
	queuedLow := awaitOutgoingBlockTransferStart(t, started)
	close(queuedLow.release)
	awaitOutgoingBlockTransferResult(t, queuedLowResult)
}

func TestModelFolderPullRetriesDownloadWhenBackoffExpires(t *testing.T) {
	wrapper, _ := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)

	runner, ok := m.folderRunners.Get("folder")
	if !ok {
		t.Fatal("folder runner not found")
	}
	folder := runner.(*sendReceiveFolder)
	// Expire the real retry timer immediately so the test uses explicit
	// request signals instead of waiting for wall-clock backoff.
	folder.pullPause = 0

	fc := addFakeConn(m, device1, "folder")
	if err := m.ScanFolder("folder"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("retry payload")
	attempts := make(chan int32, 2)
	var attempt atomic.Int32
	fc.RequestCalls(func(_ context.Context, _ *protocol.Request) ([]byte, error) {
		current := attempt.Add(1)
		attempts <- current
		if current == 1 {
			return nil, errors.New("source temporarily unavailable")
		}
		return payload, nil
	})
	fc.addFile("payload", 0o644, protocol.FileInfoTypeFile, payload)
	fc.sendIndexUpdate()

	for want := int32(1); want <= 2; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("download attempt = %d, want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for download attempt %d", want)
		}
	}
}

func TestModelFolderPauseCancelsActiveDownloadBlockTransfer(t *testing.T) {
	wrapper, _ := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)

	fc := addFakeConn(m, device1, "folder")
	if err := m.ScanFolder("folder"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	canceled := make(chan error, 1)
	fc.RequestCalls(func(ctx context.Context, _ *protocol.Request) ([]byte, error) {
		close(started)
		<-ctx.Done()
		canceled <- ctx.Err()
		return nil, ctx.Err()
	})
	payload := []byte("lifecycle payload")
	fc.addFile("payload", 0o644, protocol.FileInfoTypeFile, payload)
	fc.sendIndexUpdate()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for active folder download")
	}
	pauseFolder(t, wrapper, "folder", true)
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active folder download returned %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for folder pause to cancel active download")
	}

	m.downloadScheduler.mut.Lock()
	inFlight := m.downloadScheduler.globalInFlight
	m.downloadScheduler.mut.Unlock()
	if inFlight != 0 {
		t.Fatalf("active folder download retained %d In-Flight bytes after cancellation", inFlight)
	}
}

type outgoingBlockTransferStart struct {
	folder     string
	device     protocol.DeviceID
	connection string
	release    chan struct{}
}

type outgoingBlockTransferResult struct {
	data     []byte
	selected Availability
	err      error
}

func newDownloadBlockTransferModel(t *testing.T, priorities map[string]int) *testModel {
	t.Helper()
	cfg := config.New(myID)
	device := cfg.Defaults.Device.Copy()
	device.DeviceID = device1
	cfg.SetDevice(device)
	device.DeviceID = device2
	cfg.SetDevice(device)
	for folderID, priority := range priorities {
		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.NetworkPriority = priority
		folder.Devices = []config.FolderDeviceConfiguration{{DeviceID: device1}, {DeviceID: device2}}
		cfg.SetFolder(folder)
	}
	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	m := newModel(t, wrapper, myID, nil)
	t.Cleanup(func() { cleanupModel(m) })
	return m
}

func configureDownloadBlockTransferSchedulerForTest(m *model, globalLimit int, priorities map[string]int) {
	folders := make(map[string]blockTransferFolder, len(priorities))
	for folder, priority := range priorities {
		folders[folder] = blockTransferFolder{priority: priority, runnable: true}
	}
	m.downloadScheduler.configure(blockTransferSchedulerConfiguration{
		globalLimit:  globalLimit,
		deviceLimits: make(map[protocol.DeviceID]int),
		folders:      folders,
	})
}

func addOutgoingBlockTransferConnection(m *model, device protocol.DeviceID, connectionID string, started chan<- outgoingBlockTransferStart) *protocolmocks.Connection {
	return addOutgoingBlockTransferConnectionWithCompletion(m, device, connectionID, started, nil)
}

func addOutgoingBlockTransferConnectionWithCompletion(m *model, device protocol.DeviceID, connectionID string, started chan<- outgoingBlockTransferStart, complete func(context.Context, *protocol.Request) error) *protocolmocks.Connection {
	conn := new(protocolmocks.Connection)
	conn.DeviceIDReturns(device)
	conn.ConnectionIDReturns(connectionID)
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		start := outgoingBlockTransferStart{folder: req.Folder, device: device, connection: connectionID, release: make(chan struct{})}
		select {
		case started <- start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-start.release:
			if complete != nil {
				if err := complete(ctx, req); err != nil {
					return nil, err
				}
			}
			return make([]byte, req.Size), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	m.mut.Lock()
	m.connections[connectionID] = conn
	m.deviceConnIDs[device] = append(m.deviceConnIDs[device], connectionID)
	m.closed[connectionID] = make(chan struct{})
	m.mut.Unlock()
	return conn
}

type observedSharedRateLimiter struct {
	limiter *rate.Limiter
	mut     sync.Mutex
	bytes   map[string]int
}

func newObservedSharedRateLimiter(limit rate.Limit, burst int) *observedSharedRateLimiter {
	return &observedSharedRateLimiter{
		limiter: rate.NewLimiter(limit, burst),
		bytes:   make(map[string]int),
	}
}

func (l *observedSharedRateLimiter) take(ctx context.Context, folder string, bytes int) error {
	for remaining := bytes; remaining > 0; {
		chunk := min(remaining, l.limiter.Burst())
		if err := l.limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		remaining -= chunk
	}
	l.mut.Lock()
	l.bytes[folder] += bytes
	l.mut.Unlock()
	return nil
}

func (l *observedSharedRateLimiter) bytesFor(folder string) int {
	l.mut.Lock()
	defer l.mut.Unlock()
	return l.bytes[folder]
}

func requestOutgoingBlockTransfer(t *testing.T, m *model, device protocol.DeviceID, folder string, size int) <-chan outgoingBlockTransferResult {
	t.Helper()
	return requestOutgoingBlockTransferContext(t.Context(), m, device, folder, size)
}

func requestOutgoingBlockTransferContext(ctx context.Context, m *model, device protocol.DeviceID, folder string, size int) <-chan outgoingBlockTransferResult {
	result := make(chan outgoingBlockTransferResult, 1)
	go func() {
		data, err := m.RequestGlobal(ctx, device, folder, "payload", 0, 0, size, nil, false)
		result <- outgoingBlockTransferResult{data: data, err: err}
	}()
	return result
}

func requestOutgoingBlockTransferFromAny(t *testing.T, m *model, candidates []Availability, folder string, size int) <-chan outgoingBlockTransferResult {
	t.Helper()
	result := make(chan outgoingBlockTransferResult, 1)
	go func() {
		data, selected, err := m.requestGlobalFromAny(t.Context(), candidates, &protocol.Request{Folder: folder, Name: "payload", Size: size})
		result <- outgoingBlockTransferResult{data: data, selected: selected, err: err}
	}()
	return result
}

func requestOutgoingBlockTransferWithAvailability(t *testing.T, m *model, availability func() []Availability, folder string, size int) <-chan outgoingBlockTransferResult {
	t.Helper()
	result := make(chan outgoingBlockTransferResult, 1)
	go func() {
		data, selected, err := m.requestGlobalWithAvailability(t.Context(), availability, &protocol.Request{Folder: folder, Name: "payload", Size: size})
		result <- outgoingBlockTransferResult{data: data, selected: selected, err: err}
	}()
	return result
}

func awaitOutgoingBlockTransferStart(t *testing.T, started <-chan outgoingBlockTransferStart) outgoingBlockTransferStart {
	t.Helper()
	select {
	case start := <-started:
		return start
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outgoing Block Transfer to start")
		return outgoingBlockTransferStart{}
	}
}

func assertNoOutgoingBlockTransferStarted(t *testing.T, started <-chan outgoingBlockTransferStart) {
	t.Helper()
	select {
	case start := <-started:
		close(start.release)
		t.Fatalf("outgoing Block Transfer for folder %q started before admission", start.folder)
	default:
	}
}

func awaitOutgoingBlockTransferResult(t *testing.T, result <-chan outgoingBlockTransferResult) []byte {
	t.Helper()
	return awaitOutgoingBlockTransferResultValue(t, result).data
}

func awaitOutgoingBlockTransferResultValue(t *testing.T, result <-chan outgoingBlockTransferResult) outgoingBlockTransferResult {
	t.Helper()
	select {
	case result := <-result:
		if result.err != nil {
			t.Fatalf("outgoing Block Transfer failed: %v", result.err)
		}
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outgoing Block Transfer result")
		return outgoingBlockTransferResult{}
	}
}
