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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	protocolmocks "github.com/syncthing/syncthing/lib/protocol/mocks"
	"github.com/syncthing/syncthing/lib/rand"
	"github.com/syncthing/syncthing/lib/testutil"
)

const observingBlockTransferFilesystemType fs.FilesystemType = "observing-block-transfer"

var observingBlockTransferFilesystems sync.Map

func init() {
	fs.RegisterFilesystemType(observingBlockTransferFilesystemType, func(root string, _ ...fs.Option) (fs.Filesystem, error) {
		control, ok := observingBlockTransferFilesystems.Load(root)
		if !ok {
			return nil, errors.New("missing observing Block Transfer filesystem")
		}
		return &observingBlockTransferFilesystem{
			Filesystem: control.(*blockTransferFilesystemControl).filesystem,
			control:    control.(*blockTransferFilesystemControl),
		}, nil
	})
}

type blockTransferFilesystemControl struct {
	filesystem fs.Filesystem
	opened     chan struct{}
	armed      atomic.Bool
}

type observingBlockTransferFilesystem struct {
	fs.Filesystem
	control *blockTransferFilesystemControl
}

func (f *observingBlockTransferFilesystem) Open(name string) (fs.File, error) {
	if f.control.armed.Load() {
		select {
		case f.control.opened <- struct{}{}:
		default:
		}
	}
	return f.Filesystem.Open(name)
}

func TestModelRequestSchedulesBeforeFileOpenAndReleasesOnCompletion(t *testing.T) {
	wrapper, controls := newBlockTransferRequestConfig(t, map[string]int{
		"low":  -100,
		"high": 100,
	})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)

	payload := make([]byte, 1024)
	hash := sha256.Sum256(payload)
	for _, control := range controls {
		writeFile(t, control.filesystem, "payload", payload)
	}

	active, err := m.Request(device1Conn, &protocol.Request{Folder: "low", Name: "payload", Size: len(payload), Hash: hash[:]})
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range controls {
		control.armed.Store(true)
	}
	enqueued := observeEnqueuedBlockTransfers(m.model)

	lowResult := make(chan blockTransferRequestResult, 1)
	go requestBlockTransfer(m.model, "low", payload, hash, lowResult)
	awaitEnqueuedBlockTransfer(t, enqueued, "low")
	highResult := make(chan blockTransferRequestResult, 1)
	go requestBlockTransfer(m.model, "high", payload, hash, highResult)
	awaitEnqueuedBlockTransfer(t, enqueued, "high")

	assertFileNotOpened(t, controls["low"])
	assertFileNotOpened(t, controls["high"])
	active.Close()

	awaitFileOpen(t, controls["high"])
	assertFileNotOpened(t, controls["low"])
	high := awaitBlockTransferRequest(t, highResult)
	assertBlockTransferRequestWaiting(t, lowResult)
	high.Close()

	awaitFileOpen(t, controls["low"])
	awaitBlockTransferRequest(t, lowResult).Close()
}

func TestModelRequestCancelsQueuedTransferWhenFolderStops(t *testing.T) {
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
			wrapper, controls := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
			m := setupModel(t, wrapper)
			defer cleanupModel(m)

			payload := make([]byte, 1024)
			hash := sha256.Sum256(payload)
			writeFile(t, controls["folder"].filesystem, "payload", payload)
			active, err := m.Request(device1Conn, &protocol.Request{Folder: "folder", Name: "payload", Size: len(payload), Hash: hash[:]})
			if err != nil {
				t.Fatal(err)
			}
			controls["folder"].armed.Store(true)
			enqueued := observeEnqueuedBlockTransfers(m.model)

			queuedResult := make(chan blockTransferRequestResult, 1)
			go requestBlockTransfer(m.model, "folder", payload, hash, queuedResult)
			awaitEnqueuedBlockTransfer(t, enqueued, "folder")
			stopFolder(t, wrapper)

			result := <-queuedResult
			if !errors.Is(result.err, protocol.ErrGeneric) {
				t.Fatalf("queued request returned %v, expected protocol error", result.err)
			}
			if result.response != nil {
				result.response.Close()
				t.Fatal("queued request returned a response after its folder stopped")
			}
			assertFileNotOpened(t, controls["folder"])
			if len(active.Data()) != len(payload) {
				t.Fatal("stopping a folder interrupted an active response")
			}
			active.Close()

			response, err := m.Request(device1Conn, &protocol.Request{Folder: "folder", Name: "payload", Size: len(payload), Hash: hash[:]})
			if response != nil {
				response.Close()
				t.Fatal("stopped folder returned a response for a new request")
			}
			if !errors.Is(err, protocol.ErrGeneric) {
				t.Fatalf("new request for stopped folder returned %v, expected protocol error", err)
			}
		})
	}
}

func TestModelRequestErrorHoldsAdmissionUntilResponseCompletion(t *testing.T) {
	wrapper, controls := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)

	payload := make([]byte, 1024)
	hash := sha256.Sum256(payload)
	control := controls["folder"]
	writeFile(t, control.filesystem, "payload", payload)
	control.armed.Store(true)

	response, err := m.Request(device1Conn, &protocol.Request{Folder: "folder", Name: "missing", Size: len(payload), Hash: hash[:]})
	if response != nil {
		response.Close()
		t.Fatal("missing file returned a response")
	}
	if !errors.Is(err, protocol.ErrNoSuchFile) {
		t.Fatalf("missing file returned %v, expected no-such-file error", err)
	}
	ordered, ok := err.(interface {
		WaitForResponse()
		Close()
	})
	if !ok {
		t.Fatal("post-admission error did not preserve its response lifecycle")
	}

	enqueued := observeEnqueuedBlockTransfers(m.model)
	validResult := make(chan blockTransferRequestResult, 1)
	go requestBlockTransfer(m.model, "folder", payload, hash, validResult)
	awaitEnqueuedBlockTransfer(t, enqueued, "folder")
	assertFileNotOpened(t, control)
	ordered.WaitForResponse()
	ordered.Close()

	awaitFileOpen(t, control)
	awaitBlockTransferRequest(t, validResult).Close()
}

func TestModelRequestFrameFinishesOnWireAfterFolderRemoval(t *testing.T) {
	wrapper, controls := newBlockTransferRequestConfig(t, map[string]int{"folder": 0})
	m := setupModel(t, wrapper)
	defer cleanupModel(m)

	payload := make([]byte, 1024)
	hash := sha256.Sum256(payload)
	writeFile(t, controls["folder"].filesystem, "payload", payload)

	localReader, remoteWriter := io.Pipe()
	remoteReader, localWriter := io.Pipe()
	uploadWriter := &controlledUploadWriter{
		Writer:  localWriter,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	localModel := &blockTransferWireModel{request: m.Request, clusterConfigured: make(chan struct{})}
	remoteModel := &blockTransferWireModel{clusterConfigured: make(chan struct{})}
	localConnection := protocol.NewConnection(device1, localReader, uploadWriter, testutil.NoopCloser{}, localModel, new(protocolmocks.ConnectionInfo), protocol.CompressionNever, nil)
	remoteConnection := protocol.NewConnection(myID, remoteReader, remoteWriter, testutil.NoopCloser{}, remoteModel, new(protocolmocks.ConnectionInfo), protocol.CompressionNever, nil)
	localConnection.Start()
	remoteConnection.Start()
	localConnection.ClusterConfig(&protocol.ClusterConfig{}, nil)
	remoteConnection.ClusterConfig(&protocol.ClusterConfig{}, nil)
	awaitClusterConfiguration(t, localModel)
	awaitClusterConfiguration(t, remoteModel)
	t.Cleanup(func() {
		_ = localReader.Close()
		_ = localWriter.Close()
		_ = remoteReader.Close()
		_ = remoteWriter.Close()
		<-localConnection.Closed()
		<-remoteConnection.Closed()
	})

	uploadWriter.armed.Store(true)
	activeResult := make(chan blockTransferWireResult, 1)
	go requestBlockTransferOnWire(t.Context(), remoteConnection, payload, hash, activeResult)
	select {
	case <-uploadWriter.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for active response frame to begin writing")
	}
	m.uploadScheduler.mut.Lock()
	inFlight := m.uploadScheduler.deviceInFlight[device1]
	m.uploadScheduler.mut.Unlock()
	if inFlight != len(payload) {
		t.Fatalf("wire-blocked response accounts for %d In-Flight bytes, expected %d", inFlight, len(payload))
	}

	waiter, err := wrapper.RemoveFolder("folder")
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
	rejectedResult := make(chan blockTransferWireResult, 1)
	go requestBlockTransferOnWire(t.Context(), remoteConnection, payload, hash, rejectedResult)
	select {
	case result := <-activeResult:
		t.Fatalf("active response completed while its wire frame was blocked: %v", result.err)
	default:
	}

	close(uploadWriter.release)
	active := <-activeResult
	if active.err != nil {
		t.Fatalf("active response failed after folder removal: %v", active.err)
	}
	if len(active.data) != len(payload) {
		t.Fatalf("active response returned %d bytes, expected %d", len(active.data), len(payload))
	}
	rejected := <-rejectedResult
	if !errors.Is(rejected.err, protocol.ErrGeneric) {
		t.Fatalf("new request after folder removal returned %v, expected protocol error", rejected.err)
	}
}

type blockTransferRequestResult struct {
	response protocol.RequestResponse
	err      error
}

type blockTransferWireResult struct {
	data []byte
	err  error
}

type blockTransferWireModel struct {
	request           func(protocol.Connection, *protocol.Request) (protocol.RequestResponse, error)
	clusterConfigured chan struct{}
	clusterOnce       sync.Once
}

func (*blockTransferWireModel) Index(protocol.Connection, *protocol.Index) error {
	return nil
}

func (*blockTransferWireModel) IndexUpdate(protocol.Connection, *protocol.IndexUpdate) error {
	return nil
}

func (m *blockTransferWireModel) Request(connection protocol.Connection, request *protocol.Request) (protocol.RequestResponse, error) {
	if m.request == nil {
		return nil, protocol.ErrGeneric
	}
	return m.request(connection, request)
}

func (m *blockTransferWireModel) ClusterConfig(protocol.Connection, *protocol.ClusterConfig) error {
	m.clusterOnce.Do(func() { close(m.clusterConfigured) })
	return nil
}

func (*blockTransferWireModel) Closed(protocol.Connection, error) {}

func (*blockTransferWireModel) DownloadProgress(protocol.Connection, *protocol.DownloadProgress) error {
	return nil
}

type controlledUploadWriter struct {
	io.Writer
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (w *controlledUploadWriter) Write(data []byte) (int, error) {
	if w.armed.CompareAndSwap(true, false) {
		close(w.started)
		<-w.release
	}
	return w.Writer.Write(data)
}

func requestBlockTransfer(m *model, folder string, payload []byte, hash [sha256.Size]byte, result chan<- blockTransferRequestResult) {
	response, err := m.Request(device1Conn, &protocol.Request{Folder: folder, Name: "payload", Size: len(payload), Hash: hash[:]})
	result <- blockTransferRequestResult{response: response, err: err}
}

func requestBlockTransferOnWire(ctx context.Context, connection protocol.Connection, payload []byte, hash [sha256.Size]byte, result chan<- blockTransferWireResult) {
	data, err := connection.Request(ctx, &protocol.Request{Folder: "folder", Name: "payload", Size: len(payload), Hash: hash[:]})
	result <- blockTransferWireResult{data: data, err: err}
}

func awaitClusterConfiguration(t *testing.T, model *blockTransferWireModel) {
	t.Helper()
	select {
	case <-model.clusterConfigured:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Cluster Configuration")
	}
}

func newBlockTransferRequestConfig(t *testing.T, priorities map[string]int) (config.Wrapper, map[string]*blockTransferFilesystemControl) {
	t.Helper()
	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Options.FeatureFlags = []string{config.FeatureFlagNetworkPriority}
	device := cfg.Defaults.Device.Copy()
	device.DeviceID = device1
	device.MaxRequestKiB = 1
	cfg.SetDevice(device)

	controls := make(map[string]*blockTransferFilesystemControl, len(priorities))
	for folderID, priority := range priorities {
		root := rand.String(32)
		control := &blockTransferFilesystemControl{
			filesystem: fs.NewFilesystem(fs.FilesystemTypeFake, root+"?content=true"),
			opened:     make(chan struct{}, 1),
		}
		observingBlockTransferFilesystems.Store(root, control)
		t.Cleanup(func() { observingBlockTransferFilesystems.Delete(root) })
		controls[folderID] = control

		folder := cfg.Defaults.Folder.Copy()
		folder.ID = folderID
		folder.Label = folderID
		folder.Path = root
		folder.FilesystemType = config.FilesystemType(observingBlockTransferFilesystemType)
		folder.Devices = []config.FolderDeviceConfiguration{{DeviceID: device1}}
		folder.NetworkPriority = priority
		folder.FSWatcherEnabled = false
		folder.RescanIntervalS = 0
		cfg.SetFolder(folder)
	}

	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)
	return wrapper, controls
}

func observeEnqueuedBlockTransfers(m *model) <-chan blockTransferDescriptor {
	enqueued := make(chan blockTransferDescriptor, 2)
	m.blockTransferEnqueued = func(descriptor blockTransferDescriptor) {
		enqueued <- descriptor
	}
	return enqueued
}

func awaitEnqueuedBlockTransfer(t *testing.T, enqueued <-chan blockTransferDescriptor, folder string) {
	t.Helper()
	select {
	case descriptor := <-enqueued:
		if descriptor.folder != folder {
			t.Fatalf("enqueued Block Transfer is for folder %q, expected %q", descriptor.folder, folder)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for folder %q Block Transfer to enqueue", folder)
	}
}

func awaitBlockTransferRequest(t *testing.T, results <-chan blockTransferRequestResult) protocol.RequestResponse {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("Block Transfer request failed: %v", result.err)
		}
		if result.response == nil {
			t.Fatal("Block Transfer request returned no response")
		}
		return result.response
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Block Transfer request")
		return nil
	}
}

func assertBlockTransferRequestWaiting(t *testing.T, results <-chan blockTransferRequestResult) {
	t.Helper()
	select {
	case result := <-results:
		if result.response != nil {
			result.response.Close()
		}
		t.Fatalf("Block Transfer request completed before capacity was released: %v", result.err)
	default:
	}
}

func awaitFileOpen(t *testing.T, control *blockTransferFilesystemControl) {
	t.Helper()
	select {
	case <-control.opened:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admitted Block Transfer to open its file")
	}
}

func assertFileNotOpened(t *testing.T, control *blockTransferFilesystemControl) {
	t.Helper()
	select {
	case <-control.opened:
		t.Fatal("Block Transfer opened its file before admission")
	default:
	}
}
