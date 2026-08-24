// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

func TestSourceHashWorkHashesOneSequentialQuantumPerCall(t *testing.T) {
	const path = "source"
	const finalBlockSize = 17
	blockSize := protocol.MinBlockSize
	data := make([]byte, 2*blockSize+finalBlockSize)
	for i := range data {
		data[i] = byte(i)
	}

	fss := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, fss, path, data)

	work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
		Name:         path,
		Size:         1, // The completed size must come from the retained source.
		RawBlockSize: int32(blockSize),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()

	wantBlocks, err := Blocks(t.Context(), bytes.NewReader(data), blockSize, int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := []int64{int64(blockSize), int64(blockSize), finalBlockSize}

	for i, wantBytes := range wantBytes {
		result, err := work.HashNext(t.Context())
		if err != nil {
			t.Fatalf("quantum %d: %v", i, err)
		}
		if result.Bytes != wantBytes {
			t.Errorf("quantum %d bytes = %d, want %d", i, result.Bytes, wantBytes)
		}

		last := i == len(wantBlocks)-1
		if result.Done != last {
			t.Errorf("quantum %d done = %v, want %v", i, result.Done, last)
		}
		if !last && (result.File.Name != "" || result.File.Blocks != nil || result.File.BlocksHash != nil) {
			t.Errorf("quantum %d published an incomplete file: %v", i, result.File)
		}
		if !last {
			continue
		}

		if !reflect.DeepEqual(result.File.Blocks, wantBlocks) {
			t.Errorf("completed blocks = %v, want %v", result.File.Blocks, wantBlocks)
		}
		wantBlocksHash := protocol.BlocksHash(wantBlocks)
		if !bytes.Equal(result.File.BlocksHash, wantBlocksHash) {
			t.Errorf("completed BlocksHash = %x, want %x", result.File.BlocksHash, wantBlocksHash)
		}
		if result.File.Size != int64(len(data)) {
			t.Errorf("completed size = %d, want %d", result.File.Size, len(data))
		}
	}
}

func TestSourceHashWorkPreservesHashEdgeCases(t *testing.T) {
	blockSize := protocol.MinBlockSize
	tests := []struct {
		name      string
		data      []byte
		wantBytes []int64
	}{
		{
			name:      "empty file",
			wantBytes: []int64{0},
		},
		{
			name:      "exact block multiple",
			data:      make([]byte, 2*blockSize),
			wantBytes: []int64{int64(blockSize), int64(blockSize)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const path = "source"
			fss := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
			writeSourceHashTestFile(t, fss, path, tc.data)

			work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
				Name:         path,
				Size:         int64(len(tc.data)),
				RawBlockSize: int32(blockSize),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer work.Close()

			var gotBytes []int64
			var completed protocol.FileInfo
			for {
				result, err := work.HashNext(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				gotBytes = append(gotBytes, result.Bytes)
				if result.Done {
					completed = result.File
					break
				}
			}

			if !reflect.DeepEqual(gotBytes, tc.wantBytes) {
				t.Errorf("quantum bytes = %v, want %v", gotBytes, tc.wantBytes)
			}
			wantBlocks, err := Blocks(t.Context(), bytes.NewReader(tc.data), blockSize, int64(len(tc.data)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(completed.Blocks, wantBlocks) {
				t.Errorf("completed blocks = %v, want %v", completed.Blocks, wantBlocks)
			}
			if !bytes.Equal(completed.BlocksHash, protocol.BlocksHash(wantBlocks)) {
				t.Errorf("completed BlocksHash = %x, want %x", completed.BlocksHash, protocol.BlocksHash(wantBlocks))
			}
		})
	}
}

func TestSourceHashWorkDiscardsProgressAfterMutation(t *testing.T) {
	const path = "source"
	blockSize := protocol.MinBlockSize
	data := make([]byte, 2*blockSize)
	folderID := "source-hash-mutation-" + rand.String(16)
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, path, data)
	fss := &observedSourceHashFilesystem{Filesystem: underlying}

	work, err := NewSourceHashWork(folderID, fss, protocol.FileInfo{
		Name:         path,
		Size:         int64(len(data)),
		RawBlockSize: int32(blockSize),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()

	first, err := work.HashNext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.Bytes != int64(blockSize) {
		t.Fatalf("first quantum = %+v, want a %d-byte continuation", first, blockSize)
	}
	if err := underlying.Chtimes(path, time.Time{}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	result, err := work.HashNext(t.Context())
	if !errors.Is(err, errFileChangedDuringHashing) {
		t.Fatalf("final quantum error = %v, want %v", err, errFileChangedDuringHashing)
	}
	if result.Bytes != int64(blockSize) {
		t.Errorf("final quantum bytes = %d, want %d", result.Bytes, blockSize)
	}
	assertSourceHashWorkDidNotPublish(t, result)
	if got := fss.closes.Load(); got != 1 {
		t.Errorf("source closes = %d, want 1", got)
	}
	if got := sourceHashMetricValue(t, folderID); got != float64(len(data)) {
		t.Errorf("hashed-bytes metric after final validation failure = %v, want %d", got, len(data))
	}
}

func sourceHashMetricValue(t *testing.T, folderID string) float64 {
	t.Helper()
	metric := new(dto.Metric)
	if err := metricHashedBytes.WithLabelValues(folderID).Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func TestSourceHashWorkReportsBytesAndDiscardsProgressAfterReadError(t *testing.T) {
	const path = "source"
	const bytesBeforeError = 123
	blockSize := protocol.MinBlockSize
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, path, make([]byte, 2*blockSize))
	fss := &observedSourceHashFilesystem{
		Filesystem: underlying,
		wrap: func(file fs.File) fs.File {
			return &partialReadErrorFile{File: file, bytesBeforeError: bytesBeforeError}
		},
	}
	counter := new(atomicSourceHashCounter)

	work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
		Name:         path,
		Size:         2 * int64(blockSize),
		RawBlockSize: int32(blockSize),
	}, counter)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()

	result, err := work.HashNext(t.Context())
	if !errors.Is(err, errSourceHashTestRead) {
		t.Fatalf("quantum error = %v, want %v", err, errSourceHashTestRead)
	}
	if result.Bytes != bytesBeforeError {
		t.Errorf("quantum bytes = %d, want %d", result.Bytes, bytesBeforeError)
	}
	if got := counter.total.Load(); got != bytesBeforeError {
		t.Errorf("counter bytes = %d, want %d", got, bytesBeforeError)
	}
	assertSourceHashWorkDidNotPublish(t, result)
	if got := fss.closes.Load(); got != 1 {
		t.Errorf("source closes = %d, want 1", got)
	}
}

func TestSourceHashWorkFinishesActiveQuantumBeforeCancellationCleanup(t *testing.T) {
	const path = "source"
	blockSize := protocol.MinBlockSize
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, path, make([]byte, 3*blockSize))
	ctx, cancel := context.WithCancel(t.Context())
	fss := &observedSourceHashFilesystem{
		Filesystem: underlying,
		wrap: func(file fs.File) fs.File {
			return &cancelAtOffsetFile{File: file, cancel: cancel, cancelAt: int64(blockSize)}
		},
	}
	counter := new(atomicSourceHashCounter)

	work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
		Name:         path,
		Size:         3 * int64(blockSize),
		RawBlockSize: int32(blockSize),
	}, counter)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()

	first, err := work.HashNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.Bytes != int64(blockSize) {
		t.Fatalf("first quantum = %+v, want a %d-byte continuation", first, blockSize)
	}

	result, err := work.HashNext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("quantum error = %v, want %v", err, context.Canceled)
	}
	if result.Bytes != int64(blockSize) {
		t.Errorf("quantum bytes = %d, want complete non-preemptive block of %d", result.Bytes, blockSize)
	}
	if got := counter.total.Load(); got != 2*int64(blockSize) {
		t.Errorf("counter bytes = %d, want %d", got, 2*blockSize)
	}
	assertSourceHashWorkDidNotPublish(t, result)
	if got := fss.closes.Load(); got != 1 {
		t.Errorf("source closes = %d, want 1", got)
	}
}

func TestSourceHashWorkAllowsOnlyOneActiveQuantumPerFile(t *testing.T) {
	const path = "source"
	blockSize := protocol.MinBlockSize
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, path, make([]byte, 2*blockSize))
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	fss := &observedSourceHashFilesystem{
		Filesystem: underlying,
		wrap: func(file fs.File) fs.File {
			return &quantumBarrierFile{
				File:      file,
				blockSize: int64(blockSize),
				started:   started,
				release:   release,
				once:      make([]sync.Once, len(started)),
			}
		},
	}
	work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
		Name:         path,
		Size:         2 * int64(blockSize),
		RawBlockSize: int32(blockSize),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()

	begin := make(chan struct{})
	results := make(chan sourceHashCallResult, 2)
	for range 2 {
		go func() {
			<-begin
			result, err := work.HashNext(t.Context())
			results <- sourceHashCallResult{result: result, err: err}
		}()
	}
	close(begin)

	awaitScannerSignal(t, started[0], "first Hashing Quantum")
	select {
	case <-started[1]:
		close(release[0])
		close(release[1])
		t.Fatal("second Hashing Quantum became active before the first completed")
	default:
	}
	close(release[0])

	first := <-results
	if first.err != nil {
		t.Fatal(first.err)
	}
	if first.result.Done || first.result.Bytes != int64(blockSize) {
		t.Fatalf("first result = %+v, want a %d-byte continuation", first.result, blockSize)
	}
	awaitScannerSignal(t, started[1], "second Hashing Quantum")
	close(release[1])

	second := <-results
	if second.err != nil {
		t.Fatal(second.err)
	}
	if !second.result.Done || second.result.Bytes != int64(blockSize) {
		t.Fatalf("second result = %+v, want a %d-byte completion", second.result, blockSize)
	}
	if got := fss.opens.Load(); got != 1 {
		t.Errorf("source opens = %d, want 1", got)
	}
	if got := fss.closes.Load(); got != 1 {
		t.Errorf("source closes = %d, want 1", got)
	}
}

func TestSourceHashWorkCloseDiscardsIncompleteProgress(t *testing.T) {
	const path = "source"
	blockSize := protocol.MinBlockSize
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, underlying, path, make([]byte, 2*blockSize))
	fss := &observedSourceHashFilesystem{Filesystem: underlying}
	work, err := NewSourceHashWork("default", fss, protocol.FileInfo{
		Name:         path,
		Size:         2 * int64(blockSize),
		RawBlockSize: int32(blockSize),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := work.HashNext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Done || result.File.Name != "" {
		t.Fatalf("first quantum published an incomplete file: %+v", result)
	}
	work.Close()
	work.Close()
	if got := fss.closes.Load(); got != 1 {
		t.Errorf("source closes after idempotent cleanup = %d, want 1", got)
	}
	if result, err := work.HashNext(t.Context()); !errors.Is(err, errSourceHashWorkDone) {
		t.Fatalf("HashNext after Close = (%+v, %v), want terminal error %v", result, err, errSourceHashWorkDone)
	}
}

func TestHashFileMatchesWholeFileBaseline(t *testing.T) {
	const path = "source"
	const blockSize = 7
	data := []byte("resumable sequential source hashing")
	fss := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	writeSourceHashTestFile(t, fss, path, data)
	counter := new(atomicSourceHashCounter)

	got, err := HashFile(t.Context(), "default", fss, path, blockSize, counter)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Blocks(t.Context(), bytes.NewReader(data), blockSize, int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("HashFile blocks = %v, want %v", got, want)
	}
	if got := counter.total.Load(); got != int64(len(data)) {
		t.Errorf("HashFile counter bytes = %d, want %d", got, len(data))
	}
}

func assertSourceHashWorkDidNotPublish(t *testing.T, result HashingQuantumResult) {
	t.Helper()
	if result.Done || result.File.Name != "" || result.File.Blocks != nil || result.File.BlocksHash != nil {
		t.Errorf("terminal failure published a file: %+v", result)
	}
}

type observedSourceHashFilesystem struct {
	fs.Filesystem
	wrap   func(fs.File) fs.File
	opens  atomic.Int64
	closes atomic.Int64
}

func (f *observedSourceHashFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	f.opens.Add(1)
	if f.wrap != nil {
		file = f.wrap(file)
	}
	return &observedCloseFile{File: file, closes: &f.closes}, nil
}

type observedCloseFile struct {
	fs.File
	closes *atomic.Int64
}

func (f *observedCloseFile) Close() error {
	f.closes.Add(1)
	return f.File.Close()
}

var errSourceHashTestRead = errors.New("source hash test read error")

type partialReadErrorFile struct {
	fs.File
	bytesBeforeError int
	failed           bool
}

func (f *partialReadErrorFile) Read(buf []byte) (int, error) {
	if f.failed {
		return f.File.Read(buf)
	}
	f.failed = true
	buf = buf[:min(len(buf), f.bytesBeforeError)]
	n, _ := f.File.Read(buf)
	return n, errSourceHashTestRead
}

type cancelAtOffsetFile struct {
	fs.File
	cancel   context.CancelFunc
	cancelAt int64
	once     sync.Once
	offset   int64
}

func (f *cancelAtOffsetFile) Read(buf []byte) (int, error) {
	offset := f.offset
	n, err := f.File.Read(buf)
	f.offset += int64(n)
	if offset >= f.cancelAt {
		f.once.Do(f.cancel)
	}
	return n, err
}

type quantumBarrierFile struct {
	fs.File
	blockSize int64
	started   []chan struct{}
	release   []chan struct{}
	once      []sync.Once

	mut    sync.Mutex
	offset int64
}

func (f *quantumBarrierFile) Read(buf []byte) (int, error) {
	f.mut.Lock()
	offset := f.offset
	f.mut.Unlock()
	quantum := int(offset / f.blockSize)
	if offset%f.blockSize == 0 {
		f.once[quantum].Do(func() {
			close(f.started[quantum])
			<-f.release[quantum]
		})
	}

	n, err := f.File.Read(buf)
	f.mut.Lock()
	f.offset += int64(n)
	f.mut.Unlock()
	return n, err
}

type sourceHashCallResult struct {
	result HashingQuantumResult
	err    error
}

type atomicSourceHashCounter struct {
	total atomic.Int64
}

func (c *atomicSourceHashCounter) Update(bytes int64) {
	c.total.Add(bytes)
}

func writeSourceHashTestFile(t *testing.T, fss fs.Filesystem, path string, data []byte) {
	t.Helper()
	fd, err := fss.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fd.Write(data); err != nil {
		_ = fd.Close()
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
}
