// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

const (
	sourceHashThroughputFileCount        = 16
	sourceHashThroughputFileSize         = 16<<20 + 17
	sourceHashThroughputEffectiveWorkers = 4
)

var sourceHashThroughputSink []protocol.FileInfo

type sourceHashThroughputSpec struct {
	filesystem       fs.Filesystem
	files            []protocol.FileInfo
	folder           SourceHashFolder
	effectiveWorkers int
}

type sourceHashThroughputResult struct {
	files               []protocol.FileInfo
	bytes               int64
	effectiveWorkers    int
	peakRetainedHandles int64
}

type sourceHashThroughputRun struct {
	filesystem fs.Filesystem
	handles    *sourceHashHandleObserver
	inbox      <-chan protocol.FileInfo
	results    chan ScanResult
}

func TestSourceHashThroughputPathsUseIdenticalWork(t *testing.T) {
	const (
		folder           = "throughput"
		effectiveWorkers = 2
	)
	blockSize := protocol.MinBlockSize
	filesystem := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	files := make([]protocol.FileInfo, 3)
	for i := range files {
		data := make([]byte, 2*blockSize+17+i)
		for offset := range data {
			data[offset] = byte(offset + i)
		}
		files[i] = protocol.FileInfo{
			Name:         string(rune('a' + i)),
			Size:         int64(len(data)),
			RawBlockSize: int32(blockSize),
		}
		writeSourceHashTestFile(t, filesystem, files[i].Name, data)
	}

	spec := sourceHashThroughputSpec{
		filesystem: filesystem,
		files:      files,
		folder: SourceHashFolder{
			ID:            folder,
			HasherCeiling: effectiveWorkers,
		},
		effectiveWorkers: effectiveWorkers,
	}
	baseline, err := runWholeFileHashThroughput(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := runScheduledHashThroughput(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}

	if baseline.bytes != scheduled.bytes {
		t.Errorf("scheduled bytes = %d, want baseline %d", scheduled.bytes, baseline.bytes)
	}
	if baseline.effectiveWorkers != scheduled.effectiveWorkers {
		t.Errorf("scheduled effective workers = %d, want baseline %d", scheduled.effectiveWorkers, baseline.effectiveWorkers)
	}
	if !reflect.DeepEqual(filesByName(scheduled.files), filesByName(baseline.files)) {
		t.Errorf("scheduled files differ from whole-file baseline\nscheduled: %v\nbaseline: %v", scheduled.files, baseline.files)
	}
	if scheduled.peakRetainedHandles < 1 || scheduled.peakRetainedHandles > int64(effectiveWorkers) {
		t.Errorf("scheduled peak retained handles = %d, want 1..%d", scheduled.peakRetainedHandles, effectiveWorkers)
	}
}

func BenchmarkSourceHashThroughput(b *testing.B) {
	filesystem, files, totalBytes := newSourceHashThroughputDataset(b)
	warmSourceHashThroughputDataset(b, filesystem, files)

	spec := sourceHashThroughputSpec{
		filesystem: filesystem,
		files:      files,
		folder: SourceHashFolder{
			ID:            "throughput",
			HasherCeiling: sourceHashThroughputEffectiveWorkers,
		},
		effectiveWorkers: sourceHashThroughputEffectiveWorkers,
	}
	paths := []struct {
		name string
		run  func(context.Context, sourceHashThroughputSpec) (sourceHashThroughputResult, error)
	}{
		{name: "WholeFileBaseline", run: runWholeFileHashThroughput},
		{name: "Scheduled", run: runScheduledHashThroughput},
	}

	for _, path := range paths {
		b.Run(path.name, func(b *testing.B) {
			var peakRetainedHandles int64
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := path.run(context.Background(), spec)
				if err != nil {
					b.Fatal(err)
				}
				if result.bytes != totalBytes || len(result.files) != len(files) {
					b.Fatalf("hashed (%d bytes, %d files), want (%d bytes, %d files)", result.bytes, len(result.files), totalBytes, len(files))
				}
				peakRetainedHandles = max(peakRetainedHandles, result.peakRetainedHandles)
				sourceHashThroughputSink = result.files
			}
			b.StopTimer()
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
			b.ReportMetric(float64(spec.effectiveWorkers), "hash-capacity")
			b.ReportMetric(float64(spec.folder.HasherCeiling), "folder-ceiling")
			b.ReportMetric(float64(peakRetainedHandles), "peak-retained-handles")
		})
	}
}

func runWholeFileHashThroughput(ctx context.Context, spec sourceHashThroughputSpec) (sourceHashThroughputResult, error) {
	run, err := newSourceHashThroughputRun(spec)
	if err != nil {
		return sourceHashThroughputResult{}, err
	}

	var workers sync.WaitGroup
	workers.Add(spec.effectiveWorkers)
	for range spec.effectiveWorkers {
		go func() {
			defer workers.Done()
			for file := range run.inbox {
				completed, err := hashWholeFileBaseline(ctx, spec.folder.ID, run.filesystem, file)
				run.results <- ScanResult{File: completed, Err: err, Path: file.Name}
			}
		}()
	}
	workers.Wait()
	close(run.results)

	result := sourceHashThroughputResult{
		effectiveWorkers:    spec.effectiveWorkers,
		peakRetainedHandles: run.handles.peak.Load(),
	}
	for completed := range run.results {
		if completed.Err != nil {
			return sourceHashThroughputResult{}, fmt.Errorf("whole-file baseline %s: %w", completed.Path, completed.Err)
		}
		result.files = append(result.files, completed.File)
		result.bytes += completed.File.Size
	}
	if current := run.handles.current.Load(); current != 0 {
		return sourceHashThroughputResult{}, fmt.Errorf("whole-file baseline retained %d source handles", current)
	}
	return result, nil
}

func hashWholeFileBaseline(ctx context.Context, folder string, filesystem fs.Filesystem, file protocol.FileInfo) (protocol.FileInfo, error) {
	fd, err := filesystem.Open(file.Name)
	if err != nil {
		return protocol.FileInfo{}, err
	}
	defer fd.Close()

	initial, err := fd.Stat()
	if err != nil {
		return protocol.FileInfo{}, err
	}
	blocks, err := Blocks(ctx, fd, file.BlockSize(), initial.Size(), nil)
	if err != nil {
		return protocol.FileInfo{}, err
	}
	metricHashedBytes.WithLabelValues(folder).Add(float64(initial.Size()))
	final, err := fd.Stat()
	if err != nil {
		return protocol.FileInfo{}, err
	}
	if initial.Size() != final.Size() || !initial.ModTime().Equal(final.ModTime()) {
		return protocol.FileInfo{}, errFileChangedDuringHashing
	}

	file.Blocks = blocks
	file.BlocksHash = protocol.BlocksHash(blocks)
	file.Size = 0
	for _, block := range blocks {
		file.Size += int64(block.Size)
	}
	return file, nil
}

func runScheduledHashThroughput(ctx context.Context, spec sourceHashThroughputSpec) (sourceHashThroughputResult, error) {
	run, err := newSourceHashThroughputRun(spec)
	if err != nil {
		return sourceHashThroughputResult{}, err
	}
	coordinator := NewSourceHashCoordinator(spec.effectiveWorkers)
	coordinator.Configure(spec.effectiveWorkers, map[string]int{spec.folder.ID: spec.folder.Priority})
	epoch := coordinator.BeginSourceHashEpoch(spec.folder)

	newParallelHasher(ctx, parallelHasherConfig{
		folder:      spec.folder,
		filesystem:  run.filesystem,
		coordinator: coordinator,
		epoch:       epoch,
		outbox:      run.results,
		inbox:       run.inbox,
	}, spec.effectiveWorkers)

	result := sourceHashThroughputResult{
		effectiveWorkers: spec.effectiveWorkers,
	}
	var resultErr error
	for completed := range run.results {
		if completed.Err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("scheduled %s: %w", completed.Path, completed.Err))
			continue
		}
		result.files = append(result.files, completed.File)
		result.bytes += completed.File.Size
	}
	result.peakRetainedHandles = run.handles.peak.Load()
	if current := run.handles.current.Load(); current != 0 {
		resultErr = errors.Join(resultErr, fmt.Errorf("scheduled path retained %d source handles", current))
	}
	return result, resultErr
}

func newSourceHashThroughputRun(spec sourceHashThroughputSpec) (sourceHashThroughputRun, error) {
	if err := validateSourceHashThroughputSpec(spec); err != nil {
		return sourceHashThroughputRun{}, err
	}
	handles := new(sourceHashHandleObserver)
	inbox := make(chan protocol.FileInfo, len(spec.files))
	for _, file := range spec.files {
		inbox <- file
	}
	close(inbox)
	return sourceHashThroughputRun{
		filesystem: &observedSourceHashFilesystem{Filesystem: spec.filesystem, handles: handles},
		handles:    handles,
		inbox:      inbox,
		results:    make(chan ScanResult, len(spec.files)),
	}, nil
}

func validateSourceHashThroughputSpec(spec sourceHashThroughputSpec) error {
	if spec.filesystem == nil {
		return errors.New("missing throughput benchmark filesystem")
	}
	if spec.effectiveWorkers < 1 {
		return errors.New("effective worker count must be positive")
	}
	if spec.folder.ID == "" {
		return errors.New("missing throughput benchmark Folder")
	}
	if spec.folder.HasherCeiling != spec.effectiveWorkers {
		return fmt.Errorf("per-Folder ceiling %d must equal effective worker count %d", spec.folder.HasherCeiling, spec.effectiveWorkers)
	}
	return nil
}

func filesByName(files []protocol.FileInfo) map[string]protocol.FileInfo {
	byName := make(map[string]protocol.FileInfo, len(files))
	for _, file := range files {
		byName[file.Name] = file
	}
	return byName
}

func newSourceHashThroughputDataset(tb testing.TB) (fs.Filesystem, []protocol.FileInfo, int64) {
	tb.Helper()
	filesystem := fs.NewFilesystem(fs.FilesystemTypeBasic, tb.TempDir())
	blockSize := protocol.MinBlockSize
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte((i*31 + 17) % 251)
	}

	files := make([]protocol.FileInfo, sourceHashThroughputFileCount)
	for i := range files {
		name := fmt.Sprintf("source-%02d.dat", i)
		fd, err := filesystem.Create(name)
		if err != nil {
			tb.Fatal(err)
		}
		remaining := sourceHashThroughputFileSize
		for remaining > 0 {
			writeSize := min(remaining, len(chunk))
			n, err := fd.Write(chunk[:writeSize])
			if err != nil {
				_ = fd.Close()
				tb.Fatal(err)
			}
			if n != writeSize {
				_ = fd.Close()
				tb.Fatal(io.ErrShortWrite)
			}
			remaining -= n
		}
		if err := fd.Close(); err != nil {
			tb.Fatal(err)
		}
		files[i] = protocol.FileInfo{
			Name:         name,
			Size:         sourceHashThroughputFileSize,
			RawBlockSize: int32(blockSize),
		}
	}
	return filesystem, files, int64(sourceHashThroughputFileCount * sourceHashThroughputFileSize)
}

func warmSourceHashThroughputDataset(tb testing.TB, filesystem fs.Filesystem, files []protocol.FileInfo) {
	tb.Helper()
	for _, file := range files {
		fd, err := filesystem.Open(file.Name)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, fd); err != nil {
			_ = fd.Close()
			tb.Fatal(err)
		}
		if err := fd.Close(); err != nil {
			tb.Fatal(err)
		}
	}
}
