// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"sync"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// HashFile hashes the files and returns a list of blocks representing the file.
func HashFile(ctx context.Context, folderID string, fs fs.Filesystem, path string, blockSize int, counter Counter) ([]protocol.BlockInfo, error) {
	file, err := hashFileInfo(ctx, folderID, fs, protocol.FileInfo{Name: path}, blockSize, counter)
	if err != nil {
		l.Debugln("hash file:", err)
		return nil, err
	}
	return file.Blocks, nil
}

func hashFileInfo(ctx context.Context, folderID string, filesystem fs.Filesystem, file protocol.FileInfo, blockSize int, counter Counter) (protocol.FileInfo, error) {
	work, err := newSourceHashWork(folderID, filesystem, file, blockSize, counter)
	if err != nil {
		return protocol.FileInfo{}, err
	}
	defer work.Close()

	for {
		result, err := work.HashNext(ctx)
		if err != nil {
			return protocol.FileInfo{}, err
		}
		if result.Done {
			return result.File, nil
		}
	}
}

// The parallel hasher reads FileInfo structures from the inbox, hashes the
// file to populate the Blocks element and sends it to the outbox. A number of
// workers are used in parallel. The outbox will become closed when the inbox
// is closed and all items handled.
type parallelHasher struct {
	folderID    string
	priority    int
	ceiling     int
	fs          fs.Filesystem
	coordinator *SourceHashCoordinator
	outbox      chan<- ScanResult
	inbox       <-chan protocol.FileInfo
	counter     Counter
	done        chan<- struct{}
	wg          sync.WaitGroup
}

func newParallelHasher(ctx context.Context, folderID string, priority, ceiling int, fs fs.Filesystem, coordinator *SourceHashCoordinator, workers int, outbox chan<- ScanResult, inbox <-chan protocol.FileInfo, counter Counter, done chan<- struct{}) {
	ph := &parallelHasher{
		folderID:    folderID,
		priority:    priority,
		ceiling:     ceiling,
		fs:          fs,
		coordinator: coordinator,
		outbox:      outbox,
		inbox:       inbox,
		counter:     counter,
		done:        done,
	}

	ph.wg.Add(workers)
	for range workers {
		go ph.hashFiles(ctx)
	}

	go ph.closeWhenDone()
}

func (ph *parallelHasher) hashFiles(ctx context.Context) {
	defer ph.wg.Done()

	for {
		select {
		case f, ok := <-ph.inbox:
			if !ok {
				return
			}

			l.Debugln("started hashing:", f)

			if f.IsDirectory() || f.IsDeleted() {
				panic("Bug. Asked to hash a directory or a deleted file.")
			}

			work, err := NewSourceHashWork(ph.folderID, ph.fs, f, ph.counter)
			if err != nil {
				handleError(ctx, "hashing", f.Name, err, ph.outbox)
				continue
			}
			completion := <-ph.coordinator.submit(ctx, sourceHashRequest{
				folder:   ph.folderID,
				priority: ph.priority,
				ceiling:  ph.ceiling,
				work:     work,
			})
			if completion.err != nil {
				handleError(ctx, "hashing", f.Name, completion.err, ph.outbox)
				continue
			}
			f = completion.file

			l.Debugln("completed hashing:", f)
			select {
			case ph.outbox <- ScanResult{File: f}:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (ph *parallelHasher) closeWhenDone() {
	ph.wg.Wait()
	// In case the hasher aborted on context, wait for filesystem
	// walking/progress routine to finish.
	for range ph.inbox {
	}
	if ph.done != nil {
		close(ph.done)
	}
	close(ph.outbox)
}
