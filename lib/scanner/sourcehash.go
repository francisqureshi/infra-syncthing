// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"errors"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

var (
	errFileChangedDuringHashing = errors.New("file changed during hashing")
	errSourceHashWorkDone       = errors.New("source hash work is already done")
)

// HashingQuantumResult reports the outcome of one Hashing Quantum. File is
// populated only when Done is true and the complete source file has passed
// final validation.
type HashingQuantumResult struct {
	// Bytes is the number of source bytes consumed by this invocation,
	// including bytes consumed before a terminal error or cancellation.
	Bytes int64
	// Done distinguishes terminal success from a continuation.
	Done bool
	// File is populated atomically on terminal success and is otherwise zero.
	File protocol.FileInfo
}

// SourceHashWork owns the resumable, sequential hashing state for one source
// file. Calls are serialized so at most one Hashing Quantum can be active.
// Partial block lists remain private until final validation succeeds.
type SourceHashWork struct {
	mut sync.Mutex

	folderID string
	file     protocol.FileInfo
	fd       fs.File
	counter  Counter

	blockSize   int
	initialSize int64
	initialTime time.Time
	nextOffset  int64
	blocks      []protocol.BlockInfo
	hashes      []byte
	done        bool
}

// NewSourceHashWork opens file and captures the source state which must remain
// valid until hashing completes. Terminal outcomes close the source handle;
// callers must Close work which they abandon between quanta.
func NewSourceHashWork(folderID string, filesystem fs.Filesystem, file protocol.FileInfo, counter Counter) (*SourceHashWork, error) {
	return newSourceHashWork(folderID, filesystem, file, file.BlockSize(), counter)
}

func newSourceHashWork(folderID string, filesystem fs.Filesystem, file protocol.FileInfo, blockSize int, counter Counter) (*SourceHashWork, error) {
	fd, err := filesystem.Open(file.Name)
	if err != nil {
		return nil, err
	}

	info, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	if counter == nil {
		counter = &noopCounter{}
	}

	numBlocks := info.Size() / int64(blockSize)
	if info.Size()%int64(blockSize) != 0 {
		numBlocks++
	}

	return &SourceHashWork{
		folderID:    folderID,
		file:        file,
		fd:          fd,
		counter:     counter,
		blockSize:   blockSize,
		initialSize: info.Size(),
		initialTime: info.ModTime(),
		blocks:      make([]protocol.BlockInfo, 0, numBlocks),
		hashes:      make([]byte, 0, hashLength*numBlocks),
	}, nil
}

// HashNext executes at most one Hashing Quantum. An active quantum is
// non-preemptive, so cancellation is observed before or after its block read.
// A successful non-terminal result is a continuation; terminal success
// publishes the complete File. Every error is terminal and discards the work.
func (w *SourceHashWork) HashNext(ctx context.Context) (HashingQuantumResult, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	if w.done {
		return HashingQuantumResult{}, errSourceHashWorkDone
	}
	if err := ctx.Err(); err != nil {
		w.discard()
		return HashingQuantumResult{}, err
	}

	if w.initialSize == 0 {
		w.blocks = append(w.blocks, protocol.BlockInfo{
			Offset: 0,
			Size:   0,
			Hash:   SHA256OfNothing,
		})
		return w.complete()
	}

	quantumSize := min(int64(w.blockSize), w.initialSize-w.nextOffset)
	block, remainingHashes, bytesHashed, err := hashOneBlock(w.fd, quantumSize, w.nextOffset, w.hashes)
	w.counter.Update(bytesHashed)
	result := HashingQuantumResult{Bytes: bytesHashed}
	if err != nil {
		w.discard()
		return result, err
	}
	if err := ctx.Err(); err != nil {
		w.discard()
		return result, err
	}
	if bytesHashed != quantumSize {
		err := w.validate()
		w.discard()
		if err != nil {
			return result, err
		}
		return result, io.ErrUnexpectedEOF
	}

	w.blocks = append(w.blocks, block)
	w.hashes = remainingHashes
	w.nextOffset += bytesHashed
	if w.nextOffset < w.initialSize {
		return result, nil
	}

	completed, err := w.complete()
	completed.Bytes = bytesHashed
	return completed, err
}

func hashOneBlock(r io.Reader, size, offset int64, hashes []byte) (protocol.BlockInfo, []byte, int64, error) {
	hf := hashPool.Get().(hash.Hash)         //nolint:forcetypeassert
	buf := bufPool.Get().(*[bufSize]byte)[:] //nolint:forcetypeassert
	defer func() {
		bufPool.Put((*[bufSize]byte)(buf))
		hashPool.Put(hf)
	}()
	return hashBlock(hf, buf, r, size, offset, hashes)
}

func (w *SourceHashWork) complete() (HashingQuantumResult, error) {
	if err := w.validate(); err != nil {
		w.discard()
		return HashingQuantumResult{}, err
	}

	w.close()
	w.done = true
	w.file.Blocks = w.blocks
	w.file.BlocksHash = protocol.BlocksHash(w.blocks)
	w.file.Size = w.nextOffset
	metricHashedBytes.WithLabelValues(w.folderID).Add(float64(w.initialSize))
	return HashingQuantumResult{
		Done: true,
		File: w.file,
	}, nil
}

func (w *SourceHashWork) validate() error {
	info, err := w.fd.Stat()
	if err != nil {
		return err
	}
	if w.initialSize != info.Size() || !w.initialTime.Equal(info.ModTime()) {
		return errFileChangedDuringHashing
	}
	return nil
}

func (w *SourceHashWork) discard() {
	w.blocks = nil
	w.hashes = nil
	w.done = true
	w.close()
}

func (w *SourceHashWork) close() {
	if w.fd != nil {
		_ = w.fd.Close()
		w.fd = nil
	}
}

// Close discards incomplete progress and closes the retained source handle.
// It waits for an active Hashing Quantum to reach its block boundary.
func (w *SourceHashWork) Close() {
	w.mut.Lock()
	defer w.mut.Unlock()
	if !w.done {
		w.discard()
	}
}
