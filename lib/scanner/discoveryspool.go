// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/syncthing/syncthing/lib/protocol"
)

const discoverySpoolPattern = "syncthing-discovery-*"

var errDiscoverySpoolHashedContent = errors.New("discovery metadata contains hashed content")

// discoverySpool owns the temporary, disk-backed inventory for one buffered
// scan. It contains metadata only; source handles and block results remain
// owned by Source Hash Work after records leave the spool.
type discoverySpool struct {
	file    *os.File
	encoder *gob.Encoder
	decoder *gob.Decoder
	length  int

	closeOnce sync.Once
	closeErr  error
}

func newDiscoverySpool() (*discoverySpool, error) {
	file, err := os.CreateTemp("", discoverySpoolPattern)
	if err != nil {
		return nil, err
	}
	return &discoverySpool{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (s *discoverySpool) Append(file protocol.FileInfo) error {
	if len(file.Blocks) != 0 || len(file.BlocksHash) != 0 {
		return fmt.Errorf("%w for %q", errDiscoverySpoolHashedContent, file.Name)
	}
	if err := s.encoder.Encode(file); err != nil {
		return err
	}
	s.length++
	return nil
}

func (s *discoverySpool) Rewind() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s.decoder = gob.NewDecoder(s.file)
	return nil
}

func (s *discoverySpool) Next() (protocol.FileInfo, error) {
	if s.decoder == nil {
		return protocol.FileInfo{}, errors.New("discovery spool is not ready for reading")
	}
	var file protocol.FileInfo
	if err := s.decoder.Decode(&file); err != nil {
		return protocol.FileInfo{}, err
	}
	if len(file.Blocks) != 0 || len(file.BlocksHash) != 0 {
		return protocol.FileInfo{}, fmt.Errorf("%w for %q", errDiscoverySpoolHashedContent, file.Name)
	}
	return file, nil
}

func (s *discoverySpool) Len() int {
	return s.length
}

func (s *discoverySpool) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.file.Close(), os.Remove(s.file.Name()))
	})
	return s.closeErr
}
