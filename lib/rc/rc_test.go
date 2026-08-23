// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package rc

import "testing"

func TestDecodeConnectionStats(t *testing.T) {
	const device = "AAAAAAA-BBBBBBB-CCCCCCC-DDDDDDD-EEEEEEE-FFFFFFF-GGGGGGG-HHHHHHH"
	for _, response := range []string{
		`{"connections":{"` + device + `":{"connected":true,"inBytesTotal":123,"outBytesTotal":456}}}`,
		`{"` + device + `":{"connected":true,"inBytesTotal":123,"outBytesTotal":456}}`,
	} {
		connections, err := decodeConnectionStats([]byte(response))
		if err != nil {
			t.Fatal(err)
		}
		connection, ok := connections[device]
		if !ok {
			t.Fatalf("device connection missing from %#v", connections)
		}
		if !connection.Connected || connection.InBytesTotal != 123 || connection.OutBytesTotal != 456 {
			t.Fatalf("unexpected connection stats: %#v", connection)
		}
	}
}
