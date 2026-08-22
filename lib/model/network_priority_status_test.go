// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

func TestNetworkPrioritySchedulerStateReportsCurrentWork(t *testing.T) {
	now := time.Unix(1_000, 0)
	upload := newBlockTransferScheduler()
	upload.now = func() time.Time { return now }
	upload.configure(blockTransferSchedulerConfiguration{
		globalLimit: 10,
		folders: map[string]blockTransferFolder{
			"alpha": {priority: 50, runnable: true},
		},
	})
	download := newBlockTransferScheduler()
	download.now = upload.now
	download.configure(blockTransferSchedulerConfiguration{
		folders: map[string]blockTransferFolder{
			"alpha": {priority: 50, runnable: true},
		},
	})

	active, err := upload.enqueue(blockTransferDescriptor{
		folder: "alpha",
		bytes:  10,
		sources: []blockTransferSource{{
			device:      protocol.LocalDeviceID,
			connections: []string{"connection"},
		}},
	}).wait()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(active.close)
	queued := upload.enqueue(blockTransferDescriptor{
		folder: "alpha",
		bytes:  7,
		sources: []blockTransferSource{{
			device:      protocol.LocalDeviceID,
			connections: []string{"connection"},
		}},
	})

	now = now.Add(9 * time.Second)
	m := &model{uploadScheduler: upload, downloadScheduler: download}
	got := m.NetworkPrioritySchedulerState("alpha")
	want := NetworkPrioritySchedulerState{
		Active: true,
		Upload: NetworkPrioritySchedulerDirectionState{
			QueuedBytes:                 7,
			ActiveBytes:                 10,
			OldestSchedulingWaitSeconds: 9,
		},
	}
	if got != want {
		t.Fatalf("Network Priority scheduler state = %#v, want %#v", got, want)
	}

	active.close()
	next, err := queued.wait()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(next.close)
	got = m.NetworkPrioritySchedulerState("alpha")
	if got.Upload.QueuedBytes != 0 || got.Upload.OldestSchedulingWaitSeconds != 0 || got.Upload.ActiveBytes != 7 {
		t.Fatalf("Network Priority scheduler retained historical queue state after admission: %#v", got.Upload)
	}
}
