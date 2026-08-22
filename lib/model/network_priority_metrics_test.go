// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

type staticNetworkPrioritySchedulerStateProvider struct {
	state NetworkPrioritySchedulerState
}

func (p staticNetworkPrioritySchedulerStateProvider) NetworkPrioritySchedulerState(string) NetworkPrioritySchedulerState {
	return p.state
}

func TestNetworkPriorityMetricsExposeBoundedCurrentState(t *testing.T) {
	wrapper := config.Wrap(filepath.Join(t.TempDir(), "config.xml"), config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "alpha"}},
	}, protocol.LocalDeviceID, events.NoopLogger)
	provider := staticNetworkPrioritySchedulerStateProvider{state: NetworkPrioritySchedulerState{
		Active: true,
		Upload: NetworkPrioritySchedulerDirectionState{
			QueuedBytes:                 11,
			ActiveBytes:                 12,
			OldestSchedulingWaitSeconds: 13,
		},
		Download: NetworkPrioritySchedulerDirectionState{
			QueuedBytes:                 21,
			ActiveBytes:                 22,
			OldestSchedulingWaitSeconds: 23,
		},
	}}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newNetworkPriorityMetricsCollector(wrapper, &provider))

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]map[string]float64)
	for _, family := range families {
		values := make(map[string]float64)
		for _, metric := range family.Metric {
			if len(metric.Label) != 2 {
				t.Fatalf("metric %q has %d labels, expected only folder and direction", family.GetName(), len(metric.Label))
			}
			labels := make(map[string]string)
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			values[labels["direction"]+"/"+labels["folder"]] = metric.GetGauge().GetValue()
		}
		got[family.GetName()] = values
	}
	want := map[string]map[string]float64{
		"syncthing_model_network_priority_active_bytes": {
			"download/alpha": 22,
			"upload/alpha":   12,
		},
		"syncthing_model_network_priority_oldest_scheduling_wait_seconds": {
			"download/alpha": 23,
			"upload/alpha":   13,
		},
		"syncthing_model_network_priority_queued_bytes": {
			"download/alpha": 21,
			"upload/alpha":   11,
		},
		"syncthing_model_network_priority_scheduler_active": {
			"download/alpha": 1,
			"upload/alpha":   1,
		},
	}
	if len(got) != len(want) {
		t.Fatalf("gathered %d metric families, want %d: %#v", len(got), len(want), got)
	}
	for name, wantValues := range want {
		gotValues := got[name]
		if len(gotValues) != len(wantValues) {
			t.Fatalf("metric %q values = %#v, want %#v", name, gotValues, wantValues)
		}
		for labels, wantValue := range wantValues {
			if gotValue := gotValues[labels]; gotValue != wantValue {
				t.Fatalf("metric %q{%s} = %v, want %v", name, labels, gotValue, wantValue)
			}
		}
	}

	provider.state.Upload.OldestSchedulingWaitSeconds = 99
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "syncthing_model_network_priority_oldest_scheduling_wait_seconds" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "direction" && label.GetValue() == "upload" && metric.GetGauge().GetValue() != 99 {
					t.Fatalf("second scrape reported stale Scheduling Wait %v, want 99", metric.GetGauge().GetValue())
				}
			}
		}
	}
}
