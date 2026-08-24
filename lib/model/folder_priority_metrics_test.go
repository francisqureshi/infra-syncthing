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

type staticFolderPrioritySchedulerStateProvider struct {
	state FolderPrioritySchedulerState
}

func (p staticFolderPrioritySchedulerStateProvider) FolderPrioritySchedulerState(string) FolderPrioritySchedulerState {
	return p.state
}

func TestFolderPriorityMetricsExposeBoundedCurrentState(t *testing.T) {
	wrapper := config.Wrap(filepath.Join(t.TempDir(), "config.xml"), config.Configuration{
		Folders: []config.FolderConfiguration{{ID: "alpha"}},
	}, protocol.LocalDeviceID, events.NoopLogger)
	provider := staticFolderPrioritySchedulerStateProvider{state: FolderPrioritySchedulerState{
		Active: true,
		Upload: FolderPrioritySchedulerDirectionState{
			QueuedBytes:                 11,
			ActiveBytes:                 12,
			OldestSchedulingWaitSeconds: 13,
		},
		Download: FolderPrioritySchedulerDirectionState{
			QueuedBytes:                 21,
			ActiveBytes:                 22,
			OldestSchedulingWaitSeconds: 23,
		},
		SourceHashWork: FolderPrioritySourceHashWorkState{
			Queued:                      31,
			Active:                      32,
			OldestSchedulingWaitSeconds: 33,
			HashCapacity:                34,
			RetainedHandles:             35,
			RetainedHandleBudget:        36,
		},
	}}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newFolderPriorityMetricsCollector(wrapper, &provider))

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]map[string]float64)
	for _, family := range families {
		values := make(map[string]float64)
		for _, metric := range family.Metric {
			labels := make(map[string]string)
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			for label := range labels {
				if label != "folder" && label != "work_class" {
					t.Fatalf("metric %q exposes unbounded label %q", family.GetName(), label)
				}
			}
			key := "node"
			if folder := labels["folder"]; folder != "" {
				key = folder
			}
			if workClass := labels["work_class"]; workClass != "" {
				key = workClass + "/" + key
			}
			values[key] = metric.GetGauge().GetValue()
		}
		got[family.GetName()] = values
	}
	want := map[string]map[string]float64{
		"syncthing_model_folder_priority_active_bytes": {
			"download/alpha": 22,
			"upload/alpha":   12,
		},
		"syncthing_model_folder_priority_hash_capacity": {
			"node": 34,
		},
		"syncthing_model_folder_priority_oldest_scheduling_wait_seconds": {
			"download/alpha":    23,
			"source_hash/alpha": 33,
			"upload/alpha":      13,
		},
		"syncthing_model_folder_priority_queued_bytes": {
			"download/alpha": 21,
			"upload/alpha":   11,
		},
		"syncthing_model_folder_priority_retained_handle_budget": {
			"node": 36,
		},
		"syncthing_model_folder_priority_retained_handles": {
			"node": 35,
		},
		"syncthing_model_folder_priority_scheduler_active": {
			"download/alpha":    1,
			"source_hash/alpha": 1,
			"upload/alpha":      1,
		},
		"syncthing_model_folder_priority_source_hash_work_active": {
			"alpha": 32,
		},
		"syncthing_model_folder_priority_source_hash_work_queued": {
			"alpha": 31,
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

	provider.state.SourceHashWork.Queued = 0
	provider.state.SourceHashWork.Active = 0
	provider.state.SourceHashWork.OldestSchedulingWaitSeconds = 0
	provider.state.SourceHashWork.RetainedHandles = 0
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			sourceHashMetric := family.GetName() == "syncthing_model_folder_priority_source_hash_work_queued" ||
				family.GetName() == "syncthing_model_folder_priority_source_hash_work_active" ||
				family.GetName() == "syncthing_model_folder_priority_retained_handles"
			for _, label := range metric.Label {
				if family.GetName() == "syncthing_model_folder_priority_oldest_scheduling_wait_seconds" && label.GetName() == "work_class" && label.GetValue() == "source_hash" {
					sourceHashMetric = true
				}
			}
			if sourceHashMetric && metric.GetGauge().GetValue() != 0 {
				t.Fatalf("second scrape retained completed Source Hash Work in %q: %v", family.GetName(), metric.GetGauge().GetValue())
			}
		}
	}
}
