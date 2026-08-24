// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/syncthing/syncthing/lib/config"
)

type folderPriorityMetricsCollector struct {
	cfg      config.Wrapper
	provider FolderPrioritySchedulerStateProvider

	activeDesc               *prometheus.Desc
	queuedDesc               *prometheus.Desc
	activeBytesDesc          *prometheus.Desc
	waitDesc                 *prometheus.Desc
	sourceQueuedDesc         *prometheus.Desc
	sourceActiveDesc         *prometheus.Desc
	hashCapacityDesc         *prometheus.Desc
	retainedHandlesDesc      *prometheus.Desc
	retainedHandleBudgetDesc *prometheus.Desc
}

func newFolderPriorityMetricsCollector(cfg config.Wrapper, provider FolderPrioritySchedulerStateProvider) *folderPriorityMetricsCollector {
	labels := []string{"folder", "work_class"}
	return &folderPriorityMetricsCollector{
		cfg:      cfg,
		provider: provider,
		activeDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_scheduler_active",
			"Whether Folder Priority scheduling is active by Folder and work class.",
			labels,
			nil,
		),
		queuedDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_queued_bytes",
			"Current queued Block Transfer bytes by folder and direction.",
			labels,
			nil,
		),
		activeBytesDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_active_bytes",
			"Current active Block Transfer bytes by folder and direction.",
			labels,
			nil,
		),
		waitDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_oldest_scheduling_wait_seconds",
			"Current oldest Scheduling Wait in seconds by Folder and work class.",
			labels,
			nil,
		),
		sourceQueuedDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_source_hash_work_queued",
			"Current queued Source Hash Work by Folder.",
			[]string{"folder"},
			nil,
		),
		sourceActiveDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_source_hash_work_active",
			"Current active Source Hash Work by Folder.",
			[]string{"folder"},
			nil,
		),
		hashCapacityDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_hash_capacity",
			"Current effective node-wide Hash Capacity.",
			nil,
			nil,
		),
		retainedHandlesDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_retained_handles",
			"Current node-wide retained source handle usage.",
			nil,
			nil,
		),
		retainedHandleBudgetDesc: prometheus.NewDesc(
			"syncthing_model_folder_priority_retained_handle_budget",
			"Current node-wide retained source handle budget.",
			nil,
			nil,
		),
	}
}

// RegisterFolderPriorityMetrics registers live Folder Priority scheduler
// metrics for the process model.
func RegisterFolderPriorityMetrics(cfg config.Wrapper, m Model) {
	provider, ok := m.(FolderPrioritySchedulerStateProvider)
	if !ok {
		return
	}
	prometheus.DefaultRegisterer.MustRegister(newFolderPriorityMetricsCollector(cfg, provider))
}

func (c *folderPriorityMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeDesc
	ch <- c.queuedDesc
	ch <- c.activeBytesDesc
	ch <- c.waitDesc
	ch <- c.sourceQueuedDesc
	ch <- c.sourceActiveDesc
	ch <- c.hashCapacityDesc
	ch <- c.retainedHandlesDesc
	ch <- c.retainedHandleBudgetDesc
}

func (c *folderPriorityMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	nodeState := c.provider.FolderPrioritySchedulerState("").SourceHashWork
	ch <- prometheus.MustNewConstMetric(c.hashCapacityDesc, prometheus.GaugeValue, float64(nodeState.HashCapacity))
	ch <- prometheus.MustNewConstMetric(c.retainedHandlesDesc, prometheus.GaugeValue, float64(nodeState.RetainedHandles))
	ch <- prometheus.MustNewConstMetric(c.retainedHandleBudgetDesc, prometheus.GaugeValue, float64(nodeState.RetainedHandleBudget))

	for _, folder := range c.cfg.FolderList() {
		state := c.provider.FolderPrioritySchedulerState(folder.ID)
		active := 0.0
		if state.Active {
			active = 1
		}
		for _, direction := range []struct {
			name  string
			state FolderPrioritySchedulerDirectionState
		}{
			{name: "upload", state: state.Upload},
			{name: "download", state: state.Download},
		} {
			labels := []string{folder.ID, direction.name}
			ch <- prometheus.MustNewConstMetric(c.activeDesc, prometheus.GaugeValue, active, labels...)
			ch <- prometheus.MustNewConstMetric(c.queuedDesc, prometheus.GaugeValue, float64(direction.state.QueuedBytes), labels...)
			ch <- prometheus.MustNewConstMetric(c.activeBytesDesc, prometheus.GaugeValue, float64(direction.state.ActiveBytes), labels...)
			ch <- prometheus.MustNewConstMetric(c.waitDesc, prometheus.GaugeValue, direction.state.OldestSchedulingWaitSeconds, labels...)
		}
		sourceLabels := []string{folder.ID, "source_hash"}
		ch <- prometheus.MustNewConstMetric(c.activeDesc, prometheus.GaugeValue, active, sourceLabels...)
		ch <- prometheus.MustNewConstMetric(c.waitDesc, prometheus.GaugeValue, state.SourceHashWork.OldestSchedulingWaitSeconds, sourceLabels...)
		ch <- prometheus.MustNewConstMetric(c.sourceQueuedDesc, prometheus.GaugeValue, float64(state.SourceHashWork.Queued), folder.ID)
		ch <- prometheus.MustNewConstMetric(c.sourceActiveDesc, prometheus.GaugeValue, float64(state.SourceHashWork.Active), folder.ID)
	}
}
