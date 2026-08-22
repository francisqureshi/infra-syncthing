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

type networkPriorityMetricsCollector struct {
	cfg      config.Wrapper
	provider NetworkPrioritySchedulerStateProvider

	activeDesc      *prometheus.Desc
	queuedDesc      *prometheus.Desc
	activeBytesDesc *prometheus.Desc
	waitDesc        *prometheus.Desc
}

func newNetworkPriorityMetricsCollector(cfg config.Wrapper, provider NetworkPrioritySchedulerStateProvider) *networkPriorityMetricsCollector {
	labels := []string{"folder", "direction"}
	return &networkPriorityMetricsCollector{
		cfg:      cfg,
		provider: provider,
		activeDesc: prometheus.NewDesc(
			"syncthing_model_network_priority_scheduler_active",
			"Whether Network Priority scheduling is active by folder and direction.",
			labels,
			nil,
		),
		queuedDesc: prometheus.NewDesc(
			"syncthing_model_network_priority_queued_bytes",
			"Current queued Block Transfer bytes by folder and direction.",
			labels,
			nil,
		),
		activeBytesDesc: prometheus.NewDesc(
			"syncthing_model_network_priority_active_bytes",
			"Current active Block Transfer bytes by folder and direction.",
			labels,
			nil,
		),
		waitDesc: prometheus.NewDesc(
			"syncthing_model_network_priority_oldest_scheduling_wait_seconds",
			"Current oldest Scheduling Wait in seconds among queued Block Transfers by folder and direction.",
			labels,
			nil,
		),
	}
}

// RegisterNetworkPriorityMetrics registers live Network Priority scheduler
// metrics for the process model.
func RegisterNetworkPriorityMetrics(cfg config.Wrapper, m Model) {
	provider, ok := m.(NetworkPrioritySchedulerStateProvider)
	if !ok {
		return
	}
	prometheus.DefaultRegisterer.MustRegister(newNetworkPriorityMetricsCollector(cfg, provider))
}

func (c *networkPriorityMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeDesc
	ch <- c.queuedDesc
	ch <- c.activeBytesDesc
	ch <- c.waitDesc
}

func (c *networkPriorityMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, folder := range c.cfg.FolderList() {
		state := c.provider.NetworkPrioritySchedulerState(folder.ID)
		active := 0.0
		if state.Active {
			active = 1
		}
		for _, direction := range []struct {
			name  string
			state NetworkPrioritySchedulerDirectionState
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
	}
}
