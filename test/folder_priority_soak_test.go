// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration && benchmark
// +build integration,benchmark

package integration

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
)

const (
	studioSoakDefaultRateKiB    = 64 * 1024
	studioSoakDefaultMultiplier = 4.0
)

func TestBenchmarkFolderPriorityStudioSoak(t *testing.T) {
	profile, safetyMultiplier, err := folderPriorityStudioSoakProfile()
	if err != nil {
		t.Fatal(err)
	}
	if err := studioDiskPreflight(profile.ArtifactRoot, studioTotalBytes(profile.ProjectSizes), safetyMultiplier); err != nil {
		t.Fatal(err)
	}
	t.Logf("Folder Priority studio soak: seed=%d bytes=%d distribution=%s send=%dKiB/s receive=%dKiB/s uploadInFlight=%dKiB downloadInFlight=%dKiB timeout=%s artifactRoot=%s diskMultiplier=%.2f",
		profile.Seed, studioTotalBytes(profile.ProjectSizes), studioEnvString("STUDIO_SOAK_DISTRIBUTION", "equal"),
		profile.SendRateKiB, profile.ReceiveRateKiB, profile.UploadInFlightKiB, profile.DownloadInFlightKiB,
		profile.Timeout, profile.ArtifactRoot, safetyMultiplier)
	runFolderPriorityStudioWorkload(t, profile)
}

func folderPriorityStudioSoakProfile() (folderPriorityStudioProfile, float64, error) {
	totalBytes, err := studioParseBytes(studioEnvString("STUDIO_SOAK_TOTAL_BYTES", "12GiB"))
	if err != nil {
		return folderPriorityStudioProfile{}, 0, fmt.Errorf("STUDIO_SOAK_TOTAL_BYTES: %w", err)
	}
	distribution := strings.ToLower(studioEnvString("STUDIO_SOAK_DISTRIBUTION", "equal"))
	sizes, err := studioDistributeBytes(totalBytes, distribution)
	if err != nil {
		return folderPriorityStudioProfile{}, 0, err
	}
	timeout, err := time.ParseDuration(studioEnvString("STUDIO_SOAK_DURATION", "12h"))
	if err != nil || timeout <= 0 {
		return folderPriorityStudioProfile{}, 0, fmt.Errorf("STUDIO_SOAK_DURATION must be a positive Go duration")
	}
	sendRate, err := studioEnvPositiveInt("STUDIO_SOAK_SEND_KIB", studioSoakDefaultRateKiB)
	if err != nil {
		return folderPriorityStudioProfile{}, 0, err
	}
	receiveRate, err := studioEnvPositiveInt("STUDIO_SOAK_RECEIVE_KIB", studioSoakDefaultRateKiB)
	if err != nil {
		return folderPriorityStudioProfile{}, 0, err
	}
	uploadLimit, err := studioEnvPositiveInt("STUDIO_SOAK_UPLOAD_INFLIGHT_KIB", studioDefaultInFlightKiB)
	if err != nil {
		return folderPriorityStudioProfile{}, 0, err
	}
	downloadLimit, err := studioEnvPositiveInt("STUDIO_SOAK_DOWNLOAD_INFLIGHT_KIB", studioDefaultInFlightKiB)
	if err != nil {
		return folderPriorityStudioProfile{}, 0, err
	}
	if uploadLimit < studioDefaultInFlightKiB || downloadLimit < studioDefaultInFlightKiB {
		return folderPriorityStudioProfile{}, 0, fmt.Errorf("soak In-Flight Limits must be at least %d KiB", studioDefaultInFlightKiB)
	}
	multiplier, err := strconv.ParseFloat(studioEnvString("STUDIO_SOAK_DISK_MULTIPLIER", "4"), 64)
	if err != nil || multiplier < studioSoakDefaultMultiplier {
		return folderPriorityStudioProfile{}, 0, fmt.Errorf("STUDIO_SOAK_DISK_MULTIPLIER must be at least %.1f", studioSoakDefaultMultiplier)
	}
	return folderPriorityStudioProfile{
		Name:                "soak",
		Seed:                studioEnvInt64("STUDIO_SEED", studioDefaultSeed),
		ProjectSizes:        sizes,
		SendRateKiB:         sendRate,
		ReceiveRateKiB:      receiveRate,
		UploadInFlightKiB:   uploadLimit,
		DownloadInFlightKiB: downloadLimit,
		ObservationInterval: 2 * time.Second,
		Timeout:             timeout,
		ArtifactRoot:        studioEnvString("STUDIO_ARTIFACT_DIR", filepath.Join("logs", "folder-priority")),
		RetainArtifacts:     true,
	}, multiplier, nil
}

func studioParseBytes(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("size is empty")
	}
	upper := strings.ToUpper(value)
	multiplier := float64(1)
	for _, unit := range []struct {
		suffix string
		factor float64
	}{
		{"TIB", 1 << 40},
		{"GIB", 1 << 30},
		{"MIB", 1 << 20},
		{"KIB", 1 << 10},
		{"TB", 1e12},
		{"GB", 1e9},
		{"MB", 1e6},
		{"KB", 1e3},
		{"B", 1},
	} {
		if strings.HasSuffix(upper, unit.suffix) {
			upper = strings.TrimSpace(strings.TrimSuffix(upper, unit.suffix))
			multiplier = unit.factor
			break
		}
	}
	number, err := strconv.ParseFloat(upper, 64)
	if err != nil || number <= 0 || number > float64(math.MaxInt64)/multiplier {
		return 0, fmt.Errorf("invalid positive byte size %q", value)
	}
	return int64(number * multiplier), nil
}

func studioDistributeBytes(total int64, distribution string) ([]int64, error) {
	if total < studioProjectCount {
		return nil, fmt.Errorf("total soak bytes %d is smaller than %d projects", total, studioProjectCount)
	}
	weights := make([]int64, studioProjectCount)
	switch distribution {
	case "equal":
		for i := range weights {
			weights[i] = 1
		}
	case "ramp":
		for i := range weights {
			weights[i] = int64(i + 1)
		}
	default:
		return nil, fmt.Errorf("STUDIO_SOAK_DISTRIBUTION must be equal or ramp, got %q", distribution)
	}
	var totalWeight int64
	for _, weight := range weights {
		totalWeight += weight
	}
	sizes := make([]int64, studioProjectCount)
	var assigned int64
	for i, weight := range weights {
		sizes[i] = total * weight / totalWeight
		assigned += sizes[i]
	}
	for remaining, i := total-assigned, 0; remaining > 0; remaining, i = remaining-1, (i+1)%len(sizes) {
		sizes[i]++
	}
	return sizes, nil
}

func studioDiskPreflight(root string, logicalBytes int64, multiplier float64) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	usage, err := disk.Usage(root)
	if err != nil {
		return fmt.Errorf("measure free disk at %s: %w", root, err)
	}
	required := uint64(math.Ceil(float64(logicalBytes) * multiplier))
	if usage.Free < required {
		return fmt.Errorf("studio soak refused: %d bytes free at %s, require %d bytes for %d logical bytes with %.2fx safety multiplier", usage.Free, root, required, logicalBytes, multiplier)
	}
	return nil
}

func studioTotalBytes(sizes []int64) int64 {
	var total int64
	for _, size := range sizes {
		total += size
	}
	return total
}

func studioEnvPositiveInt(name string, fallback int) (int, error) {
	value := studioEnvString(name, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}
