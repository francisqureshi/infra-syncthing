// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rc"
)

const (
	studioPeerHub = iota
	studioPeerIngest
	studioPeerEdit
	studioPeerCount

	studioHighProjects    = 5
	studioNormalProjects  = 15
	studioLowProjects     = 5
	studioDynamicProjects = 5
	studioProjectCount    = studioHighProjects + studioNormalProjects + studioLowProjects + studioDynamicProjects

	studioDefaultSeed        = int64(363)
	studioDefaultProjectMiB  = int64(12)
	studioDefaultRateKiB     = 8 * 1024
	studioDefaultInFlightKiB = 2 * protocol.MaxBlockSize / 1024
	studioReentryMaxBytes    = int64(48 << 20)
	studioControlMaxBytes    = int64(48 << 20)
	studioDemotionMaxBytes   = int64(48 << 20)
	studioDirectionMaxBytes  = int64(96 << 20)
)

type studioProjectClass string

const (
	studioClassHigh    studioProjectClass = "high"
	studioClassNormal  studioProjectClass = "normal"
	studioClassLow     studioProjectClass = "low"
	studioClassDynamic studioProjectClass = "dynamic"
)

type networkPriorityStudioProfile struct {
	Name                string
	Seed                int64
	ProjectSizes        []int64
	SendRateKiB         int
	ReceiveRateKiB      int
	UploadInFlightKiB   int
	DownloadInFlightKiB int
	ObservationInterval time.Duration
	Timeout             time.Duration
	ArtifactRoot        string
	RetainArtifacts     bool
}

type studioProject struct {
	ID                     string             `json:"id"`
	Class                  studioProjectClass `json:"class"`
	Source                 int                `json:"sourcePeer"`
	InitialNetworkPriority int                `json:"initialNetworkPriority"`
	FinalNetworkPriority   int                `json:"finalNetworkPriority"`
	Size                   int64              `json:"size"`
	ReentryBytes           int64              `json:"reentryBytes,omitempty"`
	ControlBytes           int64              `json:"controlBytes,omitempty"`
	DemotionBytes          int64              `json:"demotionBytes,omitempty"`
	DirectionBytes         int64              `json:"directionBytes,omitempty"`
}

type studioActiveTransfer struct {
	ProjectID   string
	InSyncBytes int64
	ActiveBytes int64
}

type studioPeer struct {
	index            int
	name             string
	id               protocol.DeviceID
	apiAddress       string
	listenAddress    string
	home             string
	process          *rc.Process
	starts           int
	transferredBytes int64
	configRevision   int
}

type studioProcessUsage struct {
	UserCPUSeconds   float64 `json:"userCpuSeconds"`
	SystemCPUSeconds float64 `json:"systemCpuSeconds"`
	PeakRSSKiB       int64   `json:"peakRssKiB,omitempty"`
}

type studioDirectionStatus struct {
	QueuedBytes                 int64   `json:"queuedBytes"`
	ActiveBytes                 int64   `json:"activeBytes"`
	OldestSchedulingWaitSeconds float64 `json:"oldestSchedulingWaitSeconds"`
}

type studioFolderStatus struct {
	GlobalBytes                     int64  `json:"globalBytes"`
	InSyncBytes                     int64  `json:"inSyncBytes"`
	NeedBytes                       int64  `json:"needBytes"`
	NeedTotalItems                  int    `json:"needTotalItems"`
	State                           string `json:"state"`
	NetworkPrioritySchedulingActive bool   `json:"networkPrioritySchedulingActive"`
	NetworkPriorityScheduling       struct {
		Upload   studioDirectionStatus `json:"upload"`
		Download studioDirectionStatus `json:"download"`
	} `json:"networkPriorityScheduling"`
}

type studioTimelineEntry struct {
	At                          time.Time `json:"at"`
	ElapsedMilliseconds         int64     `json:"elapsedMilliseconds"`
	Seed                        int64     `json:"seed"`
	Phase                       string    `json:"phase"`
	Kind                        string    `json:"kind"`
	Peer                        string    `json:"peer,omitempty"`
	Folder                      string    `json:"folder,omitempty"`
	Direction                   string    `json:"direction,omitempty"`
	NetworkPriority             int       `json:"networkPriority,omitempty"`
	GlobalBytes                 int64     `json:"globalBytes,omitempty"`
	InSyncBytes                 int64     `json:"inSyncBytes,omitempty"`
	NeedBytes                   int64     `json:"needBytes,omitempty"`
	QueuedBytes                 int64     `json:"queuedBytes,omitempty"`
	ActiveBytes                 int64     `json:"activeBytes,omitempty"`
	OldestSchedulingWaitSeconds float64   `json:"oldestSchedulingWaitSeconds,omitempty"`
	ConfiguredRateKiB           int       `json:"configuredRateKiB,omitempty"`
	InFlightLimitKiB            int       `json:"inFlightLimitKiB,omitempty"`
	ConnectedDevices            []string  `json:"connectedDevices,omitempty"`
	ConfigRevision              int       `json:"configRevision,omitempty"`
	EventID                     int       `json:"eventId,omitempty"`
	Detail                      string    `json:"detail,omitempty"`
}

type studioReport struct {
	Profile                   string                          `json:"profile"`
	Seed                      int64                           `json:"seed"`
	StartedAt                 time.Time                       `json:"startedAt"`
	FinishedAt                time.Time                       `json:"finishedAt"`
	DurationSeconds           float64                         `json:"durationSeconds"`
	LogicalSourceBytes        int64                           `json:"logicalSourceBytes"`
	CompletionOrder           []string                        `json:"completionOrder"`
	NormalFairnessSpread      int64                           `json:"normalFairnessSpreadBytes"`
	NormalFairnessLimit       int64                           `json:"normalFairnessLimitBytes"`
	LowWorkRateBytesPerSecond float64                         `json:"lowWorkRateBytesPerSecond"`
	HubUploadBytes            int64                           `json:"hubUploadBytes"`
	HubDownloadBytes          int64                           `json:"hubDownloadBytes"`
	TransferredBytes          int64                           `json:"transferredBytes"`
	RateLimitCompliance       string                          `json:"rateLimitCompliance"`
	InFlightLimitCompliance   string                          `json:"inFlightLimitCompliance"`
	Restarts                  int                             `json:"restarts"`
	Errors                    int                             `json:"observationErrors"`
	ChecksumResult            string                          `json:"checksumResult"`
	PeerUsage                 map[string][]studioProcessUsage `json:"peerUsage"`
	Limitations               []string                        `json:"limitations,omitempty"`
}

type networkPriorityStudio struct {
	t                              *testing.T
	profile                        networkPriorityStudioProfile
	projects                       []studioProject
	peers                          [studioPeerCount]*studioPeer
	runDir                         string
	started                        time.Time
	phaseMu                        sync.RWMutex
	phase                          string
	networkPriorityMu              sync.RWMutex
	networkPriorities              map[int]map[string]int
	timelineMu                     sync.Mutex
	timeline                       *os.File
	reportMu                       sync.Mutex
	report                         studioReport
	completionMu                   sync.RWMutex
	completionTime                 map[string]time.Time
	nonPreemptiveDemotionProjectID string
	nonPreemptiveLowProjectIDs     map[string]struct{}
	observerCancel                 context.CancelFunc
	observerDone                   chan struct{}
	closed                         bool
}

func TestNetworkPriorityStudioWorkload(t *testing.T) {
	runNetworkPriorityStudioWorkload(t, gatedNetworkPriorityStudioProfile())
}

func gatedNetworkPriorityStudioProfile() networkPriorityStudioProfile {
	seed := studioEnvInt64("STUDIO_SEED", studioDefaultSeed)
	sizes := make([]int64, studioProjectCount)
	for i := range sizes {
		sizes[i] = studioDefaultProjectMiB << 20
	}
	for i := studioHighProjects + studioNormalProjects; i < studioHighProjects+studioNormalProjects+studioLowProjects; i++ {
		sizes[i] = 64 << 20
	}
	return networkPriorityStudioProfile{
		Name:                "gated",
		Seed:                seed,
		ProjectSizes:        sizes,
		SendRateKiB:         studioDefaultRateKiB,
		ReceiveRateKiB:      studioDefaultRateKiB,
		UploadInFlightKiB:   studioDefaultInFlightKiB,
		DownloadInFlightKiB: studioDefaultInFlightKiB,
		ObservationInterval: 2 * time.Second,
		Timeout:             12 * time.Minute,
		ArtifactRoot:        studioEnvString("STUDIO_ARTIFACT_DIR", filepath.Join("logs", "network-priority")),
	}
}

func runNetworkPriorityStudioWorkload(t *testing.T, profile networkPriorityStudioProfile) {
	t.Helper()
	studio, err := newNetworkPriorityStudio(t, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer studio.close()

	ctx, cancel := context.WithTimeout(context.Background(), profile.Timeout)
	defer cancel()

	studio.must(studio.prepare(), "prepare isolated studio topology")
	studio.must(studio.startPeers(ctx), "start studio peers")
	studio.must(studio.configureThroughREST(ctx), "apply rates, In-Flight Limits, and Network Priority through REST")
	studio.startObserver(ctx)

	studio.setPhase("low-ingest")
	studio.must(studio.generateClass(studioClassLow), "generate deterministic Low ingest data")
	studio.must(studio.resumeAll(), "connect studio peers")
	studio.must(studio.rescanClass(studioClassLow), "scan Low ingest data")
	studio.must(studio.waitForLowProgress(ctx), "wait for Low work to use idle capacity")
	studio.must(studio.measureLowWorkConservation(ctx), "measure Low work conservation and rate compliance")

	studio.setPhase("mixed-workload")
	studio.must(studio.generateNonLow(), "generate High, Normal, and Dynamic data")
	studio.must(studio.rescanNonLow(ctx), "queue High, Normal, and Dynamic data")
	studio.must(studio.waitForMixedNetworkPriorityQueue(ctx), "observe mixed Network Priority queue")
	studio.must(studio.verifyDirectionalNetworkPriority(ctx), "verify direction-specific Network Priority behavior")
	studio.must(studio.measureIndependentDirections(ctx), "validate simultaneous upload and download")
	studio.must(studio.verifyStrictWaiting(ctx), "observe strict Scheduling Wait")

	studio.setPhase("live-reprioritization")
	activeDynamic, err := studio.activateDynamicDemotion(ctx)
	studio.must(err, "observe active Dynamic work before reprioritization")
	studio.must(studio.exerciseLifecycleAndReprioritization(ctx, activeDynamic), "exercise lifecycle pressure and live REST reprioritization")
	studio.must(studio.verifyNoDaemonRestartForNetworkPriorityChanges(), "verify live Network Priority changes did not restart available daemons")

	studio.setPhase("strict-drain")
	studio.must(studio.waitForHighPrecedence(ctx), "verify later High work precedes comparison projects")

	studio.setPhase("equal-priority-share")
	studio.must(studio.measureNormalFairness(ctx), "measure Equal-Priority Share")
	studio.must(studio.exerciseFolderPause(ctx), "exercise folder pause and resume")
	studio.must(studio.verifyLowResumes(ctx), "verify Low work resumes after higher Network Priority work drains")

	studio.setPhase("convergence")
	studio.must(studio.waitForConvergence(ctx), "wait for all 30 projects to converge")
	studio.must(studio.waitForCompletionEvents(ctx, studio.projects), "wait for all completion events")
	studio.must(studio.verifyCompletionPrecedence(), "verify High/Normal/Low and Dynamic completion precedence")
	studio.must(studio.verifyDirectories(), "verify byte-identical contents and no partial files")
	studio.reportMu.Lock()
	studio.report.ChecksumResult = "passed"
	studio.reportMu.Unlock()
}

func newNetworkPriorityStudio(t *testing.T, profile networkPriorityStudioProfile) (*networkPriorityStudio, error) {
	if len(profile.ProjectSizes) != studioProjectCount {
		return nil, fmt.Errorf("profile has %d project sizes, expected %d", len(profile.ProjectSizes), studioProjectCount)
	}
	if profile.SendRateKiB <= 0 || profile.ReceiveRateKiB <= 0 {
		return nil, errors.New("studio send and receive rates must be positive")
	}
	if profile.UploadInFlightKiB < studioDefaultInFlightKiB || profile.DownloadInFlightKiB < studioDefaultInFlightKiB {
		return nil, fmt.Errorf("studio In-Flight Limits must be at least %d KiB", studioDefaultInFlightKiB)
	}
	if profile.ProjectSizes[studioHighProjects] < 2 ||
		profile.ProjectSizes[studioHighProjects+studioNormalProjects] < 2 ||
		profile.ProjectSizes[studioHighProjects+studioNormalProjects+1] < 2 ||
		profile.ProjectSizes[studioHighProjects+studioNormalProjects+studioLowProjects+2] < 2 {
		return nil, errors.New("selected Normal, Low, and Dynamic projects must reserve bytes for scheduler control work")
	}
	if err := os.MkdirAll(profile.ArtifactRoot, 0o755); err != nil {
		return nil, err
	}
	runDir := filepath.Join(profile.ArtifactRoot, fmt.Sprintf("%s-seed-%d-%d", profile.Name, profile.Seed, time.Now().UnixNano()))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, err
	}
	timeline, err := os.Create(filepath.Join(runDir, "timeline.jsonl"))
	if err != nil {
		return nil, err
	}
	studio := &networkPriorityStudio{
		t:                          t,
		profile:                    profile,
		runDir:                     runDir,
		started:                    time.Now().UTC(),
		phase:                      "setup",
		networkPriorities:          make(map[int]map[string]int),
		timeline:                   timeline,
		observerDone:               make(chan struct{}),
		completionTime:             make(map[string]time.Time),
		nonPreemptiveLowProjectIDs: make(map[string]struct{}),
	}
	studio.projects = studioProjectPlan(profile.ProjectSizes)
	studio.report = studioReport{
		Profile:                 profile.Name,
		Seed:                    profile.Seed,
		StartedAt:               studio.started,
		PeerUsage:               make(map[string][]studioProcessUsage),
		RateLimitCompliance:     "not-run",
		InFlightLimitCompliance: "not-run",
		ChecksumResult:          "not-run",
	}
	for _, project := range studio.projects {
		studio.report.LogicalSourceBytes += project.Size
	}
	return studio, nil
}

func studioProjectPlan(sizes []int64) []studioProject {
	projects := make([]studioProject, 0, studioProjectCount)
	nextSize := func() int64 {
		return sizes[len(projects)]
	}
	for i := 0; i < studioHighProjects; i++ {
		networkPriority := 50
		if i == 0 {
			networkPriority = config.NetworkPriorityMax
		}
		projects = append(projects, studioProject{
			ID: fmt.Sprintf("studio-high-%02d", i), Class: studioClassHigh, Source: studioPeerHub,
			InitialNetworkPriority: networkPriority, FinalNetworkPriority: networkPriority, Size: nextSize(),
		})
	}
	for i := 0; i < studioNormalProjects; i++ {
		project := studioProject{
			ID: fmt.Sprintf("studio-normal-%02d", i), Class: studioClassNormal, Source: studioPeerHub,
			InitialNetworkPriority: 0, FinalNetworkPriority: 0, Size: nextSize(),
		}
		if i == 0 {
			project.ControlBytes = min(project.Size-1, studioControlMaxBytes)
		}
		projects = append(projects, project)
	}
	for i := 0; i < studioLowProjects; i++ {
		project := studioProject{
			ID: fmt.Sprintf("studio-low-%02d", i), Class: studioClassLow, Source: studioPeerIngest,
			InitialNetworkPriority: -50, FinalNetworkPriority: -50, Size: nextSize(),
		}
		if i == 0 {
			project.ReentryBytes = min(project.Size-1, studioReentryMaxBytes)
		} else if i == 1 {
			project.DirectionBytes = min(project.Size-1, studioDirectionMaxBytes)
		}
		projects = append(projects, project)
	}
	dynamicInitialNetworkPriorities := []int{0, -50, 50, 0, 0}
	dynamicFinalNetworkPriorities := []int{50, 50, -50, 50, -50}
	for i := 0; i < studioDynamicProjects; i++ {
		project := studioProject{
			ID: fmt.Sprintf("studio-dynamic-%02d", i), Class: studioClassDynamic, Source: studioPeerHub,
			InitialNetworkPriority: dynamicInitialNetworkPriorities[i], FinalNetworkPriority: dynamicFinalNetworkPriorities[i], Size: nextSize(),
		}
		if i == 2 {
			project.DemotionBytes = min(project.Size-1, studioDemotionMaxBytes)
		}
		projects = append(projects, project)
	}
	return projects
}

func (s *networkPriorityStudio) prepare() error {
	ids := []string{id1, id2, id3}
	names := []string{"storage-hub", "ingest", "edit-delivery"}
	for i := 0; i < studioPeerCount; i++ {
		id, err := protocol.DeviceIDFromString(ids[i])
		if err != nil {
			return err
		}
		peer := &studioPeer{
			index:         i,
			name:          names[i],
			id:            id,
			apiAddress:    fmt.Sprintf("127.0.0.1:%d", 8081+i),
			listenAddress: fmt.Sprintf("tcp://127.0.0.1:%d", 22001+i),
			home:          filepath.Join(s.runDir, names[i]+"-home"),
		}
		if err := os.MkdirAll(peer.home, 0o755); err != nil {
			return err
		}
		for _, name := range []string{"cert.pem", "key.pem"} {
			if err := studioCopyFile(filepath.Join(fmt.Sprintf("h%d", i+1), name), filepath.Join(peer.home, name)); err != nil {
				return fmt.Errorf("copy %s fixture for %s: %w", name, peer.name, err)
			}
		}
		s.peers[i] = peer
		s.networkPriorities[i] = make(map[string]int)
	}

	for _, peer := range s.peers {
		if err := s.writePeerConfig(peer); err != nil {
			return err
		}
	}
	plan, err := json.MarshalIndent(s.projects, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.runDir, "project-plan.json"), append(plan, '\n'), 0o644)
}

func (s *networkPriorityStudio) writePeerConfig(peer *studioPeer) error {
	cfg := config.New(peer.id)
	cfg.GUI.RawAddress = peer.apiAddress
	cfg.GUI.APIKey = apiKey
	cfg.Options.RawListenAddresses = []string{peer.listenAddress}
	cfg.Options.GlobalAnnEnabled = false
	cfg.Options.LocalAnnEnabled = false
	cfg.Options.RelaysEnabled = false
	cfg.Options.NATEnabled = false
	cfg.Options.ReconnectIntervalS = 5
	cfg.Options.LimitBandwidthInLan = true
	cfg.Options.AutoUpgradeIntervalH = 0
	cfg.Options.URAccepted = -1

	for _, remote := range s.peers {
		if remote.index == peer.index {
			continue
		}
		dev := cfg.Defaults.Device.Copy()
		dev.DeviceID = remote.id
		dev.Name = remote.name
		dev.Addresses = []string{remote.listenAddress}
		dev.RawNumConnections = -1
		cfg.SetDevice(dev)
	}

	allDevices := make([]config.FolderDeviceConfiguration, 0, studioPeerCount)
	for _, projectPeer := range s.peers {
		allDevices = append(allDevices, config.FolderDeviceConfiguration{DeviceID: projectPeer.id})
	}
	for _, project := range s.projects {
		folder := cfg.Defaults.Folder.Copy()
		folder.ID = project.ID
		folder.Label = project.ID
		folder.Path = s.folderPath(peer, project.ID)
		folder.Type = config.FolderTypeSendReceive
		folder.Devices = slices.Clone(allDevices)
		folder.RescanIntervalS = 3600
		folder.FSWatcherEnabled = false
		folder.IgnorePerms = true
		folder.DisableFsync = true
		folder.MaxConflicts = -1
		folder.NetworkPriority = 0
		if project.ReentryBytes > 0 {
			folder.PullerPauseS = 1
		}
		cfg.SetFolder(folder)
		if err := os.MkdirAll(folder.Path, 0o755); err != nil {
			return err
		}
	}
	fd, err := os.Create(filepath.Join(peer.home, "config.xml"))
	if err != nil {
		return err
	}
	defer fd.Close()
	return cfg.WriteXML(fd)
}

func (s *networkPriorityStudio) startPeers(ctx context.Context) error {
	for _, peer := range s.peers {
		if err := s.startPeer(ctx, peer); err != nil {
			return err
		}
		if err := peer.process.PauseAll(); err != nil {
			return fmt.Errorf("pause devices on %s: %w", peer.name, err)
		}
	}
	return nil
}

func (s *networkPriorityStudio) startPeer(ctx context.Context, peer *studioPeer) error {
	bin, err := filepath.Abs(filepath.Join("..", "bin", "syncthing"))
	if err != nil {
		return err
	}
	if _, err := os.Stat(bin); err != nil {
		return fmt.Errorf("integration binary %q is unavailable; build it with `go run build.go build syncthing`: %w", bin, err)
	}
	peer.starts++
	process := rc.NewProcess(peer.apiAddress)
	logName := fmt.Sprintf("%s-%02d.log", peer.name, peer.starts)
	if err := process.LogTo(filepath.Join(s.runDir, logName)); err != nil {
		return err
	}
	if err := process.Start(bin, "--home", peer.home, "--no-browser", "--no-restart"); err != nil {
		return fmt.Errorf("start %s: %w", peer.name, err)
	}
	peer.process = process
	started := make(chan struct{})
	go func() {
		process.AwaitStartup()
		close(started)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("await %s startup: %w", peer.name, ctx.Err())
	case <-process.Stopped():
		return fmt.Errorf("%s stopped during startup", peer.name)
	case <-started:
		return nil
	}
}

func (s *networkPriorityStudio) configureThroughREST(ctx context.Context) error {
	options := map[string]any{
		"maxSendKbps":                     s.profile.SendRateKiB,
		"maxRecvKbps":                     s.profile.ReceiveRateKiB,
		"limitBandwidthInLan":             true,
		"maxConcurrentIncomingRequestKiB": s.profile.UploadInFlightKiB,
		"maxConcurrentOutgoingRequestKiB": s.profile.DownloadInFlightKiB,
		"maxFolderConcurrency":            studioProjectCount,
	}
	for _, peer := range s.peers {
		if err := s.patchAndVerify(ctx, peer, "/rest/config/options", options, nil); err != nil {
			return err
		}
		for _, project := range s.projects {
			if err := s.patchNetworkPriority(ctx, peer, project.ID, project.InitialNetworkPriority); err != nil {
				return err
			}
		}
	}
	return s.captureTimeline(ctx, "configuration-applied")
}

func (s *networkPriorityStudio) patchNetworkPriority(ctx context.Context, peer *studioPeer, folder string, networkPriority int) error {
	verify := func(bs []byte) error {
		var got config.FolderConfiguration
		if err := json.Unmarshal(bs, &got); err != nil {
			return err
		}
		if got.NetworkPriority != networkPriority {
			return fmt.Errorf("%s accepted Network Priority %d for %s as %d", peer.name, networkPriority, folder, got.NetworkPriority)
		}
		return nil
	}
	path := "/rest/config/folders/" + url.PathEscape(folder)
	if err := s.patchAndVerify(ctx, peer, path, map[string]int{"networkPriority": networkPriority}, verify); err != nil {
		return err
	}
	s.networkPriorityMu.Lock()
	s.networkPriorities[peer.index][folder] = networkPriority
	s.networkPriorityMu.Unlock()
	return nil
}

func (s *networkPriorityStudio) patchFolderPaused(ctx context.Context, peer *studioPeer, folder string, paused bool) error {
	verify := func(bs []byte) error {
		var got config.FolderConfiguration
		if err := json.Unmarshal(bs, &got); err != nil {
			return err
		}
		if got.Paused != paused {
			return fmt.Errorf("%s accepted paused=%t for %s as %t", peer.name, paused, folder, got.Paused)
		}
		return nil
	}
	return s.patchAndVerify(ctx, peer, "/rest/config/folders/"+url.PathEscape(folder), map[string]bool{"paused": paused}, verify)
}

func (s *networkPriorityStudio) patchAndVerify(ctx context.Context, peer *studioPeer, path string, body any, verify func([]byte) error) error {
	if _, err := s.requestJSON(ctx, peer, http.MethodPatch, path, body); err != nil {
		return fmt.Errorf("PATCH %s on %s: %w", path, peer.name, err)
	}
	accepted, err := s.requestJSON(ctx, peer, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("verify %s on %s: %w", path, peer.name, err)
	}
	if verify != nil {
		if err := verify(accepted); err != nil {
			return err
		}
	}
	peer.configRevision++
	s.record(studioTimelineEntry{
		Kind: "config-accepted", Peer: peer.name, ConfigRevision: peer.configRevision,
		Detail: fmt.Sprintf("PATCH %s", path),
	})
	return nil
}

func (s *networkPriorityStudio) requestJSON(ctx context.Context, peer *studioPeer, method, path string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(bs)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+peer.apiAddress+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", rc.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(bs)))
	}
	return bs, nil
}

func (s *networkPriorityStudio) generateClass(class studioProjectClass) error {
	for _, project := range s.projects {
		if project.Class != class {
			continue
		}
		if err := s.generateProject(project); err != nil {
			return err
		}
	}
	return nil
}

func (s *networkPriorityStudio) generateNonLow() error {
	for _, project := range s.projects {
		if project.Class == studioClassLow {
			continue
		}
		if err := s.generateProject(project); err != nil {
			return err
		}
	}
	return nil
}

func (s *networkPriorityStudio) generateProject(project studioProject) error {
	return s.generateProjectFile(project, "media.bin", project.Size-project.ReentryBytes-project.ControlBytes-project.DemotionBytes-project.DirectionBytes, project.ID)
}

func (s *networkPriorityStudio) generateProjectFile(project studioProject, name string, size int64, seedLabel string) error {
	path := filepath.Join(s.folderPath(s.peers[project.Source], project.ID), name)
	seed := s.profile.Seed + int64(studioStableHash(seedLabel))
	rng := rand.New(rand.NewSource(seed))
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(fd, rng, size); err != nil {
		fd.Close()
		return err
	}
	if err := fd.Close(); err != nil {
		return err
	}
	mtime := time.Unix(1_700_000_000+s.profile.Seed%10_000, 0).UTC()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		return err
	}
	sum, err := sha256file(path)
	if err != nil {
		return err
	}
	s.record(studioTimelineEntry{Kind: "generated", Folder: project.ID, Peer: s.peers[project.Source].name, Detail: fmt.Sprintf("file=%s bytes=%d sha256=%x", name, size, sum)})
	return nil
}

func (s *networkPriorityStudio) rescanClass(class studioProjectClass) error {
	for _, project := range s.projects {
		if project.Class == class {
			if err := s.peers[project.Source].process.Rescan(project.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *networkPriorityStudio) rescanNonLow(ctx context.Context) error {
	// Stage lower Network Priority work before High work so the profile
	// deterministically observes later High arrivals overtaking a live queue.
	for _, class := range []studioProjectClass{studioClassNormal, studioClassDynamic, studioClassHigh} {
		if class == studioClassHigh {
			if err := s.snapshotNonPreemptiveLow(ctx); err != nil {
				return err
			}
		}
		for _, project := range s.projects {
			if project.Class != class {
				continue
			}
			if err := s.peers[project.Source].process.Rescan(project.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *networkPriorityStudio) snapshotNonPreemptiveLow(ctx context.Context) error {
	var nonPreemptiveLow []string
	for _, project := range s.projectsByClass(studioClassLow) {
		if project.ReentryBytes > 0 || project.DirectionBytes > 0 {
			continue
		}
		status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
		if err != nil {
			return err
		}
		if status.NeedBytes == 0 || status.NetworkPriorityScheduling.Download.ActiveBytes > 0 {
			s.nonPreemptiveLowProjectIDs[project.ID] = struct{}{}
			nonPreemptiveLow = append(nonPreemptiveLow, project.ID)
		}
	}
	s.record(studioTimelineEntry{
		Kind: "snapshot", Peer: s.peers[studioPeerEdit].name,
		Detail: fmt.Sprintf("Low work complete or active immediately before High rescan=%v", nonPreemptiveLow),
	})
	return nil
}

func (s *networkPriorityStudio) resumeAll() error {
	for _, peer := range s.peers {
		if err := peer.process.ResumeAll(); err != nil {
			return err
		}
	}
	return nil
}

func (s *networkPriorityStudio) waitForLowProgress(ctx context.Context) error {
	return s.waitUntil(ctx, 200*time.Millisecond, func() (bool, error) {
		var global, inSync int64
		for _, project := range s.projectsByClass(studioClassLow) {
			status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
			if err != nil {
				return false, err
			}
			global += status.GlobalBytes
			inSync += status.InSyncBytes
		}
		return global > 0 && inSync >= 1<<20 && inSync < global, nil
	})
}

func (s *networkPriorityStudio) measureLowWorkConservation(ctx context.Context) error {
	before, err := s.aggregateProgress(ctx, studioPeerEdit, studioClassLow)
	if err != nil {
		return err
	}
	started := time.Now()
	if err := studioWaitContext(ctx, s.profile.ObservationInterval); err != nil {
		return err
	}
	after, err := s.aggregateProgress(ctx, studioPeerEdit, studioClassLow)
	if err != nil {
		return err
	}
	duration := time.Since(started).Seconds()
	delta := after - before
	rate := float64(delta) / duration
	configured := float64(s.profile.ReceiveRateKiB * 1024)
	block := int64(protocol.BlockSize(s.projectsByClass(studioClassLow)[0].Size))
	if rate < configured*0.45 {
		return fmt.Errorf("Low work used %.0f B/s, below 45%% of configured %0.f B/s", rate, configured)
	}
	if float64(delta) > configured*duration*1.35+float64(block) {
		return fmt.Errorf("Low work transferred %d bytes in %s, exceeding rate tolerance", delta, time.Since(started))
	}
	s.reportMu.Lock()
	s.report.LowWorkRateBytesPerSecond = rate
	s.reportMu.Unlock()
	s.record(studioTimelineEntry{Kind: "measurement", Detail: fmt.Sprintf("work-conservation lowBytes=%d duration=%s rate=%.0fB/s", delta, time.Since(started), rate)})
	return nil
}

func (s *networkPriorityStudio) waitForMixedNetworkPriorityQueue(ctx context.Context) error {
	high := s.projectsByClass(studioClassHigh)
	normal := s.projectsByClass(studioClassNormal)
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		var highNeeded bool
		for _, project := range high {
			status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
			if err != nil {
				return false, err
			}
			highNeeded = highNeeded || status.NeedBytes > 0
		}
		var normalNeeded bool
		for _, project := range normal {
			status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
			if err != nil {
				return false, err
			}
			normalNeeded = normalNeeded || status.NeedBytes > 0
		}
		return highNeeded && normalNeeded, nil
	})
}

func (s *networkPriorityStudio) verifyDirectionalNetworkPriority(ctx context.Context) error {
	high := s.projectsByClass(studioClassHigh)
	normal := s.projectsByClass(studioClassNormal)
	hub := s.peers[studioPeerHub]
	edit := s.peers[studioPeerEdit]
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		var hubHighUploadActive, editHighDownloadActive bool
		for _, project := range high {
			hubStatus, err := s.folderStatus(ctx, hub, project.ID)
			if err != nil {
				return false, err
			}
			editStatus, err := s.folderStatus(ctx, edit, project.ID)
			if err != nil {
				return false, err
			}
			hubHighUploadActive = hubHighUploadActive || hubStatus.NetworkPriorityScheduling.Upload.ActiveBytes > 0
			editHighDownloadActive = editHighDownloadActive || editStatus.NetworkPriorityScheduling.Download.ActiveBytes > 0
		}

		var hubNormalUploadQueued, editNormalDownloadQueued bool
		for _, project := range normal {
			hubStatus, err := s.folderStatus(ctx, hub, project.ID)
			if err != nil {
				return false, err
			}
			editStatus, err := s.folderStatus(ctx, edit, project.ID)
			if err != nil {
				return false, err
			}
			hubNormalUploadQueued = hubNormalUploadQueued || hubStatus.NetworkPriorityScheduling.Upload.QueuedBytes > 0
			editNormalDownloadQueued = editNormalDownloadQueued || editStatus.NetworkPriorityScheduling.Download.QueuedBytes > 0
		}

		if hubHighUploadActive && hubNormalUploadQueued && editHighDownloadActive && editNormalDownloadQueued {
			s.record(studioTimelineEntry{
				Kind: "assertion", Peer: hub.name,
				Detail: "High upload active while Normal upload queued; High download active while Normal download queued on Edit/Delivery",
			})
			return true, nil
		}
		return false, nil
	})
}

func (s *networkPriorityStudio) measureIndependentDirections(ctx context.Context) error {
	hub := s.peers[studioPeerHub]
	directionProject := s.projectsByClass(studioClassLow)[1]
	if directionProject.Source != studioPeerIngest || directionProject.DirectionBytes <= 0 {
		return fmt.Errorf("invalid independent direction project: %#v", directionProject)
	}
	before, err := hub.process.Connections()
	if err != nil {
		return err
	}
	started := time.Now()
	if err := s.generateProjectFile(directionProject, "direction-control.bin", directionProject.DirectionBytes, directionProject.ID+"/direction-control.bin"); err != nil {
		return fmt.Errorf("generate independent direction work: %w", err)
	}
	if err := s.peers[studioPeerIngest].process.Rescan(directionProject.ID); err != nil {
		return fmt.Errorf("scan independent direction work: %w", err)
	}
	maxUpload, maxDownload, err := s.waitForSimultaneousDirections(ctx, hub, directionProject)
	if err != nil {
		return err
	}
	observedUpload, observedDownload, err := s.maximumActiveBytes(ctx, hub)
	if err != nil {
		return err
	}
	maxUpload = max(maxUpload, observedUpload)
	maxDownload = max(maxDownload, observedDownload)
	if remaining := s.profile.ObservationInterval - time.Since(started); remaining > 0 {
		if err := studioWaitContext(ctx, remaining); err != nil {
			return err
		}
	}
	after, err := hub.process.Connections()
	if err != nil {
		return err
	}
	beforeIn, beforeOut := studioConnectionTotals(before)
	afterIn, afterOut := studioConnectionTotals(after)
	inDelta, outDelta := afterIn-beforeIn, afterOut-beforeOut
	if inDelta <= 0 || outDelta <= 0 {
		return fmt.Errorf("hub did not transfer in both directions: download=%d upload=%d", inDelta, outDelta)
	}
	duration := time.Since(started).Seconds()
	maxIn := int64(float64(s.profile.ReceiveRateKiB*1024)*duration*1.45) + int64(protocol.MaxBlockSize)
	maxOut := int64(float64(s.profile.SendRateKiB*1024)*duration*1.45) + int64(protocol.MaxBlockSize)
	if inDelta > maxIn || outDelta > maxOut {
		return fmt.Errorf("hub bypassed a configured rate: download=%d/%d upload=%d/%d", inDelta, maxIn, outDelta, maxOut)
	}
	if maxUpload == 0 || maxDownload == 0 {
		return fmt.Errorf("directional In-Flight telemetry did not observe simultaneous work: upload=%d download=%d", maxUpload, maxDownload)
	}
	// REST reports one folder at a time. A connection can finish one block and
	// admit the next between two reads, so a non-atomic sum may count one stale
	// block per direct connection in addition to the configured atomic cap.
	blockSize := int64(protocol.BlockSize(s.projects[0].Size))
	connectionCount := int64(studioPeerCount - 1)
	uploadObservationLimit := int64(s.profile.UploadInFlightKiB)*1024 + connectionCount*blockSize
	downloadObservationLimit := int64(s.profile.DownloadInFlightKiB)*1024 + connectionCount*blockSize
	if maxUpload > uploadObservationLimit || maxDownload > downloadObservationLimit {
		return fmt.Errorf("directional In-Flight observation tolerance exceeded: upload=%d/%d download=%d/%d", maxUpload, uploadObservationLimit, maxDownload, downloadObservationLimit)
	}
	s.reportMu.Lock()
	s.report.HubDownloadBytes += inDelta
	s.report.HubUploadBytes += outDelta
	s.report.RateLimitCompliance = "passed"
	s.report.InFlightLimitCompliance = "passed"
	s.reportMu.Unlock()
	s.record(studioTimelineEntry{Kind: "measurement", Peer: hub.name, Detail: fmt.Sprintf("simultaneous download=%d upload=%d maxActiveDownload=%d/%d maxActiveUpload=%d/%d", inDelta, outDelta, maxDownload, downloadObservationLimit, maxUpload, uploadObservationLimit)})
	return nil
}

func (s *networkPriorityStudio) waitForSimultaneousDirections(ctx context.Context, peer *studioPeer, downloadProject studioProject) (int64, int64, error) {
	var activeUpload, activeDownload int64
	err := s.waitUntil(ctx, 25*time.Millisecond, func() (bool, error) {
		var highUploadActive, downloadProjectActive bool
		activeUpload, activeDownload = 0, 0
		for _, project := range s.projects {
			status, err := s.folderStatus(ctx, peer, project.ID)
			if err != nil {
				return false, err
			}
			upload := status.NetworkPriorityScheduling.Upload.ActiveBytes
			download := status.NetworkPriorityScheduling.Download.ActiveBytes
			activeUpload += upload
			activeDownload += download
			if project.Class == studioClassHigh && upload > 0 {
				highUploadActive = true
			}
			if project.ID == downloadProject.ID && download > 0 {
				downloadProjectActive = true
			}
		}
		return highUploadActive && downloadProjectActive, nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("wait for simultaneous upload and download: %w", err)
	}
	return activeUpload, activeDownload, nil
}

func (s *networkPriorityStudio) maximumActiveBytes(ctx context.Context, peer *studioPeer) (int64, int64, error) {
	var maxUpload, maxDownload int64
	deadline := time.NewTimer(750 * time.Millisecond)
	defer deadline.Stop()
	for {
		var upload, download int64
		for _, project := range s.projects {
			status, err := s.folderStatus(ctx, peer, project.ID)
			if err != nil {
				return 0, 0, err
			}
			upload += status.NetworkPriorityScheduling.Upload.ActiveBytes
			download += status.NetworkPriorityScheduling.Download.ActiveBytes
		}
		maxUpload = max(maxUpload, upload)
		maxDownload = max(maxDownload, download)
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-deadline.C:
			return maxUpload, maxDownload, nil
		default:
		}
	}
}

func (s *networkPriorityStudio) verifyStrictWaiting(ctx context.Context) error {
	low := s.projectsByClass(studioClassLow)
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		for _, project := range low {
			status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
			if err != nil {
				return false, err
			}
			wait := status.NetworkPriorityScheduling.Download.OldestSchedulingWaitSeconds
			queued := status.NetworkPriorityScheduling.Download.QueuedBytes
			if queued > 0 && wait > 0 {
				s.record(studioTimelineEntry{Kind: "assertion", Peer: s.peers[studioPeerEdit].name, Folder: project.ID, Direction: "download", QueuedBytes: queued, OldestSchedulingWaitSeconds: wait, Detail: "intentional strict Network Priority starvation observed"})
				return true, nil
			}
		}
		return false, nil
	})
}

func (s *networkPriorityStudio) activateDynamicDemotion(ctx context.Context) (studioActiveTransfer, error) {
	dynamic := s.projectsByClass(studioClassDynamic)
	demoted := dynamic[2]
	if demoted.Source != studioPeerHub || demoted.DemotionBytes <= 0 {
		return studioActiveTransfer{}, fmt.Errorf("invalid active demotion project: %#v", demoted)
	}
	if err := s.generateProjectFile(demoted, "active-demotion.bin", demoted.DemotionBytes, demoted.ID+"/active-demotion.bin"); err != nil {
		return studioActiveTransfer{}, fmt.Errorf("generate active demotion work: %w", err)
	}
	if err := s.peers[studioPeerHub].process.Rescan(demoted.ID); err != nil {
		return studioActiveTransfer{}, fmt.Errorf("scan active demotion work: %w", err)
	}
	var active studioActiveTransfer
	err := s.waitUntil(ctx, 25*time.Millisecond, func() (bool, error) {
		status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], demoted.ID)
		if err != nil {
			return false, err
		}
		if status.NeedBytes > 0 && status.NetworkPriorityScheduling.Download.ActiveBytes > 0 {
			active = studioActiveTransfer{
				ProjectID:   demoted.ID,
				InSyncBytes: status.InSyncBytes,
				ActiveBytes: status.NetworkPriorityScheduling.Download.ActiveBytes,
			}
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return studioActiveTransfer{}, fmt.Errorf("wait for active Dynamic demotion work: %w", err)
	}
	return active, nil
}

func (s *networkPriorityStudio) exerciseLifecycleAndReprioritization(ctx context.Context, active studioActiveTransfer) error {
	dynamic := s.projectsByClass(studioClassDynamic)
	demoted := dynamic[2]
	if active.ProjectID != demoted.ID || active.ActiveBytes <= 0 {
		return fmt.Errorf("active demotion precondition missing for %s", demoted.ID)
	}
	edit := s.peers[studioPeerEdit]
	if err := s.patchNetworkPriority(ctx, edit, demoted.ID, demoted.FinalNetworkPriority); err != nil {
		return err
	}
	afterDemotion, err := s.folderStatus(ctx, edit, demoted.ID)
	if err != nil {
		return err
	}
	if afterDemotion.NetworkPriorityScheduling.Download.ActiveBytes == 0 && afterDemotion.InSyncBytes <= active.InSyncBytes {
		return fmt.Errorf("active Block Transfer for %s was neither retained nor completed across demotion", demoted.ID)
	}
	s.nonPreemptiveDemotionProjectID = demoted.ID
	s.record(studioTimelineEntry{
		Kind: "assertion", Peer: edit.name, Folder: demoted.ID, Direction: "download",
		NetworkPriority: demoted.FinalNetworkPriority,
		InSyncBytes:     afterDemotion.InSyncBytes,
		ActiveBytes:     afterDemotion.NetworkPriorityScheduling.Download.ActiveBytes,
		Detail:          fmt.Sprintf("active Block Transfer remained non-preemptive across demotion; beforeInSync=%d beforeActive=%d", active.InSyncBytes, active.ActiveBytes),
	})

	ingest := s.peers[studioPeerIngest]
	if err := s.stopPeer(ingest); err != nil {
		return err
	}
	if err := s.waitForConnection(ctx, s.peers[studioPeerHub], ingest.id, false); err != nil {
		return err
	}
	reentryProject := s.projectsByClass(studioClassLow)[0]
	if reentryProject.Source != studioPeerIngest || reentryProject.ReentryBytes <= 0 {
		return fmt.Errorf("invalid lifecycle re-entry project: %#v", reentryProject)
	}
	if err := s.generateProjectFile(reentryProject, "reentry.bin", reentryProject.ReentryBytes, reentryProject.ID+"/reentry.bin"); err != nil {
		return fmt.Errorf("generate unavailable re-entry work: %w", err)
	}
	outageStarted := time.Now()
	availableBefore, err := s.aggregateAvailableProgress(ctx)
	if err != nil {
		return err
	}

	for _, peer := range []*studioPeer{s.peers[studioPeerHub], s.peers[studioPeerEdit]} {
		for _, project := range dynamic {
			if peer.index == studioPeerEdit && project.ID == demoted.ID {
				continue
			}
			if err := s.patchNetworkPriority(ctx, peer, project.ID, project.FinalNetworkPriority); err != nil {
				return err
			}
		}
	}
	s.record(studioTimelineEntry{Kind: "lifecycle", Peer: ingest.name, Detail: "Network Priority changed on available peers while this peer was unavailable"})
	if err := s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		availableAfter, err := s.aggregateAvailableProgress(ctx)
		return availableAfter > availableBefore, err
	}); err != nil {
		return fmt.Errorf("available work did not progress while Ingest was unavailable: %w", err)
	}
	s.record(studioTimelineEntry{Kind: "assertion", Peer: ingest.name, Detail: "available High/Normal/Dynamic work progressed while Ingest work was unavailable"})

	// Leave the peer unavailable for one complete configured reconnect interval,
	// so the surviving peers enter retry/backoff before the work becomes runnable again.
	if remaining := 5500*time.Millisecond - time.Since(outageStarted); remaining > 0 {
		if err := studioWaitContext(ctx, remaining); err != nil {
			return err
		}
	}
	if err := s.startPeer(ctx, ingest); err != nil {
		return err
	}
	for _, project := range dynamic {
		if err := s.patchNetworkPriority(ctx, ingest, project.ID, project.FinalNetworkPriority); err != nil {
			return err
		}
	}
	if err := ingest.process.ResumeAll(); err != nil {
		return err
	}
	if err := s.waitForConnection(ctx, s.peers[studioPeerHub], ingest.id, true); err != nil {
		return err
	}
	if err := s.waitForConnection(ctx, s.peers[studioPeerEdit], ingest.id, true); err != nil {
		return err
	}
	controlProject := s.projectsByClass(studioClassNormal)[0]
	if controlProject.Source != studioPeerHub || controlProject.ControlBytes <= 0 {
		return fmt.Errorf("invalid lifecycle control project: %#v", controlProject)
	}
	if err := s.generateProjectFile(controlProject, "reentry-control.bin", controlProject.ControlBytes, controlProject.ID+"/reentry-control.bin"); err != nil {
		return fmt.Errorf("generate re-entry control work: %w", err)
	}
	if err := s.peers[studioPeerHub].process.Rescan(controlProject.ID); err != nil {
		return fmt.Errorf("scan re-entry control work: %w", err)
	}
	if err := s.waitForProjectDownloadActive(ctx, controlProject); err != nil {
		return err
	}
	if err := ingest.process.Rescan(reentryProject.ID); err != nil {
		return fmt.Errorf("scan re-entry work: %w", err)
	}
	if err := s.waitForReenteredStrictOrdering(ctx, reentryProject); err != nil {
		return err
	}
	s.reportMu.Lock()
	s.report.Restarts++
	s.reportMu.Unlock()
	s.record(studioTimelineEntry{Kind: "lifecycle", Peer: ingest.name, Detail: "disconnect, retry/backoff, restart, and strict Network Priority re-entry completed"})
	return nil
}

func (s *networkPriorityStudio) waitForProjectDownloadActive(ctx context.Context, project studioProject) error {
	edit := s.peers[studioPeerEdit]
	return s.waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		status, err := s.folderStatus(ctx, edit, project.ID)
		if err != nil {
			return false, err
		}
		return status.NeedBytes > 0 && status.NetworkPriorityScheduling.Download.ActiveBytes > 0, nil
	})
}

func (s *networkPriorityStudio) aggregateAvailableProgress(ctx context.Context) (int64, error) {
	var progress int64
	for _, class := range []studioProjectClass{studioClassHigh, studioClassNormal, studioClassDynamic} {
		classProgress, err := s.aggregateProgress(ctx, studioPeerEdit, class)
		if err != nil {
			return 0, err
		}
		progress += classProgress
	}
	return progress, nil
}

func (s *networkPriorityStudio) waitForReenteredStrictOrdering(ctx context.Context, reentryProject studioProject) error {
	edit := s.peers[studioPeerEdit]
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		var higherNetworkPriorityActive bool
		for _, project := range s.projects {
			if s.networkPriority(edit.index, project.ID) <= reentryProject.FinalNetworkPriority {
				continue
			}
			status, err := s.folderStatus(ctx, edit, project.ID)
			if err != nil {
				return false, err
			}
			higherNetworkPriorityActive = higherNetworkPriorityActive || status.NeedBytes > 0 && status.NetworkPriorityScheduling.Download.ActiveBytes > 0
		}

		reentryStatus, err := s.folderStatus(ctx, edit, reentryProject.ID)
		if err != nil {
			return false, err
		}
		lowQueued := reentryStatus.NetworkPriorityScheduling.Download.QueuedBytes
		lowActive := reentryStatus.NetworkPriorityScheduling.Download.ActiveBytes
		if higherNetworkPriorityActive && reentryStatus.GlobalBytes == reentryProject.Size && reentryStatus.NeedBytes > 0 && lowQueued > 0 {
			s.record(studioTimelineEntry{
				Kind: "assertion", Peer: edit.name, Folder: reentryProject.ID, Direction: "download",
				QueuedBytes: lowQueued, ActiveBytes: lowActive,
				Detail: "re-entered Low work retained a queued remainder behind active higher Network Priority work; any previously admitted Low Block Transfers remained non-preemptive",
			})
			return true, nil
		}
		return false, nil
	})
}

func (s *networkPriorityStudio) verifyNoDaemonRestartForNetworkPriorityChanges() error {
	for _, index := range []int{studioPeerHub, studioPeerEdit} {
		if s.peers[index].starts != 1 {
			return fmt.Errorf("%s restarted during live Network Priority changes", s.peers[index].name)
		}
	}
	return nil
}

func (s *networkPriorityStudio) waitForHighPrecedence(ctx context.Context) error {
	high := s.projectsByClass(studioClassHigh)
	comparison := append(s.projectsByClass(studioClassNormal), s.projectsByClass(studioClassLow)...)
	if err := s.waitForCompletionEvents(ctx, high); err != nil {
		return err
	}
	for _, project := range comparison {
		if project.Class == studioClassLow {
			if _, ok := s.nonPreemptiveLowProjectIDs[project.ID]; ok {
				continue
			}
		}
		status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
		if err != nil {
			return err
		}
		if status.NeedBytes == 0 && status.GlobalBytes > 0 {
			return fmt.Errorf("comparison project %s completed before all later High projects", project.ID)
		}
	}
	return nil
}

func (s *networkPriorityStudio) verifyCompletionPrecedence() error {
	highFirst, highLast, err := s.completionBounds(s.projectsByClass(studioClassHigh))
	if err != nil {
		return err
	}
	normalFirst, normalLast, err := s.completionBounds(s.projectsByClass(studioClassNormal))
	if err != nil {
		return err
	}
	var comparableLow []studioProject
	for _, project := range s.projectsByClass(studioClassLow) {
		if _, ok := s.nonPreemptiveLowProjectIDs[project.ID]; !ok {
			comparableLow = append(comparableLow, project)
		}
	}
	lowFirst, _, err := s.completionBounds(comparableLow)
	if err != nil {
		return err
	}
	if !highLast.Before(normalFirst) {
		return fmt.Errorf("High completion precedence failed: last High=%s first Normal=%s", highLast, normalFirst)
	}
	if !normalLast.Before(lowFirst) {
		return fmt.Errorf("Normal completion precedence failed: last Normal=%s first Low=%s", normalLast, lowFirst)
	}

	var promoted, demoted []studioProject
	for _, project := range s.projectsByClass(studioClassDynamic) {
		switch {
		case project.FinalNetworkPriority > project.InitialNetworkPriority:
			promoted = append(promoted, project)
		case project.FinalNetworkPriority < project.InitialNetworkPriority && project.ID != s.nonPreemptiveDemotionProjectID:
			demoted = append(demoted, project)
		}
	}
	_, promotedLast, err := s.completionBounds(promoted)
	if err != nil {
		return err
	}
	demotedFirst, _, err := s.completionBounds(demoted)
	if err != nil {
		return err
	}
	if !promotedLast.Before(normalFirst) {
		return fmt.Errorf("promoted Dynamic completion precedence failed: last promoted=%s first Normal=%s", promotedLast, normalFirst)
	}
	if !normalLast.Before(demotedFirst) {
		return fmt.Errorf("queued-demoted Dynamic completion precedence failed: last Normal=%s first demoted=%s", normalLast, demotedFirst)
	}
	s.record(studioTimelineEntry{
		Kind: "assertion",
		Detail: fmt.Sprintf("completion precedence passed: High=%s..%s promotedLast=%s Normal=%s..%s LowFirst=%s demotedFirst=%s",
			highFirst, highLast, promotedLast, normalFirst, normalLast, lowFirst, demotedFirst),
	})
	return nil
}

func (s *networkPriorityStudio) completionBounds(projects []studioProject) (time.Time, time.Time, error) {
	s.completionMu.RLock()
	defer s.completionMu.RUnlock()
	var first, last time.Time
	for _, project := range projects {
		completed := s.completionTime[project.ID]
		if completed.IsZero() {
			return time.Time{}, time.Time{}, fmt.Errorf("completion event missing for %s", project.ID)
		}
		if first.IsZero() || completed.Before(first) {
			first = completed
		}
		if last.IsZero() || completed.After(last) {
			last = completed
		}
	}
	return first, last, nil
}

func (s *networkPriorityStudio) verifyLowResumes(ctx context.Context) error {
	before, err := s.aggregateProgress(ctx, studioPeerEdit, studioClassLow)
	if err != nil {
		return err
	}
	if err := s.waitUntil(ctx, 200*time.Millisecond, func() (bool, error) {
		after, err := s.aggregateProgress(ctx, studioPeerEdit, studioClassLow)
		return after > before, err
	}); err != nil {
		return err
	}
	s.record(studioTimelineEntry{Kind: "assertion", Peer: s.peers[studioPeerEdit].name, Detail: "Low work resumed after High drained"})
	return nil
}

func (s *networkPriorityStudio) measureNormalFairness(ctx context.Context) error {
	dynamic := s.projectsByClass(studioClassDynamic)
	var promoted []studioProject
	for _, project := range dynamic {
		if project.FinalNetworkPriority >= 50 {
			promoted = append(promoted, project)
		}
	}
	if err := s.waitForCompletionEvents(ctx, promoted); err != nil {
		return err
	}
	normals := s.projectsByClass(studioClassNormal)
	if err := s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		for _, project := range normals {
			status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
			if err != nil {
				return false, err
			}
			if status.InSyncBytes == 0 || status.NeedBytes == 0 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	progress := make([]int64, 0, len(normals))
	started := time.Now()
	for _, project := range normals {
		status, err := s.folderStatus(ctx, s.peers[studioPeerEdit], project.ID)
		if err != nil {
			return err
		}
		progress = append(progress, status.InSyncBytes)
	}
	observation := time.Since(started)
	minimum, maximum := slices.Min(progress), slices.Max(progress)
	spread := maximum - minimum
	// One direct connection can have one completed project block not yet
	// reflected by another folder. REST sampling skew adds at most the bytes
	// accepted during the observation interval.
	maxBlockSize := int64(protocol.MaxBlockSize)
	connectionCount := int64(studioPeerCount - 1)
	tolerance := connectionCount*maxBlockSize + int64(math.Ceil(float64(s.profile.ReceiveRateKiB*1024)*observation.Seconds()))
	if spread > tolerance {
		return fmt.Errorf("Normal Equal-Priority Share spread %d exceeds tolerance %d (connections=%d maxBlock=%d observation=%s)", spread, tolerance, connectionCount, maxBlockSize, observation)
	}
	s.reportMu.Lock()
	s.report.NormalFairnessSpread = spread
	s.report.NormalFairnessLimit = tolerance
	s.reportMu.Unlock()
	s.record(studioTimelineEntry{Kind: "measurement", Peer: s.peers[studioPeerEdit].name, Detail: fmt.Sprintf("normal fairness spread=%d tolerance=%d connections=%d maxBlock=%d observation=%s", spread, tolerance, connectionCount, maxBlockSize, observation)})
	return nil
}

func (s *networkPriorityStudio) exerciseFolderPause(ctx context.Context) error {
	peer := s.peers[studioPeerEdit]
	project := s.projectsByClass(studioClassNormal)[studioNormalProjects-1]
	before, err := s.folderStatus(ctx, peer, project.ID)
	if err != nil {
		return err
	}
	if before.NeedBytes == 0 {
		return fmt.Errorf("folder-pause project %s completed before pause", project.ID)
	}
	if err := s.patchFolderPaused(ctx, peer, project.ID, true); err != nil {
		return err
	}
	if err := studioWaitContext(ctx, 750*time.Millisecond); err != nil {
		return err
	}
	after, err := s.folderStatus(ctx, peer, project.ID)
	if err != nil {
		return err
	}
	blockSize := int64(protocol.BlockSize(project.Size))
	if after.InSyncBytes-before.InSyncBytes > blockSize {
		return fmt.Errorf("paused folder %s advanced %d bytes, more than one active non-preemptive block (%d)", project.ID, after.InSyncBytes-before.InSyncBytes, blockSize)
	}
	if err := s.patchFolderPaused(ctx, peer, project.ID, false); err != nil {
		return err
	}
	s.record(studioTimelineEntry{Kind: "lifecycle", Peer: peer.name, Folder: project.ID, Detail: "folder paused and resumed through REST"})
	return nil
}

func (s *networkPriorityStudio) waitForConvergence(ctx context.Context) error {
	return s.waitUntil(ctx, 500*time.Millisecond, func() (bool, error) {
		for _, project := range s.projects {
			for _, peer := range s.peers {
				status, err := s.folderStatus(ctx, peer, project.ID)
				if err != nil {
					return false, err
				}
				if status.NeedBytes != 0 || status.NeedTotalItems != 0 || status.GlobalBytes != project.Size {
					return false, nil
				}
				if status.NetworkPriorityScheduling.Upload.QueuedBytes != 0 || status.NetworkPriorityScheduling.Download.QueuedBytes != 0 ||
					status.NetworkPriorityScheduling.Upload.ActiveBytes != 0 || status.NetworkPriorityScheduling.Download.ActiveBytes != 0 ||
					status.NetworkPriorityScheduling.Upload.OldestSchedulingWaitSeconds != 0 ||
					status.NetworkPriorityScheduling.Download.OldestSchedulingWaitSeconds != 0 {
					return false, nil
				}
			}
		}
		return true, nil
	})
}

func (s *networkPriorityStudio) verifyDirectories() error {
	for _, project := range s.projects {
		dirs := make([]string, 0, studioPeerCount)
		for _, peer := range s.peers {
			dir := s.folderPath(peer, project.ID)
			dirs = append(dirs, dir)
			if partial, err := studioFindPartial(dir); err != nil {
				return err
			} else if partial != "" {
				return fmt.Errorf("unexpected partial file in %s: %s", project.ID, partial)
			}
		}
		if err := compareDirectories(dirs...); err != nil {
			return fmt.Errorf("project %s: %w", project.ID, err)
		}
		sum, err := sha256file(filepath.Join(dirs[0], "media.bin"))
		if err != nil {
			return err
		}
		s.record(studioTimelineEntry{Kind: "checksum", Folder: project.ID, Detail: fmt.Sprintf("sha256=%x result=passed", sum)})
	}
	return nil
}

func (s *networkPriorityStudio) startObserver(ctx context.Context) {
	observerCtx, cancel := context.WithCancel(ctx)
	s.observerCancel = cancel
	go func() {
		defer close(s.observerDone)
		timelineTicker := time.NewTicker(time.Second)
		eventTicker := time.NewTicker(250 * time.Millisecond)
		defer timelineTicker.Stop()
		defer eventTicker.Stop()
		since := 0
		seenCompletion := make(map[string]struct{})
		seenFinishedItems := make(map[string]map[string]struct{})
		for {
			select {
			case <-observerCtx.Done():
				return
			case <-timelineTicker.C:
				if err := s.captureTimeline(observerCtx, "sample"); err != nil {
					s.observationError(err)
				}
			case <-eventTicker.C:
				events, err := s.events(observerCtx, s.peers[studioPeerEdit], since)
				if err != nil {
					s.observationError(err)
					continue
				}
				for _, event := range events {
					if event.ID > since {
						since = event.ID
					}
					data, _ := event.Data.(map[string]any)
					folder, _ := data["folder"].(string)
					item, _ := data["item"].(string)
					eventError := studioEventError(data["error"])
					s.record(studioTimelineEntry{Kind: "event", Peer: s.peers[studioPeerEdit].name, Folder: folder, EventID: event.ID, Detail: event.Type + eventError})
					if event.Type == "ItemFinished" && item != "" && folder != "" && eventError == "" {
						if _, ok := seenCompletion[folder]; ok {
							continue
						}
						items := seenFinishedItems[folder]
						if items == nil {
							items = make(map[string]struct{})
							seenFinishedItems[folder] = items
						}
						items[item] = struct{}{}
						if len(items) < s.expectedProjectFileCount(folder) {
							continue
						}
						seenCompletion[folder] = struct{}{}
						s.completionMu.Lock()
						s.completionTime[folder] = event.Time
						s.completionMu.Unlock()
						s.reportMu.Lock()
						s.report.CompletionOrder = append(s.report.CompletionOrder, folder)
						s.reportMu.Unlock()
					}
				}
			}
		}
	}()
}

func (s *networkPriorityStudio) expectedProjectFileCount(folder string) int {
	for _, project := range s.projects {
		if project.ID == folder {
			count := 1
			if project.ReentryBytes > 0 {
				count++
			}
			if project.ControlBytes > 0 {
				count++
			}
			if project.DemotionBytes > 0 {
				count++
			}
			if project.DirectionBytes > 0 {
				count++
			}
			return count
		}
	}
	return 1
}

func (s *networkPriorityStudio) events(ctx context.Context, peer *studioPeer, since int) ([]rc.Event, error) {
	path := fmt.Sprintf("/rest/events?since=%d&limit=256&timeout=0&events=ItemFinished,DeviceConnected,DeviceDisconnected,FolderPaused,FolderResumed,ConfigSaved", since)
	bs, err := s.requestJSON(ctx, peer, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var result []rc.Event
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *networkPriorityStudio) captureTimeline(ctx context.Context, detail string) error {
	for _, peer := range s.peers {
		connections, err := peer.process.Connections()
		if err != nil {
			return err
		}
		connected := make([]string, 0, len(connections))
		for device, connection := range connections {
			if connection.Connected {
				connected = append(connected, device)
			}
		}
		slices.Sort(connected)
		for _, project := range s.projects {
			status, err := s.folderStatus(ctx, peer, project.ID)
			if err != nil {
				return err
			}
			networkPriority := s.networkPriority(peer.index, project.ID)
			for _, direction := range []struct {
				name  string
				state studioDirectionStatus
				rate  int
				limit int
			}{
				{"upload", status.NetworkPriorityScheduling.Upload, s.profile.SendRateKiB, s.profile.UploadInFlightKiB},
				{"download", status.NetworkPriorityScheduling.Download, s.profile.ReceiveRateKiB, s.profile.DownloadInFlightKiB},
			} {
				s.record(studioTimelineEntry{
					Kind: "status", Peer: peer.name, Folder: project.ID, Direction: direction.name,
					NetworkPriority: networkPriority, GlobalBytes: status.GlobalBytes, InSyncBytes: status.InSyncBytes, NeedBytes: status.NeedBytes,
					QueuedBytes: direction.state.QueuedBytes, ActiveBytes: direction.state.ActiveBytes,
					OldestSchedulingWaitSeconds: direction.state.OldestSchedulingWaitSeconds,
					ConfiguredRateKiB:           direction.rate, InFlightLimitKiB: direction.limit,
					ConnectedDevices: connected, ConfigRevision: peer.configRevision, Detail: detail,
				})
			}
		}
	}
	return nil
}

func (s *networkPriorityStudio) folderStatus(ctx context.Context, peer *studioPeer, folder string) (studioFolderStatus, error) {
	bs, err := s.requestJSON(ctx, peer, http.MethodGet, "/rest/db/status?folder="+url.QueryEscape(folder), nil)
	if err != nil {
		return studioFolderStatus{}, err
	}
	var status studioFolderStatus
	if err := json.Unmarshal(bs, &status); err != nil {
		return status, err
	}
	if !status.NetworkPrioritySchedulingActive {
		return status, fmt.Errorf("Network Priority scheduling inactive on %s/%s", peer.name, folder)
	}
	return status, nil
}

func (s *networkPriorityStudio) aggregateProgress(ctx context.Context, peerIndex int, class studioProjectClass) (int64, error) {
	var result int64
	for _, project := range s.projectsByClass(class) {
		status, err := s.folderStatus(ctx, s.peers[peerIndex], project.ID)
		if err != nil {
			return 0, err
		}
		result += status.InSyncBytes
	}
	return result, nil
}

func (s *networkPriorityStudio) waitForCompletionEvents(ctx context.Context, projects []studioProject) error {
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		s.completionMu.RLock()
		defer s.completionMu.RUnlock()
		for _, project := range projects {
			if s.completionTime[project.ID].IsZero() {
				return false, nil
			}
		}
		return true, nil
	})
}

func (s *networkPriorityStudio) waitForConnection(ctx context.Context, peer *studioPeer, device protocol.DeviceID, connected bool) error {
	return s.waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		connections, err := peer.process.Connections()
		if err != nil {
			return false, err
		}
		state, ok := connections[device.String()]
		if connected {
			return ok && state.Connected, nil
		}
		return !ok || !state.Connected, nil
	})
}

func (s *networkPriorityStudio) waitUntil(ctx context.Context, interval time.Duration, condition func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *networkPriorityStudio) stopPeer(peer *studioPeer) error {
	if peer.process == nil {
		return nil
	}
	select {
	case <-peer.process.Stopped():
		return nil
	default:
	}
	if connections, err := peer.process.Connections(); err == nil {
		in, out := studioConnectionTotals(connections)
		peer.transferredBytes += in + out
	}
	state, err := peer.process.Stop()
	if state != nil {
		usage := studioUsage(state)
		s.reportMu.Lock()
		s.report.PeerUsage[peer.name] = append(s.report.PeerUsage[peer.name], usage)
		s.reportMu.Unlock()
	}
	return err
}

func (s *networkPriorityStudio) close() {
	if s.closed {
		return
	}
	s.closed = true
	if s.observerCancel != nil {
		s.observerCancel()
		<-s.observerDone
	}
	for _, peer := range s.peers {
		if peer != nil {
			if err := s.stopPeer(peer); err != nil {
				s.t.Errorf("stop %s: %v", peer.name, err)
			}
		}
	}
	finished := time.Now().UTC()
	s.reportMu.Lock()
	s.report.FinishedAt = finished
	s.report.DurationSeconds = finished.Sub(s.started).Seconds()
	var transferred int64
	for _, peer := range s.peers {
		if peer != nil {
			transferred += peer.transferredBytes
		}
	}
	// Each transferred byte appears once at the sender and once at the
	// receiver. Divide the cluster counter by two to avoid double counting.
	s.report.TransferredBytes = transferred / 2
	report := s.report
	s.reportMu.Unlock()
	if s.timeline != nil {
		if err := s.timeline.Close(); err != nil {
			s.t.Errorf("close timeline: %v", err)
		}
	}
	if err := studioWriteJSON(filepath.Join(s.runDir, "summary.json"), report); err != nil {
		s.t.Errorf("write studio summary: %v", err)
	}
	userCPU, systemCPU, peakRSS := studioUsageTotals(report.PeerUsage)
	s.t.Logf("Network Priority studio result: seed=%d sourceBytes=%d duration=%s completionOrder=%v fairness=%d/%d throughput=%.0fB/s rateLimit=%s inFlightLimit=%s configuredRate=%d/%dKiB/s configuredInFlight=%d/%dKiB cpu=%.2fu/%.2fs peakRSS=%dKiB restarts=%d errors=%d checksums=%s artifacts=%s",
		report.Seed, report.LogicalSourceBytes, time.Duration(report.DurationSeconds*float64(time.Second)), report.CompletionOrder,
		report.NormalFairnessSpread, report.NormalFairnessLimit, float64(report.TransferredBytes)/max(report.DurationSeconds, 1),
		report.RateLimitCompliance, report.InFlightLimitCompliance, s.profile.SendRateKiB, s.profile.ReceiveRateKiB,
		s.profile.UploadInFlightKiB, s.profile.DownloadInFlightKiB, userCPU, systemCPU, peakRSS,
		report.Restarts, report.Errors, report.ChecksumResult, s.runDir)
	if !s.t.Failed() && !s.profile.RetainArtifacts {
		if err := os.RemoveAll(s.runDir); err != nil {
			s.t.Errorf("remove successful run artifacts: %v", err)
		}
	} else {
		s.t.Logf("retained replay artifacts in %s", s.runDir)
	}
}

func studioUsageTotals(peerUsage map[string][]studioProcessUsage) (float64, float64, int64) {
	var userCPU, systemCPU float64
	var peakRSS int64
	for _, processUsages := range peerUsage {
		for _, usage := range processUsages {
			userCPU += usage.UserCPUSeconds
			systemCPU += usage.SystemCPUSeconds
			peakRSS = max(peakRSS, usage.PeakRSSKiB)
		}
	}
	return userCPU, systemCPU, peakRSS
}

func (s *networkPriorityStudio) must(err error, action string) {
	s.t.Helper()
	if err != nil {
		s.record(studioTimelineEntry{Kind: "failure", Detail: action + ": " + err.Error()})
		s.t.Fatalf("%s: %v", action, err)
	}
}

func (s *networkPriorityStudio) setPhase(phase string) {
	s.phaseMu.Lock()
	s.phase = phase
	s.phaseMu.Unlock()
	s.record(studioTimelineEntry{Kind: "phase", Detail: phase})
}

func (s *networkPriorityStudio) currentPhase() string {
	s.phaseMu.RLock()
	defer s.phaseMu.RUnlock()
	return s.phase
}

func (s *networkPriorityStudio) networkPriority(peer int, folder string) int {
	s.networkPriorityMu.RLock()
	defer s.networkPriorityMu.RUnlock()
	return s.networkPriorities[peer][folder]
}

func (s *networkPriorityStudio) record(entry studioTimelineEntry) {
	entry.At = time.Now().UTC()
	entry.ElapsedMilliseconds = entry.At.Sub(s.started).Milliseconds()
	entry.Seed = s.profile.Seed
	if entry.Phase == "" {
		entry.Phase = s.currentPhase()
	}
	s.timelineMu.Lock()
	defer s.timelineMu.Unlock()
	if s.timeline == nil {
		return
	}
	if err := json.NewEncoder(s.timeline).Encode(entry); err != nil {
		s.observationError(err)
	}
}

func (s *networkPriorityStudio) observationError(err error) {
	s.reportMu.Lock()
	s.report.Errors++
	s.reportMu.Unlock()
}

func (s *networkPriorityStudio) projectsByClass(class studioProjectClass) []studioProject {
	result := make([]studioProject, 0)
	for _, project := range s.projects {
		if project.Class == class {
			result = append(result, project)
		}
	}
	return result
}

func (s *networkPriorityStudio) folderPath(peer *studioPeer, folder string) string {
	return filepath.Join(s.runDir, "projects", peer.name, folder)
}

func studioConnectionTotals(connections map[string]rc.ConnectionStats) (int64, int64) {
	var in, out int64
	for _, connection := range connections {
		in += connection.InBytesTotal
		out += connection.OutBytesTotal
	}
	return in, out
}

func studioStableHash(value string) uint64 {
	sum := sha256.Sum256([]byte(value))
	var result uint64
	for _, b := range sum[:8] {
		result = result<<8 | uint64(b)
	}
	return result
}

func studioEventError(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case string:
		if value == "" {
			return ""
		}
		return " error=" + value
	default:
		return fmt.Sprintf(" error=%v", value)
	}
}

func studioCopyFile(source, destination string) error {
	bs, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, bs, 0o600)
}

func studioFindPartial(root string) (string, error) {
	var found string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := info.Name()
		if strings.HasPrefix(name, ".syncthing.") && strings.HasSuffix(name, ".tmp") {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	return found, err
}

func studioWriteJSON(path string, value any) error {
	bs, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(bs, '\n'), 0o644)
}

func studioWaitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func studioEnvString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func studioEnvInt64(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
