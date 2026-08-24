# Operating Folder Priority scheduling

Folder Priority is a device-local integer from -100 through 100, defaulting
to zero. Higher values strictly precede lower values for the next runnable
Block Transfer. Active transfers and active protocol frames finish without
preemption. The scheduler also serves installations where every folder keeps
the default priority of zero, applying Equal-Priority Share universally.

Strict priority intentionally permits starvation. Continuously runnable work
at a higher priority can keep lower-priority work queued indefinitely. Use the
current Scheduling Wait status and metrics below to detect that condition, then
change the local priorities if the policy is too aggressive.

## Configure each participating device

The setting is neither synchronized nor sent to peers. A controller that needs
coordinated end-to-end behavior must update every relevant Syncthing device
independently through its REST API. A peer using a different local value is
valid because each device controls its own resources.

`folderPriority` is part of the existing folder configuration returned by
`GET /rest/config/folders/:id` and accepted by `PUT` or `PATCH` on that path:

```json
{
  "folderPriority": 50
}
```

The inclusive bounds are -100 and 100. Invalid values reject the configuration
update without changing the current value. Scheduling is universal and is not
gated by a `featureFlags` entry.

## Source Hash Work resource bounds

Source Hash Work uses one node-wide active window. At any time, the coordinator
owns no more than effective Hash Capacity plus a fixed lookahead of three
enrolled files. Each enrolled file contributes at most one next-Hashing-Quantum
descriptor; complete block arrays remain private to the file owner and are
never materialized in scheduler state.

The same `Hash Capacity + 3` bound applies to active plus retained source
handles across all Folders. Files beyond the active window remain under scan
backpressure without an open source handle. A newly runnable higher-priority
file may displace lower-priority retained work when the window is full. That
closes the displaced handle, discards its incomplete block list, preserves its
actual-byte Equal-Priority Share charge, and later starts a fresh pass from
block zero after reopening and restating the source. Hashes from before the
close are never joined to the reopened handle.

Live Hash Capacity shrink does not interrupt active Hashing Quanta. Queued
descriptors and retained handles are released immediately where possible, then
usage converges to the smaller `Hash Capacity + 3` bound as active quanta reach
their block boundaries. Folder pause/removal, mutation, read errors, and
successful completion close every handle that no longer has an owner.

## In-Flight Limits are not rate limits

`maxConcurrentIncomingRequestKiB` is the node-wide upload In-Flight Limit for
active response bytes serving incoming block requests.
`maxConcurrentOutgoingRequestKiB` is the independent node-wide download
In-Flight Limit for active outgoing block requests. For both settings, zero
selects the 256 MiB default, a negative value disables the cap, and a small
positive value is raised to the safe protocol minimum.

An In-Flight Limit caps concurrent active bytes; it does not cap bytes per
second. `maxSendKbps`, `maxRecvKbps`, and per-device rate limits remain
authoritative. Folder Priority does not bypass those limiters, reserve their
tokens, or guarantee bandwidth.

## Observe current scheduler state

`GET /rest/db/status?folder=:id` reports whether scheduling is active and the
current state for each direction:

```json
{
  "folderPrioritySchedulingActive": true,
  "folderPriorityScheduling": {
    "upload": {
      "queuedBytes": 1048576,
      "activeBytes": 4194304,
      "oldestSchedulingWaitSeconds": 12.5
    },
    "download": {
      "queuedBytes": 0,
      "activeBytes": 0,
      "oldestSchedulingWaitSeconds": 0
    }
  }
}
```

`queuedBytes` and `activeBytes` are current totals. Scheduling Wait is the age
of the oldest work that is currently queued, not historical admission latency.
It keeps increasing while work starves and returns to zero when the queue is
empty. The REST response exposes stable behavior only; it does not expose
internal queue records.

Prometheus exposes equivalent gauges with only the bounded `folder` and
`direction` labels:

- `syncthing_model_folder_priority_scheduler_active`
- `syncthing_model_folder_priority_queued_bytes`
- `syncthing_model_folder_priority_active_bytes`
- `syncthing_model_folder_priority_oldest_scheduling_wait_seconds`

## Validate a three-peer studio workload

The bounded integration profile starts three real daemons with the repository
fixture identities and direct TCP connections. It creates exactly 30 folders:
five High/Critical, fifteen Normal, five Low ingests, and five Dynamic
projects. The default seed is `363`; set `STUDIO_SEED` to replay an alternate
deterministic data set. Build the integration binary before running the focused
test:

```sh
go run build.go install syncthing
go test -v -timeout 60m -tags integration ./test \
  -run '^TestFolderPriorityStudioWorkload$' -count=10
```

Failures retain the generated homes, configurations, daemon logs, project
data, JSON Lines timeline, and JSON summary under
`test/logs/folder-priority`. Set `STUDIO_ARTIFACT_DIR` to use another artifact
root.

The opt-in soak uses the same scenario engine behind the existing
`integration,benchmark` tags:

```sh
STUDIO_SOAK_TOTAL_BYTES=12GiB \
go test -v -timeout 12h -tags 'integration,benchmark' ./test \
  -run '^TestBenchmarkFolderPriorityStudioSoak$'
```

Soak controls are:

- `STUDIO_SOAK_TOTAL_BYTES`: total real source bytes; accepts bytes or
  `KiB`/`MiB`/`GiB`/`TiB` (and decimal `KB`/`MB`/`GB`/`TB`). Literal
  multi-terabyte runs are supported.
- `STUDIO_SOAK_DISTRIBUTION`: `equal` or `ramp` project sizes.
- `STUDIO_SOAK_DURATION`: positive Go duration used as the scenario timeout.
- `STUDIO_SOAK_SEND_KIB` and `STUDIO_SOAK_RECEIVE_KIB`: node rate limits.
- `STUDIO_SOAK_UPLOAD_INFLIGHT_KIB` and
  `STUDIO_SOAK_DOWNLOAD_INFLIGHT_KIB`: directional In-Flight Limits.
- `STUDIO_SOAK_DISK_MULTIPLIER`: disk safety multiplier, minimum and default
  `4`. The soak refuses to create data unless free space is at least this
  multiple of the requested logical source bytes.

Soak artifacts are always retained. The end-of-run summary records completion
order, Equal-Priority Share tolerance, work-conservation throughput, rate and
In-Flight observations, reconnect/error counts, total transferred bytes,
per-process CPU and peak RSS, and checksum results.
