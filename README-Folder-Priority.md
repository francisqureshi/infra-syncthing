# Operating Folder Priority scheduling

Folder Priority is a device-local integer from -100 through 100, defaulting
to zero. Higher values strictly precede lower values for the next compatible
Folder-owned work. It covers upload and download Block Transfers,
Folder-Scoped Metadata, pulls, priority-enrolled prerequisite scan admission,
and Source Hash Work from explicit, scheduled, watcher, and prerequisite
scans. Active transfers, protocol frames, directory traversals, and Hashing
Quanta finish without preemption. The scheduler also serves installations
where every Folder keeps the default priority of zero, applying
Equal-Priority Share universally.

Ordinary explicit and maintenance scan admission remains arrival-ordered;
only the already-enrolled prerequisite scan policy uses Folder Priority.
Receive-side verification, reuse hashing, version cleanup, and unrelated
maintenance hashing do not consume Hash Capacity. Folder Priority is not sent
through BEP and adds no field, scheduling hint, capability negotiation, or
other wire-protocol change.

The [scheduler prototype](https://github.com/francisqureshi/infra-syncthing/tree/6683a077eaec7b6c0cbfc1c6e3a50ec497b06c97)
remains linked design evidence only. Its HTML and reducer are not production
code and are not shipped by Syncthing.

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

## Configure Hash Capacity and per-Folder ceilings

`hashCapacity` is part of the options configuration returned by
`GET /rest/config/options` and accepted by `PUT` or `PATCH` on that path:

```json
{
  "hashCapacity": 4
}
```

Zero selects automatic node-wide Hash Capacity, which is the current positive
`GOMAXPROCS` value. A positive value sets the live node-wide pool explicitly.
Increasing it admits compatible queued Source Hash Work immediately. Reducing
it lets active Hashing Quanta finish and withholds replacements until active
usage reaches the new limit. A negative value rejects the complete
configuration update without changing the active capacity.

The existing per-Folder `hashers` setting is a ceiling on that Folder's
concurrent Hashing Quanta. Zero inherits the whole node-wide pool; a positive
value limits the Folder to the smaller of that value and available Hash
Capacity. A ceiling does not reserve capacity, so other Folders can use every
slot it leaves idle. Unlike node-wide Hash Capacity, changing a positive
per-Folder `hashers` value retains the existing Folder restart lifecycle.

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
current Block Transfer and Source Hash Work state:

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
    },
    "sourceHashWork": {
      "queued": 3,
      "active": 2,
      "oldestSchedulingWaitSeconds": 8.25,
      "hashCapacity": 4,
      "retainedHandles": 5,
      "retainedHandleBudget": 7
    }
  }
}
```

`queuedBytes` and `activeBytes` are current totals. Scheduling Wait is the age
of the oldest work that is currently queued, not historical admission latency.
It keeps increasing while work starves and returns to zero when the queue is
empty. The REST response exposes stable behavior only; it does not expose
internal queue records.

`sourceHashWork.queued` includes work waiting in the coordinator, work under
bounded enrollment backpressure, and changed-file metadata still waiting in a
buffered discovery spool. `active` counts current Hashing Quanta. Hash Capacity
and retained-handle usage and budget are node-wide current values repeated in
each Folder status so an operator can explain local work in the context of the
shared resource. Successful drain, cancellation, Folder pause/removal, handle
eviction, and discovery-spool cleanup remove their work and handle usage from
the next status response.

Prometheus exposes equivalent gauges. Per-work-class metrics use only current
configured `folder` values and the bounded `work_class` values `upload`,
`download`, and `source_hash`; no file name or queue identifier is a label:

- `syncthing_model_folder_priority_scheduler_active`
- `syncthing_model_folder_priority_queued_bytes`
- `syncthing_model_folder_priority_active_bytes`
- `syncthing_model_folder_priority_oldest_scheduling_wait_seconds`
- `syncthing_model_folder_priority_source_hash_work_queued`
- `syncthing_model_folder_priority_source_hash_work_active`
- `syncthing_model_folder_priority_hash_capacity`
- `syncthing_model_folder_priority_retained_handles`
- `syncthing_model_folder_priority_retained_handle_budget`

## Reproduce the Source Hash Work throughput gate

`BenchmarkSourceHashThroughput` creates 16 deterministic files of
`16 MiB + 17 bytes` on one basic filesystem, warms that same data once, and
uses 128 KiB blocks for both paths. `WholeFileBaseline` reproduces the previous
whole-file worker behavior. `Scheduled` exercises the production Source Hash
Work coordinator. Both use four effective workers; the scheduled path uses
Hash Capacity four and a per-Folder ceiling of four.

Run ten samples of each path on the same host:

```sh
GOMAXPROCS=4 go test ./lib/scanner -run '^$' \
  -bench '^BenchmarkSourceHashThroughput$' -benchmem \
  -benchtime=3s -count=10 | tee /tmp/source-hash-throughput.txt
```

The Go benchmark header records the operating system, architecture, package,
and CPU. Each sample reports bytes per second and allocations, plus custom
`gomaxprocs`, `hash-capacity`, `folder-ceiling`, and
`peak-retained-handles` values. Record `go version` and relevant host details
with the raw output.

Extract and sort each path's MB/s samples, calculate the median (the mean of
the two middle values for an even run count), and divide the scheduled median
by the baseline median. The gate passes only when that ratio is at least
`0.95`. This comparison belongs in benchmark analysis, not a wall-clock
functional test.

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
