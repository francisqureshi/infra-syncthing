# Operating Network Priority scheduling

Network Priority is a device-local integer from -100 through 100, defaulting
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

`networkPriority` is part of the existing folder configuration returned by
`GET /rest/config/folders/:id` and accepted by `PUT` or `PATCH` on that path:

```json
{
  "networkPriority": 50
}
```

The inclusive bounds are -100 and 100. Invalid values reject the configuration
update without changing the current value. No feature flag or configuration
migration is required. An existing `networkPriority` entry in the generic
`featureFlags` list is harmless but no longer controls scheduling.

## In-Flight Limits are not rate limits

`maxConcurrentIncomingRequestKiB` is the node-wide upload In-Flight Limit for
active response bytes serving incoming block requests.
`maxConcurrentOutgoingRequestKiB` is the independent node-wide download
In-Flight Limit for active outgoing block requests. For both settings, zero
selects the 256 MiB default, a negative value disables the cap, and a small
positive value is raised to the safe protocol minimum.

An In-Flight Limit caps concurrent active bytes; it does not cap bytes per
second. `maxSendKbps`, `maxRecvKbps`, and per-device rate limits remain
authoritative. Network Priority does not bypass those limiters, reserve their
tokens, or guarantee bandwidth.

## Observe current scheduler state

`GET /rest/db/status?folder=:id` reports whether scheduling is active and the
current state for each direction:

```json
{
  "networkPrioritySchedulingActive": true,
  "networkPriorityScheduling": {
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

- `syncthing_model_network_priority_scheduler_active`
- `syncthing_model_network_priority_queued_bytes`
- `syncthing_model_network_priority_active_bytes`
- `syncthing_model_network_priority_oldest_scheduling_wait_seconds`

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
  -run '^TestNetworkPriorityStudioWorkload$' -count=10
```

Failures retain the generated homes, configurations, daemon logs, project
data, JSON Lines timeline, and JSON summary under
`test/logs/network-priority`. Set `STUDIO_ARTIFACT_DIR` to use another artifact
root.

The opt-in soak uses the same scenario engine behind the existing
`integration,benchmark` tags:

```sh
STUDIO_SOAK_TOTAL_BYTES=12GiB \
go test -v -timeout 12h -tags 'integration,benchmark' ./test \
  -run '^TestBenchmarkNetworkPriorityStudioSoak$'
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
