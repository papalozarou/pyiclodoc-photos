# Configuration

This is the practical map of what needs setting in `.env`, what it does, and
which values you can mostly leave alone.

## How the variables are grouped

- `H_` values describe the host.
- `C_` values describe shared in-container values.
- `ALICE_` and `BOB_` values are per-service settings.

## Host variables (`H_`)

Start here first.

- `H_TZ`: timezone for worker behaviour and schedule calculations.
- `H_TGM_CHAT_ID`: only this Telegram chat is accepted for commands.
- `H_DKR_SECRETS`: host path containing source secret files.
- `H_DATA_PATH`: base host path for service data directories.

## Runtime identity mapping

These are usually left as-is unless you need explicit UID or GID mapping.

- `C_UID`: runtime UID inside the container.
- `C_GID`: runtime GID inside the container.
- Compose maps `PUID=${C_UID}` and `PGID=${C_GID}` into each service.
- Entrypoint starts as root only to read secrets, then drops to `PUID:PGID`
  before starting the worker.

*N.B.*

If `/output` already contains files with different ownership, the first-run
safety net can block backup. When that happens, the worker tells you the
expected UID and GID in both Telegram and the container logs.

## Shared container variables (`C_`)

These are shared by all services in the example stack.

- `C_LOG_LEVEL`: `info`, `debug`, or `error`.
- `C_LOG_ROTATE_DAILY`: rotate the worker log when the local date changes.
- `C_LOG_ROTATE_MAX_MIB`: rotate the worker log when it reaches this MiB size.
- `C_LOG_ROTATE_KEEP_DAYS`: keep rotated log archives for this many days.

## Build variables

- `IMG_NAME`: image repository/name used for service image tags, with
  `:alpine-${ALP_VER}` appended in Compose.
- `ALP_VER`: Alpine base image version used during Docker build.
- `MCK_VER`: Microcheck image version used during Docker build.

## Service variables (`ALICE_*`, `BOB_*`)

### Paths and secrets

| Variable name | Possible values | `.env.example` |
| --- | --- | --- |
| `<SVC>_CONFIG_PATH` | Host path | `ALICE_CONFIG_PATH` |
| `<SVC>_OUTPUT_PATH` | Host path | `ALICE_OUTPUT_PATH` |
| `<SVC>_LOGS_PATH` | Host path | `ALICE_LOGS_PATH` |
| `<SVC>_TGM_BOT_TOKEN_FILE` | Container secret-file path | `ALICE_TGM_BOT_TOKEN_FILE` |
| `<SVC>_ICLOUD_EMAIL_FILE` | Container secret-file path | `ALICE_ICLOUD_EMAIL_FILE` |
| `<SVC>_ICLOUD_PASSWORD_FILE` | Container secret-file path | `ALICE_ICLOUD_PASSWORD_FILE` |

### Scheduling and runtime behaviour

| Variable name | Possible values | `.env.example` |
| --- | --- | --- |
| `<SVC>_RUN_ONCE` | `true` or `false` | `ALICE_RUN_ONCE` |
| `<SVC>_SCHEDULE_MODE` | `interval`, `daily`, `weekly`, `twice_weekly`, or `monthly` | `ALICE_SCHEDULE_MODE` |
| `<SVC>_SCHEDULE_BACKUP_TIME` | `HH:MM` 24-hour local time | `ALICE_SCHEDULE_BACKUP_TIME` |
| `<SVC>_SCHEDULE_WEEKDAYS` | Comma-separated weekday names | `ALICE_SCHEDULE_WEEKDAYS` |
| `<SVC>_SCHEDULE_MONTHLY_WEEK` | `first`, `second`, `third`, `fourth`, or `last` | `ALICE_SCHEDULE_MONTHLY_WEEK` |
| `<SVC>_SCHEDULE_INTERVAL_MINUTES` | Positive integer minutes | `ALICE_SCHEDULE_INTERVAL_MINUTES` |
| `<SVC>_REAUTH_INTERVAL_DAYS` | Positive integer days | `ALICE_REAUTH_INTERVAL_DAYS` |
| `<SVC>_RESTART_POLICY` | Compose restart policy, for example `unless-stopped` or `no` | `ALICE_RESTART_POLICY` |

*N.B.*

Use one weekday in `<SVC>_SCHEDULE_WEEKDAYS` for `weekly`, two distinct
weekdays for `twice_weekly`, and one weekday plus
`<SVC>_SCHEDULE_MONTHLY_WEEK` for `monthly`.

When `<SVC>_RUN_ONCE=true`, set `<SVC>_RESTART_POLICY=no`. If you leave a
restart policy such as `unless-stopped` in place, the one-shot container exits
and then immediately starts again.

If a numeric schedule value is set to an invalid value such as `abc`, the
worker treats that as a startup configuration error. It does not silently
accept the value and continue with the default.

### Transfer and backup behaviour

| Variable name | Possible values | `.env.example` |
| --- | --- | --- |
| `<SVC>_SYNC_DOWNLOAD_WORKERS` | `auto` or an integer from `1` to `16` | `ALICE_SYNC_DOWNLOAD_WORKERS` |
| `<SVC>_SYNC_DOWNLOAD_CHUNK_MIB` | Positive integer MiB value | `ALICE_SYNC_DOWNLOAD_CHUNK_MIB` |
| `<SVC>_BACKUP_DISCOVERY_MODE` | `full` or `until_found` | `ALICE_BACKUP_DISCOVERY_MODE` |
| `<SVC>_BACKUP_UNTIL_FOUND_COUNT` | Positive consecutive-match count | `ALICE_BACKUP_UNTIL_FOUND_COUNT` |
| `<SVC>_BACKUP_DELETE_REMOVED` | `true` or `false` | `ALICE_BACKUP_DELETE_REMOVED` |
| `<SVC>_BACKUP_ALBUMS_ENABLED` | `true` or `false` | `ALICE_BACKUP_ALBUMS_ENABLED` |
| `<SVC>_BACKUP_ALBUM_LINKS_MODE` | `hardlink` or `copy` | `ALICE_BACKUP_ALBUM_LINKS_MODE` |
| `<SVC>_BACKUP_INCLUDE_SHARED_ALBUMS` | `true` or `false` | `ALICE_BACKUP_INCLUDE_SHARED_ALBUMS` |
| `<SVC>_BACKUP_INCLUDE_FAVOURITES` | `true` or `false` | `ALICE_BACKUP_INCLUDE_FAVOURITES` |

*N.B.*

`full` is the default and safest discovery mode. `until_found` is an explicit
performance mode based on `pyicloud` documenting that `All Photos` is ordered
with the most recently added assets first.

`until_found` stops scanning once it has seen the configured number of
consecutive unchanged canonical entries. It can reduce remote listing work on
incremental runs, but only for canonical-library discovery. `full` remains the
safer default when you want the most conservative behaviour.

`until_found` cannot be combined with `<SVC>_BACKUP_DELETE_REMOVED=true` or
`<SVC>_BACKUP_ALBUMS_ENABLED=true`. Both of those features require a full
authoritative remote snapshot.

With those restrictions in place, `until_found` is best thought of as a
library-discovery optimisation rather than a general reduced-work mode.

`hardlink` is the opinionated default. It avoids duplicate data where the host
filesystem and bind mount allow hard links. `copy` is strict copy-only mode and
does not attempt hard links first.

If a numeric transfer value is set to an invalid value such as `abc`, the
worker treats that as a startup configuration error. It does not silently
accept the value and continue with the default.

When `<SVC>_BACKUP_ALBUMS_ENABLED=false`, the worker stops creating, refreshing,
and deleting files under `albums/`. Existing album output is left untouched.

## Logging

- `C_LOG_LEVEL=info`: stage-level worker messages.
- `C_LOG_LEVEL=debug`: stage-level messages plus transfer planning detail,
  album-view refreshes, delete detail, and more verbose sync diagnostics.
- `C_LOG_LEVEL=error`: only error lines.

## Default container paths

- `/config`: auth, session, keyring, manifest, and runtime metadata.
- `/output`: downloaded iCloud Photos files and derived album views.
- `/logs`: worker logs and healthcheck heartbeat.

## `/config` layout

Each worker mounts `/config` from a different host location:

- `icloud_photos_alice` -> `${ALICE_CONFIG_PATH}`
- `icloud_photos_bob` -> `${BOB_CONFIG_PATH}`

Runtime layout:

```text
/config
├── pyiclodoc-photos-auth_state.json
├── pyiclodoc-photos.lock
├── pyiclodoc-photos-manifest.json
├── pyiclodoc-photos-safety_net_done.flag
├── pyiclodoc-photos-safety_net_blocked.flag
├── cookies/
├── session/
├── icloudpd/
│   ├── cookies -> /config/cookies
│   └── session -> /config/session
└── keyring/
    └── keyring_pass.cfg
```

*N.B.*

- `pyiclodoc-photos-safety_net_done.flag` is created when first-run safety
  checks pass.
- `pyiclodoc-photos.lock` is held by the active worker process to prevent more
  than one writer using the same config and output state at the same time.
- `pyiclodoc-photos-safety_net_blocked.flag` is created when first-run safety
  checks block backup.
- corrupt JSON state is quarantined beside the original file with a
  `.corrupt` suffix so the worker can recover without deleting the bad state.
- `icloudpd/cookies` and `icloudpd/session` are compatibility symlinks used so
  auth state can be reused safely with related tooling layouts.

## `/output` layout

Canonical library layout:

```text
/output/library/YYYY/MM/DD/filename.ext
```

Derived album layout:

```text
/output/albums/Album Name/filename.ext
```

Example:

```text
/output/library/2026/03/14/IMG_0001.HEIC
/output/library/2026/03/14/IMG_0001--0d4e6f8a1b2c.HEIC
/output/albums/Trips/IMG_0001.HEIC
```

*N.B.*

When two different assets would otherwise collide on the same canonical dated
path, the worker adds a deterministic suffix before the file extension. That
keeps one canonical file per asset without abandoning the readable
`year/month/day` layout.

## Log files

- `/logs/pyiclodoc-photos-worker.log`: active worker log.
- `/logs/pyiclodoc-photos-heartbeat.txt`: healthcheck heartbeat.
- `/logs/pyiclodoc-photos-worker.*.log.gz`: rotated compressed log archives.

*N.B.*

Logger settings are cached inside the worker process and rotation checks are
throttled per log file. That keeps debug-heavy runs lower in overhead terms
without changing the visible logging contract.

For scheduling compatibility and mode-specific behaviour, see
[SCHEDULING.md](SCHEDULING.md).
