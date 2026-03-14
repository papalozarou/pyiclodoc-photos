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

N.B.

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

- `<SVC>_CONFIG_PATH`: host path mounted to `/config`.
- `<SVC>_OUTPUT_PATH`: host path mounted to `/output`.
- `<SVC>_LOGS_PATH`: host path mounted to `/logs`.
- `<SVC>_TGM_BOT_TOKEN_FILE`: Telegram bot token file path.
- `<SVC>_ICLOUD_EMAIL_FILE`: iCloud email file path.
- `<SVC>_ICLOUD_PASSWORD_FILE`: iCloud password file path.

### Scheduling and runtime behaviour

- `<SVC>_RUN_ONCE`: run one backup pass and exit (`true` or `false`).
- `<SVC>_SCHEDULE_MODE`: `interval`, `daily`, `weekly`, `twice_weekly`, or
  `monthly`.
- `<SVC>_SCHEDULE_BACKUP_TIME`: local run time in `HH:MM` 24-hour format.
- `<SVC>_SCHEDULE_WEEKDAYS`: comma-separated weekday names. Use one day for
  `weekly`, two distinct days for `twice_weekly`, and one day for `monthly`.
- `<SVC>_SCHEDULE_MONTHLY_WEEK`: one of `first`, `second`, `third`, `fourth`,
  `last`.
- `<SVC>_SCHEDULE_INTERVAL_MINUTES`: interval run spacing in minutes.
- `<SVC>_REAUTH_INTERVAL_DAYS`: reauthentication window in days.
- `<SVC>_RESTART_POLICY`: Compose restart policy for the service, for example
  `unless-stopped` or `no`.

N.B.

When `<SVC>_RUN_ONCE=true`, set `<SVC>_RESTART_POLICY=no`. If you leave a
restart policy such as `unless-stopped` in place, the one-shot container exits
and then immediately starts again.

### Transfer and backup behaviour

- `<SVC>_SYNC_TRAVERSAL_WORKERS`: retained for parity with the drive project.
  Current photo listing does not depend on deep directory traversal in the same
  way as the Drive worker.
- `<SVC>_SYNC_DOWNLOAD_WORKERS`: `auto` or an integer from `1` to `16`.
- `<SVC>_SYNC_DOWNLOAD_CHUNK_MIB`: streamed download chunk size in MiB.
- `<SVC>_BACKUP_DELETE_REMOVED`: remove local files and empty directories when
  they no longer exist remotely.
- `<SVC>_BACKUP_LIBRARY_ENABLED`: build the canonical `library/` tree.
- `<SVC>_BACKUP_ALBUMS_ENABLED`: build the derived `albums/` tree.
- `<SVC>_BACKUP_ALBUM_LINKS_MODE`: `hardlink` or `copy`.
- `<SVC>_BACKUP_INCLUDE_SHARED_ALBUMS`: include shared albums in `albums/`.
- `<SVC>_BACKUP_INCLUDE_FAVOURITES`: include the favourites album in
  `albums/`.

N.B.

`hardlink` is the opinionated default. It avoids duplicate data where the host
filesystem and bind mount allow hard links. If hard links are not possible, the
worker falls back to copying files into the album view.

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

N.B.

- `pyiclodoc-photos-safety_net_done.flag` is created when first-run safety
  checks pass.
- `pyiclodoc-photos-safety_net_blocked.flag` is created when first-run safety
  checks block backup.
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
/output/albums/Trips/IMG_0001.HEIC
```

## Log files

- `/logs/pyiclodoc-photos-worker.log`: active worker log.
- `/logs/pyiclodoc-photos-heartbeat.txt`: healthcheck heartbeat.
- `/logs/pyiclodoc-photos-worker.*.log.gz`: rotated compressed log archives.

For scheduling compatibility and mode-specific behaviour, see
[SCHEDULING.md](SCHEDULING.md).
