# Configuration

## Quick start

Set these first:

- `H_DATA_PATH`
- `H_DKR_SECRETS`
- `H_TGM_CHAT_ID`
- `ALICE_ICLOUD_EMAIL_FILE`
- `ALICE_ICLOUD_PASSWORD_FILE`

Then start the stack with `docker compose up -d --build`.

## Shared variables

- `ALP_VER`: Alpine base version.
- `MCK_VER`: `microcheck` image version.
- `H_TZ`: container timezone.
- `C_UID`: runtime UID inside the container.
- `C_GID`: runtime GID inside the container.
- `C_LOG_LEVEL`: `info`, `debug`, or `error`.
- `C_LOG_ROTATE_DAILY`: rotate the worker log when the local date changes.
- `C_LOG_ROTATE_MAX_MIB`: rotate the worker log when it reaches this size.
- `C_LOG_ROTATE_KEEP_DAYS`: keep rotated log archives for this many days.

## Per-service variables

- `<SVC>_CONFIG_PATH`: persistent config and auth state path.
- `<SVC>_OUTPUT_PATH`: backup output path.
- `<SVC>_LOGS_PATH`: log path.
- `<SVC>_ICLOUD_EMAIL_FILE`: secret file containing the Apple ID email.
- `<SVC>_ICLOUD_PASSWORD_FILE`: secret file containing the Apple ID password.
- `<SVC>_REAUTH_INTERVAL_DAYS`: MFA reauth interval in days.
- `<SVC>_RUN_ONCE`: `true` or `false`.
- `<SVC>_SCHEDULE_MODE`: `interval`, `daily`, `weekly`, `twice_weekly`, or `monthly`.
- `<SVC>_SCHEDULE_BACKUP_TIME`: `HH:MM`, 24-hour clock.
- `<SVC>_SCHEDULE_WEEKDAYS`: weekday names such as `monday` or `monday,thursday`.
- `<SVC>_SCHEDULE_MONTHLY_WEEK`: `first`, `second`, `third`, `fourth`, or `last`.
- `<SVC>_SCHEDULE_INTERVAL_MINUTES`: interval schedule frequency.
- `<SVC>_SYNC_TRAVERSAL_WORKERS`: retained for parity with `pyiclodoc-drive`.
- `<SVC>_SYNC_DOWNLOAD_WORKERS`: `auto` or an integer.
- `<SVC>_SYNC_DOWNLOAD_CHUNK_MIB`: download chunk size in MiB.
- `<SVC>_BACKUP_DELETE_REMOVED`: remove local files that are no longer present remotely.
- `<SVC>_BACKUP_LIBRARY_ENABLED`: build the canonical `library/` tree.
- `<SVC>_BACKUP_ALBUMS_ENABLED`: build the derived `albums/` tree.
- `<SVC>_BACKUP_ALBUM_LINKS_MODE`: `hardlink` or `copy`.
- `<SVC>_BACKUP_INCLUDE_SHARED_ALBUMS`: include shared albums in `albums/`.
- `<SVC>_BACKUP_INCLUDE_FAVOURITES`: include the favourites album in `albums/`.
- `<SVC>_RESTART_POLICY`: Docker restart policy.
- `<SVC>_TGM_BOT_TOKEN_FILE`: Telegram bot token secret file.

## Output layout

Canonical files are stored here:

```text
/output/library/YYYY/MM/DD/filename.ext
```

Album views are stored here:

```text
/output/albums/Album Name/filename.ext
```

N.B.

Album views are derived from the canonical files. They help with browsing and manual file-based workflows, but they do not preserve Apple Photos album membership on reimport by themselves.

## Config layout

```text
/config/
├── cookies/
├── session/
├── icloudpd/
│   ├── cookies -> ../cookies
│   └── session -> ../session
├── keyring/
├── pyiclodoc-photos-auth_state.json
├── pyiclodoc-photos-manifest.json
├── pyiclodoc-photos-safety_net_blocked.flag
└── pyiclodoc-photos-safety_net_done.flag
```

## Log files

- `/logs/pyiclodoc-photos-worker.log`: active worker log.
- `/logs/pyiclodoc-photos-heartbeat.txt`: healthcheck heartbeat.
