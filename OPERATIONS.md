# Operations

## Runtime notes

- Compose `init: true` is required by the provided service definitions.
- Health checks use `parallel` from the microcheck toolbox image.
- A background heartbeat updater refreshes `/logs/pyiclodoc-photos-heartbeat.txt`
  every 30 seconds in both recurring and one-shot execution paths.
- Telegram commands are ignored unless they come from `H_TGM_CHAT_ID`.
- Entrypoint starts as root only to read Docker secret files, then drops to
  `PUID:PGID` before launching the worker process.
- Services keep `cap_drop: ALL` and add only `SETUID` and `SETGID` so
  privilege drop works.
- Set `LOG_LEVEL=debug` in Compose `default-env` for verbose runtime
  diagnostics.
- At `LOG_LEVEL=info`, worker logs still include stage boundary markers so run
  progress remains visible.
- Error lines are coloured red in container stdout; file logs remain plain
  text.
- Worker logs rotate daily and at size threshold, are compressed to
  `pyiclodoc-photos-worker.*.log.gz`, and are pruned by configured retention
  days.
- Logger settings are cached inside the worker process and rotation checks are
  throttled per log file, so verbose runs do not re-parse the logging
  environment and re-stat the same log on every emitted line.

## Privilege model

- Worker runtime is non-root.
- Root is used at startup only for secret file access under `/run/secrets`.
- If your Docker runtime blocks group switching (`setgroups`), startup can fail
  during the privilege drop step.

## Scheduling

For full scheduling behaviour, option compatibility, manual command effects,
and validation rules, see [SCHEDULING.md](SCHEDULING.md).

## One-shot mode

- Enable with `<SVC>_RUN_ONCE=true`.
- Set `<SVC>_RESTART_POLICY=no` to avoid automatic restarts.
- This pairing is required: one-shot with `unless-stopped` or similar will
  restart the container after exit and loop.
- Worker waits for Telegram `auth` or `reauth` commands when MFA or reauth is
  pending, then runs one backup attempt and exits.
- While one-shot is running, heartbeat updates continue so container health
  status reflects liveness during auth wait and backup execution.
- If auth does not complete within the one-shot wait window, worker exits
  non-zero.
- Exit is non-zero when auth is incomplete, reauth is pending, or first-run
  safety net blocks backup.

## Transfer performance

- Incremental sync uses `pyiclodoc-photos-manifest.json` and skips unchanged
  files.
- Canonical files are written under `library/<year>/<month>/<day>/`.
- When two assets would otherwise collide on the same canonical dated path,
  the worker adds a deterministic suffix before the file extension.
- Album views are created afterwards under `albums/<album>/`.
- Album views prefer hard links and fall back to file copies where linking is
  not possible.
- Download workers run in parallel automatically based on host CPU.
- Worker count is internally bounded and can be overridden with
  `SYNC_DOWNLOAD_WORKERS`.
- Download stream chunk size can be tuned with `SYNC_DOWNLOAD_CHUNK_MIB`.
- Successful downloads preserve remote modified timestamps on local files.
- Optional mirror-delete behaviour can be enabled with
  `BACKUP_DELETE_REMOVED=true`, which prunes local files and empty directories
  under `/output` when they no longer exist in iCloud.

## Safety-net behaviour

On first run only, each worker samples existing files in `/output` and checks
UID and GID for consistency against the container runtime user.

If mismatches are found, backup is blocked. Details are written to worker logs
and sent via Telegram. This is intended to avoid destructive rewrites over
existing backup trees with mixed ownership.

## State recovery

- If `pyiclodoc-photos-auth_state.json` or `pyiclodoc-photos-manifest.json`
  contains malformed JSON, the worker quarantines the bad file with a
  `.corrupt` suffix and carries on with empty in-memory state.
- This is intended to turn truncated or manually damaged state files into a
  recoverable operator problem rather than a startup crash.

## Reimport note

The canonical `library/` tree is the safest file-based export shape for later
reimport because it preserves one obvious source copy per asset. The `albums/`
tree is useful for browsing and external tooling, but file-based reimport alone
will not recreate album membership in Photos.app.
