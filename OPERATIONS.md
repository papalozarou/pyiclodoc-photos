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
- Debug logs include control-flow decisions such as why the worker is waiting,
  skipping, retrying, or polling Telegram. They do not include passwords,
  Telegram message text, bot tokens, or Apple two-factor codes.
- At `LOG_LEVEL=info`, worker logs still include stage boundary markers so run
  progress remains visible.
- Error lines are coloured red in container stdout; file logs remain plain
  text.
- Worker logs rotate daily and at size threshold, are compressed to
  `pyiclodoc-photos-worker.*.log.gz`, and are pruned by configured retention
  days.
- Worker startup acquires a shared lock file under `/config` so only one worker
  process can use the same runtime state at a time.
- If `TZ` is invalid, worker startup logs that UTC fallback is in effect for
  schedule calculations and timestamps.
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

- Persistent mode logs the initial next scheduled run time after startup.
- When a scheduled or manual run becomes due, persistent mode logs the
  recalculated next scheduled run time before the next wait cycle.
- These schedule lines are emitted in the configured local timezone, or UTC if
  `TZ` is invalid.
- A Telegram manual backup request queues one backup attempt. If that attempt
  is skipped because auth is incomplete or reauth is pending, the request is
  consumed rather than retried every five seconds.

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
- `BACKUP_DISCOVERY_MODE=full` scans the full remote photo listing each run.
- `BACKUP_DISCOVERY_MODE=until_found` stops after
  `BACKUP_UNTIL_FOUND_COUNT` consecutive unchanged entries in `All Photos`.
- `full` remains the safer default. `until_found` is an explicit performance
  optimisation that relies on `pyicloud`'s documented `All Photos` ordering.
- `until_found` is only safe for canonical-library discovery. Delete
  reconciliation and album management require a full remote snapshot, so the
  worker does not support `until_found` with those features enabled.
- in practice, that means `until_found` reduces canonical library discovery
  work only. It is not a general shortcut for every backup phase.
- Canonical files are written under `library/<year>/<month>/<day>/`.
- When two assets would otherwise collide on the same canonical dated path,
  the worker adds a deterministic suffix before the file extension.
- Album views are created afterwards under `albums/<album>/`.
- Album views prefer hard links and fall back to file copies where linking is
  not possible.
- When hard-link mode falls back to copying, worker logs record the affected
  album view path and canonical source path.
- Download workers run in parallel automatically based on host CPU.
- Worker count is internally bounded and can be overridden with
  `SYNC_DOWNLOAD_WORKERS`.
- Download stream chunk size can be tuned with `SYNC_DOWNLOAD_CHUNK_MIB`.
- Transient download failures are retried a small number of times during the
  same run before the worker records a final transfer failure.
- Successful downloads preserve remote modified timestamps on local files.
- Optional mirror-delete behaviour can be enabled with
  `BACKUP_DELETE_REMOVED=true`, which prunes local files and empty directories
  under `/output` when they no longer exist in iCloud.

## Config validation

- Invalid numeric values such as `SYNC_DOWNLOAD_CHUNK_MIB=abc` or
  `SCHEDULE_INTERVAL_MINUTES=abc` are treated as startup validation errors.
- The worker does not silently fall back to defaults when those values are set
  explicitly but cannot be parsed.

## Single-writer operation

- One worker process must have exclusive write access to a given `/config` and
  `/output` state set.
- If a second worker starts with the same shared config directory, startup is
  blocked by the runtime lock and the worker exits non-zero.

## Safety-net behaviour

On first run only, each worker samples existing files in `/output` and checks
UID and GID for consistency against the container runtime user.

If mismatches are found, backup is blocked. Details are written to worker logs
and sent via Telegram. This is intended to avoid destructive rewrites over
existing backup trees with mixed ownership.

The sample stays bounded, but it is selected deterministically across the full
tree rather than just taking the first files returned by the filesystem walk.
That makes the first-run check less biased on large libraries with many year
and album subtrees.

## State recovery

- If `pyiclodoc-photos-auth_state.json` or `pyiclodoc-photos-manifest.json`
  contains malformed JSON, the worker quarantines the bad file with a
  `.corrupt` suffix and carries on with empty in-memory state.
- This is intended to turn truncated or manually damaged state files into a
  recoverable operator problem rather than a startup crash.
- If auth-state JSON is valid but fields are malformed, the worker normalizes
  or resets the affected fields, logs what changed, and continues with a safe
  in-memory auth state.
- Persisted auth timestamps are expected to be offset-aware ISO-8601 values.
  If a legacy or hand-edited timestamp omits its timezone offset, the worker
  logs the repair and treats it as UTC before reminder logic runs.
- If manifest JSON is valid but not a top-level object, the worker logs that
  the manifest shape was invalid and continues with an empty manifest.
- Each backup run logs manifest growth detail as `previous_entries`,
  `refreshed_entries`, and `delta` so long-term manifest size can be monitored
  from normal worker logs.

## Reimport note

The canonical `library/` tree is the safest file-based export shape for later
reimport because it preserves one obvious source copy per asset. The `albums/`
tree is useful for browsing and external tooling, but file-based reimport alone
will not recreate album membership in Photos.app.
