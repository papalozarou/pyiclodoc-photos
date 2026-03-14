# Operations

## Runtime behaviour

- The container starts as root only to read secrets.
- The entrypoint then drops to the configured UID and GID with `su-exec`.
- The worker keeps a heartbeat file fresh under `/logs/pyiclodoc-photos-heartbeat.txt`.
- The worker stores auth state, manifest state, keyring state, cookies, and session data under `/config`.

## Safety net

- The safety net only runs before the first successful backup.
- It samples existing files under `/output`.
- If ownership does not match the runtime UID and GID, backup is blocked.
- The worker logs explicit mismatches and sends the expected UID and GID via Telegram.
- Once the safety net passes, `pyiclodoc-photos-safety_net_done.flag` prevents the check from running again.

## Backup layout

- Canonical files are written to `library/<year>/<month>/<day>/`.
- Album views are written to `albums/<album>/`.
- Album views use hard links where possible.
- If hard links are not possible, the worker falls back to normal file copies.

## Reimport note

The canonical `library/` tree is the safest file-based export shape for later reimport because it preserves one obvious source copy per asset. The album tree is useful for browsing and for external tooling, but file-based reimport alone will not recreate album membership in Photos.app.
