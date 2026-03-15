# Telegram

## Command format

Commands are only accepted from the chat ID configured in `H_TGM_CHAT_ID`.

Supported command forms:

- `<username> backup`
- `<username> auth`
- `<username> auth 123456`
- `<username> reauth`
- `<username> reauth 123456`

N.B.

`<username>` must match the container username for that worker service.

## Authentication and reauthentication flow

1. On startup, the worker attempts iCloud authentication using saved session
   state and configured credentials.
2. If MFA is required, the worker marks auth pending and sends a prompt.
3. The user sends either `auth <code>` or `reauth <code>` via Telegram to
   complete the current pending challenge.
4. `auth <code>` and `reauth <code>` do not start a fresh login attempt; they
   only validate against the active pending session.
5. If a worker restart clears in-memory auth session state, send `auth` or
   `reauth` without a code first to trigger a new challenge prompt.
6. If successful, pending auth state is cleared and normal backup flow resumes.

## Reminder and reauth timing

- When reauthentication is due within five days, the worker sends a reminder.
- When reauthentication is due within two days, the worker switches to a
  reauth-required prompt.
- If reauth is still pending, automatic backup does not proceed until auth is
  completed.

## Password file behaviour

`<SVC>_ICLOUD_PASSWORD_FILE` can hold either:

- an Apple Account password; or
- an app-specific password.

The value is passed directly to `pyicloud`, and final auth/MFA handling still
follows Apple account policy.

## Outbound Telegram messages

Messages use this compact structure:

- bold emoji header in sentence case;
- one-line action summary including Apple ID; and
- optional compact status lines.

Current message templates include:

- `*🟢 PCD Photos - Container started*`
- `*🛑 PCD Photos - Container stopped*`
- `*🔑 PCD Photos - Authentication required*`
- `*🔑 PCD Photos - Reauthentication required*`
- `*🔒 PCD Photos - Authentication complete*`
- `*❌ PCD Photos - Authentication failed*`
- `*📥 PCD Photos - Backup requested*`
- `*⬇️ PCD Photos - Backup started*`
- `*📦 PCD Photos - Backup complete*`
- `*⏭️ PCD Photos - Backup skipped*`
- `*⚠️ PCD Photos - Safety net blocked*`
- `*📣 PCD Photos - Reauth reminder*`

Authentication-required messages can include:

- `Send: <username> auth 123456`
- `Or: <username> reauth 123456`
- `One-shot mode is waiting for an auth command before backup.`
- `Wait window: 15 mins.`

Reauthentication-required messages can include:

- `Reauthentication is due within two days.`
- `Send: <username> reauth`
- `Reauthentication required for Apple ID <apple-id>.`
- `Send: <username> reauth 123456`

Reauth reminder messages use this text:

- `Reauthentication will be required within five days.`

Backup completion messages include:

- `Transferred: <done>/<total>`
- `Skipped: <count>`
- `Errors: <count>`
- `Duration: <hh:mm:ss>`
- `Deleted: <files> files, <directories> directories` when
  `BACKUP_DELETE_REMOVED=true`
- `Average speed: <value> MiB/s` only when files were downloaded

Backup start messages include:

- `Photos downloading for Apple ID <apple-id>.`
- `Scheduled <plain English schedule>`
- `Manual, then <plain English schedule>`
- `One-shot run – configured schedule is ignored.`

Backup requested messages include:

- `Manual backup requested for Apple ID <apple-id>.`
- `Worker queued backup to run now.`

Backup skipped messages include:

- `Backup skipped for Apple ID <apple-id>.`
- `Reason: Authentication incomplete.`
- `Reason: Reauthentication pending.`

Safety-net blocked messages include an explicit expected ownership line:

- `Expected: uid <uid>, gid <gid>`

Container lifecycle messages can include:

- `Worker started for Apple ID <apple-id>.`
- `Initialising authentication and backup checks.`
- `Worker stopped for Apple ID <apple-id>.`
- `<stop-status>`
