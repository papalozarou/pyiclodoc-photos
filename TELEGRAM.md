# Telegram

## Command format

Commands are only accepted from the configured Telegram chat ID.

Supported command forms:

- `<username> backup`
- `<username> auth`
- `<username> auth 123456`
- `<username> reauth`
- `<username> reauth 123456`

`<username>` must match the container username for that worker service.

## Authentication flow

1. On startup, the worker tries to authenticate using saved session state and configured credentials.
2. If Apple requires MFA, the worker marks auth as pending and sends a Telegram prompt.
3. The user replies with `auth <code>` or `reauth <code>`.
4. If the session has been lost after a restart, the user can send `auth` or `reauth` first to trigger a fresh prompt.
5. Five days before the configured reauth deadline, the worker sends a reminder.
6. Two days before the configured reauth deadline, the worker switches to a reauth-required prompt.

## Message headings

The worker mirrors the `pyiclodoc-drive` message style, but uses `PCD Photos` in the title:

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
