# PyiCloDoc Photos

A dockerised `pyicloud` worker for backing up iCloud Photos to local storage, with Telegram used for auth prompts, reauth prompts, status messages, and manual backups.

## Quick start

1. Copy `compose.yml.example` to `compose.yml`.
2. Copy `.env.example` to `.env`.
3. Set host paths and service values in `.env`.
4. Create secret files under `${H_DKR_SECRETS}`.
5. Start the stack:

```bash
docker compose up -d --build
```

6. Check health:

```bash
docker compose ps
docker inspect --format='{{json .State.Health}}' icloud_photos_alice
docker inspect --format='{{json .State.Health}}' icloud_photos_bob
```

## What it does

- Uses the same app shape and pinned `pyicloud` version as `pyiclodoc-drive`.
- Runs as root only long enough to read secrets, then drops to the configured UID and GID.
- Stores canonical files under `library/<year>/<month>/<day>/`.
- Builds optional album views under `albums/`, using hard links where possible and copy fallback where needed.
- Keeps auth state, session state, and keyring state on mounted storage so MFA prompts are not repeated on every start.
- Supports one-shot, interval, daily, weekly, twice-weekly, and monthly schedules.
- Accepts Telegram commands in the form `<username> backup`, `<username> auth`, and `<username> reauth`.

## Testing

```bash
python3 -m unittest -q
```

## Documentation

- [CONFIGURATION.md](CONFIGURATION.md): env vars, paths, and storage layout.
- [SCHEDULING.md](SCHEDULING.md): schedule modes and examples.
- [TELEGRAM.md](TELEGRAM.md): command format and outbound messages.
- [OPERATIONS.md](OPERATIONS.md): runtime behaviour, safety net, and backup layout.

## License

This project is provided under the GNU General Public License v3.0.
