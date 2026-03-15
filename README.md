# PyiCloDoc Photos

A dockerised `pyicloud` implementation for backing up iCloud drives to local storage, with Telegram used for auth prompts and operational control. It is intended to be the photo companion to  [pyiclodoc-drive](https://github.com/papalozarou/pyiclodoc-drive).

It should have all the bits you need for real-world usage, such as:

* persistent auth/session state;
* manifest-driven incremental sync;
* optional `until_found` for faster newest-first incremental runs;
* one-shot and scheduled modes;
* Telegram-driven auth and manual backup control;
* protection of existing backups via a first-run safety net;
* a canonical photo library layout plus optional derived album views; 
* suffixes added to assets that match same `library/<year>/<month>/<day>/<filename>`; and
* backup of more than one iCloud photo library using all of the above.

It is intended to be set and forget – start it, authorise when needed, and let it do the rest.

*N.B.*

You have probably already guessed from the `PROMPT.md` file that this was AI built.

It started with the experiment in [pyiclodoc-drive](https://github.com/papalozarou/pyiclodoc-drive), and the logical next step was to build this project.

## Quick start

The example `compose.yml` and `.env` files run two isolated containers out of the box, Alice and Bob, each with separate config, output, and logs. These examples give you a flavour of what PyiCloDoc Photos can do, and enough information to configure it to your needs. Complete documentation is linked at the end of this README.

1. Copy `compose.yml.example` to `compose.yml`.
2. Copy `.env.example` to `.env`.
3. Set host and service values in `.env`.
4. Create secret files under `${H_DKR_SECRETS}`:
   `telegram_bot_token.txt`, `alice_icloud_email.txt`,
   `alice_icloud_password.txt`, `bob_icloud_email.txt`,
   `bob_icloud_password.txt`.
5. Start containers:

```bash
docker compose up -d --build
```

6. Check status:

```bash
docker compose ps
docker inspect --format='{{json .State.Health}}' icloud_photos_alice
docker inspect --format='{{json .State.Health}}' icloud_photos_bob
```

## Example usage

The example `compose.yml` and `.env` files run two isolated workers out of the
box, Alice and Bob, each with separate config, output, and logs. Those example
files are intended to be edited directly and should give you enough to get a
real deployment running.

## What the backup layout looks like

Canonical files are stored under:

```text
library/<year>/<month>/<day>/
```

For example:

```text
library/2026/03/14/IMG_1234.HEIC
library/2026/03/14/IMG_1234.MOV
library/2026/03/14/IMG_1234--0d4e6f8a1b2c.HEIC
```

Optional album views are stored under:

```text
albums/<album name>/
```

For example:

```text
albums/Trips/IMG_1234.HEIC
albums/Favourites/IMG_1234.HEIC
```

N.B.

The `albums/` tree is derived from the canonical files. It is useful for
browsing and external tooling, but it does not by itself preserve album
membership for later reimport into Photos.app.

## Testing

Run the unit tests with:

```bash
python3 -m unittest -q
```

## Detailed documentation

- [CONFIGURATION.md](CONFIGURATION.md): env variables, paths, state layout, and
  default behaviour.
- [SCHEDULING.md](SCHEDULING.md): schedule modes, compatibility rules, and
  manual backup behaviour.
- [TELEGRAM.md](TELEGRAM.md): command format, auth flow, reauth flow, and
  outbound message structure.
- [OPERATIONS.md](OPERATIONS.md): runtime behaviour, privilege model,
  performance notes, safety-net behaviour, and backup layout.

## License

This project is provided under the GNU General Public License v3.0.

You can use, modify, and redistribute this project, but any redistributed
modified version must also remain under GPL-3.0 and include the source code.
