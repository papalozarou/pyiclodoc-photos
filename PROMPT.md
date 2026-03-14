# PyiCloDoc Photos

Follow AGENTS.md as the policy for all work in this repository.

Using [pyiclodoc-drive](https://github.com/papalozarou/pyiclodoc-drive) as a template, create a minimal, non-root, Docker container to download photos from iCloud.

Where things are unclear, or if you are unsure how much to follow pyiclodoc-drive, ask the user to clarify.

## Docker container

Using pyiclodoc-drive as a template, the Docker container must:

- use a specifiable version of Alpine Linux as it's base;
- use the same version of pyicloud as pyiclodoc-drive;
- use the same app structure as pyiclodoc-drive;
- accept user and group values for the container UID and GID, with defaults of 1000 for each;
- run with minimal privileges after starting as root to access secrets;
- allow a specificed username within the container, for use with telegram messaging;
- accept a TZ environment variable to be used in the container to aid with syncing;
- allow incremental backups as efficiently as possible;
- allow for multiple containers in the same compose project;
- allow one-shot, weekly, twice weekly and monthly scheduling, specifying days and times where applicable, i.e. not one-shot;
- allow manual user triggered backup;
- allow passing docker secrets for iCloud email and password;
- allow Telegram notifications of containers starting and stopping, authentication, reauthentication, backups starting, backups finishing, with data on files transferred, errors etc.;
- allow Telegram authentication and reauthentication;
- store a users iCloud credentials in the keychain to avoid reentry until reauthentication is required;
- reuse the keychain and any associated authentication files from pyiclodoc drive, if this is feasible and not a security risk;
- allow performance tuning to enable faster downloading;
- allow only an opinionated year > day > month backup to start with;
- allow album backup, including shared albums and favourites;
- make the above two bullets extensible as we will likely build on this later;
- implement a performant safety net that checks UID and GID for consistency against the container runtime user and stops backup if mismatches are found;
- ensure the safety net is only applied on first run and that the safety net tells the user explicitly the permissions that would match existing files, via Telegram and in the container logs; and
- use a utility from https://github.com/tarampampam/microcheck as the healthcheck.

## Authentication
Using pyiclodoc-drive as a template, for authentication and reauthentication via Telegram the container must:

- prompt on initial run to authenticate with multi-factor authentication;
- subsequently alert the user that reauthentication via multi-factor authentication is required within five days;
- prompt the user to reauthenticate using multi-factor authentication when it is required within two days; and
- allow for authentication and reauthentication edge cases by allowing a user to message Telegram with "<username> auth" or "<username> reauth", where username matches the username within the container.

## Logs
Using pyiclodoc-drive as a template, three log levels will be provided, info, debug and error. Debug must be comprehensive.

## Project/repository documentation
Adhering to AGENTS.md, implement comprehensive documentation, splitting out into separate files as required.

## Supplementart information
Useful supplementary information can be found on other similar projects:

- https://github.com/boredazfcuk/docker-icloudpd
- https://github.com/mandarons/icloud-docker
- https://github.com/icloud-photos-downloader/icloud_photos_downloader
