# ------------------------------------------------------------------------------
# This module contains manifest comparison and transfer-planning helpers for the
# photo sync workflow.
# ------------------------------------------------------------------------------

from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

from app.logger import log_line

if TYPE_CHECKING:
    from app.icloud_client import RemoteEntry


# ------------------------------------------------------------------------------
# This function returns a deterministic metadata dictionary for a remote entry.
#
# 1. "ENTRY" is a remote photo metadata record.
#
# Returns: Dictionary payload persisted in the incremental manifest.
#
# N.B.
# Album paths are stored in the canonical entry metadata so derived album views
# can be recreated without re-reading remote album membership during deletes.
# ------------------------------------------------------------------------------
def entry_metadata(ENTRY: RemoteEntry) -> dict[str, Any]:
    return {
        "asset_id": ENTRY.asset_id,
        "album_paths": list(ENTRY.album_paths),
        "created": ENTRY.created,
        "download_name": ENTRY.download_name,
        "is_dir": False,
        "modified": ENTRY.modified,
        "size": ENTRY.size,
    }


# ------------------------------------------------------------------------------
# This function decides whether a file should be transferred.
#
# 1. "ENTRY" is current remote metadata.
# 2. "MANIFEST" is previous run metadata.
#
# Returns: True when transfer is required, otherwise False.
# ------------------------------------------------------------------------------
def needs_transfer(ENTRY: RemoteEntry, MANIFEST: dict[str, dict[str, Any]]) -> bool:
    EXISTING = MANIFEST.get(ENTRY.path)

    if EXISTING is None:
        return True

    if str(EXISTING.get("asset_id", "")) != ENTRY.asset_id:
        return True

    if int(EXISTING.get("size", -1)) != ENTRY.size:
        return True

    if str(EXISTING.get("modified", "")) != ENTRY.modified:
        return True

    return False


# ------------------------------------------------------------------------------
# This function decides whether a file already matches manifest state.
#
# 1. "ENTRY" is current remote metadata.
# 2. "MANIFEST" is previous run metadata.
#
# Returns: True when the manifest already reflects the current remote entry.
# ------------------------------------------------------------------------------
def entry_matches_manifest(ENTRY: RemoteEntry, MANIFEST: dict[str, dict[str, Any]]) -> bool:
    return not needs_transfer(ENTRY, MANIFEST)


# ------------------------------------------------------------------------------
# This function prepares transfer candidates and unchanged manifest entries for
# the current sync run.
#
# 1. "FILES" is the current remote photo list.
# 2. "MANIFEST" is previous run metadata.
# 3. "LOG_FILE" is optional worker log destination.
#
# Returns: Tuple "(new_manifest, transfer_candidates, skipped_files)".
# ------------------------------------------------------------------------------
def build_sync_plan(
    FILES: list[RemoteEntry],
    MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE,
) -> tuple[dict[str, dict[str, Any]], list[RemoteEntry], int]:
    NEW_MANIFEST: dict[str, dict[str, Any]] = {}
    TRANSFER_CANDIDATES: list[RemoteEntry] = []
    SKIPPED = 0

    for ENTRY in FILES:
        if needs_transfer(ENTRY, MANIFEST):
            TRANSFER_CANDIDATES.append(ENTRY)
            if LOG_FILE is not None:
                log_line(
                    LOG_FILE,
                    "debug",
                    f"Photo queued for transfer: {ENTRY.path} "
                    f"({max(ENTRY.size, 0)} bytes)",
                )
            continue

        NEW_MANIFEST[ENTRY.path] = entry_metadata(ENTRY)
        SKIPPED += 1
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", f"Photo skipped unchanged: {ENTRY.path}")

    return NEW_MANIFEST, TRANSFER_CANDIDATES, SKIPPED


# ------------------------------------------------------------------------------
# This function returns canonical paths already validated for this run.
#
# 1. "NEW_MANIFEST" is the refreshed manifest under construction.
#
# Returns: Canonical-path set safe to use for derived album output.
# ------------------------------------------------------------------------------
def get_valid_canonical_paths(NEW_MANIFEST: dict[str, dict[str, Any]]) -> set[str]:
    RESULT: set[str] = set()

    for RELATIVE_PATH, METADATA in NEW_MANIFEST.items():
        if str(METADATA.get("entry_kind", "")) == "album_link":
            continue

        RESULT.add(RELATIVE_PATH)

    return RESULT
