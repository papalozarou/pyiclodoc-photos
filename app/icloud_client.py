# ------------------------------------------------------------------------------
# This module wraps pyicloud authentication, session persistence, and iCloud
# Photos access.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
import hashlib
import os
import re
import shutil

from pyicloud import PyiCloudService

from app.config import AppConfig
from app.sync_plan import entry_matches_manifest

FILE_NAME_SANITISE_PATTERN = re.compile(r"[^A-Za-z0-9._ -]+")


# ------------------------------------------------------------------------------
# This exception signals that a download handle does not expose a supported
# payload interface for this worker.
# ------------------------------------------------------------------------------
class UnsupportedDownloadHandleError(Exception):
    pass


# ------------------------------------------------------------------------------
# This data class represents one remote photo asset and its derived backup
# targets.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class RemoteEntry:
    path: str
    is_dir: bool
    size: int
    modified: str
    asset_id: str = ""
    created: str = ""
    download_name: str = ""
    album_paths: tuple[str, ...] = field(default_factory=tuple)


# ------------------------------------------------------------------------------
# This data class captures one canonical download attempt outcome.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class DownloadResult:
    success: bool
    failure_reason: str = ""
    written_bytes: int = 0


# ------------------------------------------------------------------------------
# This data class keeps one resolved entry bound to its source asset object.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class ResolvedAssetEntry:
    entry: RemoteEntry
    asset: Any


# ------------------------------------------------------------------------------
# This class encapsulates iCloud auth, photo listing, and asset downloads.
# 
# N.B.
# The class name is retained for compatibility with the existing template and
# surrounding runtime code, even though this implementation targets Photos.
# ------------------------------------------------------------------------------
class ICloudDriveClient:
# ------------------------------------------------------------------------------
# This function stores runtime configuration and initialises client state.
#
# 1. "CONFIG" is the runtime configuration model used by this client.
#
# Returns: None.
# 
# N.B.
# The client still exposes the last failure reason for compatibility, but the
# sync layer now consumes explicit per-transfer results instead.
# ------------------------------------------------------------------------------
    def __init__(self, CONFIG: AppConfig):
        self.config = CONFIG
        self.api: PyiCloudService | None = None
        self._last_download_failure_reason = ""
        self._cached_entries: list[RemoteEntry] = []
        self._cached_assets_by_path: dict[str, Any] = {}

# ------------------------------------------------------------------------------
# This function aligns cookie and session paths with an
# icloudpd-compatible folder layout.
#
# Returns: None.
# 
# N.B.
# The compat links make it practical to reuse persistent auth artefacts from
# other pyicloud-based tooling without copying cookie trees by hand.
# ------------------------------------------------------------------------------
    def prepare_compat_paths(self) -> None:
        self.config.icloudpd_compat_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_link(self.config.icloudpd_compat_dir / "cookies", self.config.cookie_dir)
        self._ensure_link(self.config.icloudpd_compat_dir / "session", self.config.session_dir)

# ------------------------------------------------------------------------------
# This function creates a symlink and removes incompatible existing paths.
#
# 1. "LINK_PATH" is the compatibility symlink path.
# 2. "TARGET_PATH" is the canonical storage directory.
#
# Returns: None.
# 
# N.B.
# Existing non-symlink paths are removed deliberately because mixed real
# directories and symlinks in the compat tree are harder to debug.
# ------------------------------------------------------------------------------
    def _ensure_link(self, LINK_PATH: Path, TARGET_PATH: Path) -> None:
        if LINK_PATH.is_symlink():
            try:
                if LINK_PATH.resolve() == TARGET_PATH.resolve():
                    return
            except FileNotFoundError:
                pass

        if LINK_PATH.exists():
            if LINK_PATH.is_dir() and not LINK_PATH.is_symlink():
                shutil.rmtree(LINK_PATH)
            else:
                LINK_PATH.unlink()

        LINK_PATH.symlink_to(TARGET_PATH, target_is_directory=True)

# ------------------------------------------------------------------------------
# This function creates a pyicloud client with constructor compatibility
# across library versions.
#
# Returns: Initialised "PyiCloudService" instance.
# ------------------------------------------------------------------------------
    def _create_service(self) -> PyiCloudService:
        return PyiCloudService(
            self.config.icloud_email,
            self.config.icloud_password,
            cookie_directory=str(self.config.cookie_dir),
        )

# ------------------------------------------------------------------------------
# This function starts an iCloud authentication attempt.
#
# Returns: Tuple "(is_authenticated, details_message)".
# ------------------------------------------------------------------------------
    def start_authentication(self) -> tuple[bool, str]:
        self.prepare_compat_paths()
        self.api = self._create_service()
        self._clear_listing_cache()

        if self.api.requires_2fa:
            return False, "Two-factor code is required."

        if getattr(self.api, "requires_2sa", False):
            return False, "Two-step authentication is required; use app-specific passwords where possible."

        return True, "Authenticated successfully."

# ------------------------------------------------------------------------------
# This function completes a pending authentication challenge with an MFA code.
#
# 1. "CODE" is the MFA code to validate.
#
# Returns: Tuple "(is_authenticated, details_message)".
# ------------------------------------------------------------------------------
    def complete_authentication(self, CODE: str) -> tuple[bool, str]:
        if self.api is None:
            return False, "Authentication session is not initialised."

        CODE = CODE.strip()

        if not CODE:
            return False, "Two-factor code is required."

        if not self.api.requires_2fa:
            return True, "Authenticated successfully."

        if not self.api.validate_2fa_code(CODE):
            return False, "Two-factor code was rejected by Apple."

        if self.api.is_trusted_session:
            return True, "Authenticated successfully with 2FA."

        if not self.api.trust_session():
            return False, "Two-factor code was accepted, but Apple did not trust this session."

        return True, "Authenticated successfully with trusted 2FA session."

# ------------------------------------------------------------------------------
# This function authenticates with iCloud and optionally completes MFA.
#
# 1. "CODE_PROVIDER" is a zero-argument callable returning an MFA code when
#    needed.
#
# Returns: Tuple "(is_authenticated, details_message)".
# ------------------------------------------------------------------------------
    def authenticate(self, CODE_PROVIDER: Callable[[], str]) -> tuple[bool, str]:
        CODE = CODE_PROVIDER().strip()

        if CODE:
            return self.complete_authentication(CODE)

        return self.start_authentication()

# ------------------------------------------------------------------------------
# This function lists remote photo assets as canonical backup entries.
#
# Returns: Flat list of photo entries.
# 
# N.B.
# Album membership is derived separately from the canonical photo listing so
# the worker can keep one primary copy per asset.
# ------------------------------------------------------------------------------
    def list_entries(self) -> list[RemoteEntry]:
        if self.api is None:
            return []

        self._refresh_listing_cache()
        return list(self._cached_entries)

# ------------------------------------------------------------------------------
# This function lists remote photo assets using the configured discovery mode.
#
# 1. "MANIFEST" is previous sync metadata used for optional early-stop logic.
#
# Returns: Flat list of photo entries.
# 
# N.B.
# "full" keeps the safer complete scan. "until_found" stops scanning
# "All Photos" after enough consecutive unchanged canonical entries have been
# observed, using the threshold from "BACKUP_UNTIL_FOUND_COUNT".
# ------------------------------------------------------------------------------
    def list_entries_for_sync(
        self,
        MANIFEST: dict[str, dict[str, Any]],
    ) -> list[RemoteEntry]:
        if self.api is None:
            return []

        if self._should_use_until_found_discovery():
            self._refresh_listing_cache_until_found(MANIFEST)
            return list(self._cached_entries)

        self._refresh_listing_cache()
        return list(self._cached_entries)

# ------------------------------------------------------------------------------
# This function rebuilds the listing cache from the current remote state.
#
# Returns: None.
#
# N.B.
# The cache stores both the resolved entry list and the canonical-path-to-asset
# mapping so downloads can reuse one remote listing pass without losing asset
# identity when canonical paths are disambiguated.
# ------------------------------------------------------------------------------
    def _refresh_listing_cache(self) -> None:
        ALL_ASSETS = self._read_all_assets()
        ALBUM_MAP = self._read_album_membership()
        BASE_ENTRIES = self._build_remote_entries(ALL_ASSETS, ALBUM_MAP)
        RESOLVED_ENTRIES = self._resolve_entry_path_collisions(BASE_ENTRIES)
        ASSETS_BY_PATH: dict[str, Any] = {}

        for ITEM in RESOLVED_ENTRIES:
            ASSETS_BY_PATH[ITEM.entry.path] = ITEM.asset

        RESOLVED_ENTRIES.sort(key=lambda ITEM: ITEM.entry.path)
        self._cached_entries = [ITEM.entry for ITEM in RESOLVED_ENTRIES]
        self._cached_assets_by_path = ASSETS_BY_PATH

# ------------------------------------------------------------------------------
# This function decides whether the configured discovery mode can safely use
# partial newest-first scanning for the current sync run.
#
# Returns: True when "until_found" can be used safely, otherwise False.
#
# N.B.
# Delete reconciliation and album management both require an authoritative
# full-library snapshot. This guard keeps the client from using a partial
# discovery result as if it described the whole remote state.
# ------------------------------------------------------------------------------
    def _should_use_until_found_discovery(self) -> bool:
        if self.config.backup_discovery_mode != "until_found":
            return False

        if self.config.backup_delete_removed:
            return False

        if self.config.backup_albums_enabled:
            return False

        return True

# ------------------------------------------------------------------------------
# This function rebuilds the listing cache using the early-stop discovery
# mode.
#
# 1. "MANIFEST" is previous sync metadata used to detect unchanged streaks.
#
# Returns: None.
# 
# N.B.
# This relies on pyicloud documenting that "All Photos" is sorted by
# "added_date" with the most recently added assets first. It is therefore an
# optimisation mode, not the safest default.
# ------------------------------------------------------------------------------
    def _refresh_listing_cache_until_found(
        self,
        MANIFEST: dict[str, dict[str, Any]],
    ) -> None:
        ALL_ASSETS = self._read_all_assets_until_found(MANIFEST)
        ALBUM_MAP = self._read_album_membership()
        BASE_ENTRIES = self._build_remote_entries(ALL_ASSETS, ALBUM_MAP)
        RESOLVED_ENTRIES = self._resolve_entry_path_collisions(BASE_ENTRIES)
        ASSETS_BY_PATH: dict[str, Any] = {}

        for ITEM in RESOLVED_ENTRIES:
            ASSETS_BY_PATH[ITEM.entry.path] = ITEM.asset

        RESOLVED_ENTRIES.sort(key=lambda ITEM: ITEM.entry.path)
        self._cached_entries = [ITEM.entry for ITEM in RESOLVED_ENTRIES]
        self._cached_assets_by_path = ASSETS_BY_PATH

# ------------------------------------------------------------------------------
# This function clears cached remote-listing state after auth changes.
#
# Returns: None.
# ------------------------------------------------------------------------------
    def _clear_listing_cache(self) -> None:
        self._cached_entries = []
        self._cached_assets_by_path = {}

# ------------------------------------------------------------------------------
# This function builds base remote entries before collision resolution.
#
# 1. "ALL_ASSETS" is the full photo asset list.
# 2. "ALBUM_MAP" maps asset IDs to album output paths.
#
# Returns: Entry-and-asset bindings using the readable default layout.
# ------------------------------------------------------------------------------
    def _build_remote_entries(
        self,
        ALL_ASSETS: list[Any],
        ALBUM_MAP: dict[str, tuple[str, ...]],
    ) -> list[ResolvedAssetEntry]:
        RESULT: list[ResolvedAssetEntry] = []

        for INDEX, ASSET in enumerate(ALL_ASSETS, start=1):
            RESULT.append(
                ResolvedAssetEntry(
                    entry=self._build_remote_entry(ASSET, INDEX, ALBUM_MAP),
                    asset=ASSET,
                )
            )

        return RESULT

# ------------------------------------------------------------------------------
# This function resolves canonical-path collisions across built entries.
#
# 1. "ENTRIES" is the base resolved entry-and-asset list.
#
# Returns: Entry-and-asset list with deterministic disambiguation applied when
# needed.
#
# N.B.
# Distinct assets can legitimately share the same day and original filename.
# When that happens, every colliding entry receives a stable suffix derived
# from asset identity and keeps its original asset binding.
# ------------------------------------------------------------------------------
    def _resolve_entry_path_collisions(
        self,
        ENTRIES: list[ResolvedAssetEntry],
    ) -> list[ResolvedAssetEntry]:
        PATH_GROUPS: dict[str, list[ResolvedAssetEntry]] = {}

        for ENTRY in ENTRIES:
            PATH_GROUPS.setdefault(ENTRY.entry.path, []).append(ENTRY)

        RESULT: list[ResolvedAssetEntry] = []

        for ORIGINAL_PATH in sorted(PATH_GROUPS.keys()):
            GROUP = PATH_GROUPS[ORIGINAL_PATH]

            if len(GROUP) == 1:
                RESULT.extend(GROUP)
                continue

            RESULT.extend(self._disambiguate_entry_group(GROUP))

        return RESULT

# ------------------------------------------------------------------------------
# This function rewrites one colliding entry group to unique stable paths.
#
# 1. "ENTRIES" is the colliding resolved entry-and-asset group.
#
# Returns: Entry-and-asset list with suffixes applied in a stable order.
# ------------------------------------------------------------------------------
    def _disambiguate_entry_group(self, ENTRIES: list[ResolvedAssetEntry]) -> list[ResolvedAssetEntry]:
        RESULT: list[ResolvedAssetEntry] = []
        SORTED_ENTRIES = sorted(
            ENTRIES,
            key=lambda ITEM: (
                ITEM.entry.asset_id,
                ITEM.entry.modified,
                ITEM.entry.path,
            ),
        )

        for ITEM in SORTED_ENTRIES:
            DISAMBIGUATED_NAME = self._add_collision_suffix(
                ITEM.entry.download_name,
                ITEM.entry.asset_id,
            )
            CANONICAL_PATH = self._replace_file_name(ITEM.entry.path, DISAMBIGUATED_NAME)
            RESULT.append(
                ResolvedAssetEntry(
                    entry=RemoteEntry(
                        path=CANONICAL_PATH,
                        is_dir=ITEM.entry.is_dir,
                        size=ITEM.entry.size,
                        modified=ITEM.entry.modified,
                        asset_id=ITEM.entry.asset_id,
                        created=ITEM.entry.created,
                        download_name=DISAMBIGUATED_NAME,
                        album_paths=ITEM.entry.album_paths,
                    ),
                    asset=ITEM.asset,
                )
            )

        return RESULT

# ------------------------------------------------------------------------------
# This function adds a stable collision suffix to a file name.
#
# 1. "FILE_NAME" is the original or sanitised file name.
# 2. "ASSET_ID" is the stable asset identifier.
#
# Returns: File name with collision suffix inserted before the extension.
# 
# N.B.
# Suffix stability is strongest when pyicloud exposes a durable asset ID. When
# the worker must fall back to a synthetic identifier, the suffix remains
# deterministic for that observed listing but may change if upstream ordering
# changes.
# ------------------------------------------------------------------------------
    def _add_collision_suffix(self, FILE_NAME: str, ASSET_ID: str) -> str:
        BASE_NAME, FILE_SUFFIX = os.path.splitext(FILE_NAME)
        SUFFIX_DIGEST = hashlib.sha1(ASSET_ID.encode("utf-8")).hexdigest()[:12]
        return f"{BASE_NAME}--{SUFFIX_DIGEST}{FILE_SUFFIX}"

# ------------------------------------------------------------------------------
# This function replaces the trailing file name in a relative path.
#
# 1. "RELATIVE_PATH" is the relative output path.
# 2. "FILE_NAME" is the replacement file name.
#
# Returns: Relative path with the new trailing file name applied.
# ------------------------------------------------------------------------------
    def _replace_file_name(self, RELATIVE_PATH: str, FILE_NAME: str) -> str:
        PARENT_TEXT = str(Path(RELATIVE_PATH).parent).replace("\\", "/")

        if PARENT_TEXT == ".":
            return FILE_NAME

        return f"{PARENT_TEXT}/{FILE_NAME}"

# ------------------------------------------------------------------------------
# This function downloads one asset into the canonical library tree.
#
# 1. "REMOTE_PATH" is the canonical relative output path.
# 2. "LOCAL_PATH" is the full destination file path.
#
# Returns: "DownloadResult" describing download success or failure.
# 
# Failure behaviour:
# 1. Keeps the final destination untouched unless the full download succeeds.
# 2. Returns a concrete failure token instead of relying on shared mutable
#    state for concurrent callers.
# ------------------------------------------------------------------------------
    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        if self.api is None:
            return self._set_download_failure_result("not_authenticated")

        ASSET = self._get_asset_by_remote_path(REMOTE_PATH)

        if ASSET is None:
            return self._set_download_failure_result("asset_not_found")

        DOWNLOAD_HANDLE = self._open_asset_download(ASSET)

        if DOWNLOAD_HANDLE is None:
            return self._set_download_failure_result("download_unavailable")

        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        TEMP_PATH = self._get_temporary_download_path(LOCAL_PATH)
        EXPECTED_SIZE = self._asset_size(ASSET)

        try:
            WRITTEN_BYTES = self._write_download_to_temp_file(DOWNLOAD_HANDLE, TEMP_PATH)
            FAILURE_REASON = self._validate_download_size(WRITTEN_BYTES, EXPECTED_SIZE)

            if FAILURE_REASON:
                self._cleanup_download_temp_file(TEMP_PATH)
                return self._set_download_failure_result(FAILURE_REASON)

            TEMP_PATH.replace(LOCAL_PATH)
        except UnsupportedDownloadHandleError:
            self._cleanup_download_temp_file(TEMP_PATH)
            return self._set_download_failure_result("empty_download")
        except OSError:
            self._cleanup_download_temp_file(TEMP_PATH)
            return self._set_download_failure_result("write_failed")
        except Exception:
            self._cleanup_download_temp_file(TEMP_PATH)
            return self._set_download_failure_result("download_read_failed")

        self._last_download_failure_reason = ""
        return DownloadResult(True, written_bytes=WRITTEN_BYTES)

# ------------------------------------------------------------------------------
# This function downloads one asset into the canonical library tree.
#
# 1. "REMOTE_PATH" is the canonical relative output path.
# 2. "LOCAL_PATH" is the full destination file path.
#
# Returns: True on success, otherwise False.
# 
# Failure behaviour:
# 1. Preserves the historic boolean return contract for compatibility.
# ------------------------------------------------------------------------------
    def download_file(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> bool:
        return self.download_file_result(REMOTE_PATH, LOCAL_PATH).success

# ------------------------------------------------------------------------------
# This function stores a failure result and mirrors it to compatibility state.
#
# 1. "FAILURE_REASON" is the short result token for the failed transfer.
#
# Returns: Failed "DownloadResult" value.
# ------------------------------------------------------------------------------
    def _set_download_failure_result(self, FAILURE_REASON: str) -> DownloadResult:
        self._last_download_failure_reason = FAILURE_REASON
        return DownloadResult(False, failure_reason=FAILURE_REASON)

# ------------------------------------------------------------------------------
# This function returns a temporary sibling path for an in-progress download.
#
# 1. "LOCAL_PATH" is the final canonical destination path.
#
# Returns: Temporary sibling path used until download success is confirmed.
# ------------------------------------------------------------------------------
    def _get_temporary_download_path(self, LOCAL_PATH: Path) -> Path:
        return LOCAL_PATH.with_name(f".{LOCAL_PATH.name}.tmp")

# ------------------------------------------------------------------------------
# This function writes download content to a temporary file and counts bytes.
#
# 1. "DOWNLOAD_HANDLE" is a response-like object or byte payload.
# 2. "TEMP_PATH" is the temporary output path.
#
# Returns: Total byte count written to the temporary file.
# ------------------------------------------------------------------------------
    def _write_download_to_temp_file(self, DOWNLOAD_HANDLE: Any, TEMP_PATH: Path) -> int:
        WRITTEN_BYTES = 0

        self._cleanup_download_temp_file(TEMP_PATH)

        with TEMP_PATH.open("wb") as HANDLE:
            for CHUNK in self._iter_download_chunks(DOWNLOAD_HANDLE):
                if not CHUNK:
                    continue

                HANDLE.write(CHUNK)
                WRITTEN_BYTES += len(CHUNK)

        return WRITTEN_BYTES

# ------------------------------------------------------------------------------
# This function validates the written byte count against expected asset size.
#
# 1. "WRITTEN_BYTES" is the completed temporary-file byte count.
# 2. "EXPECTED_SIZE" is the declared remote asset size.
#
# Returns: Failure reason token, or an empty string on success.
# ------------------------------------------------------------------------------
    def _validate_download_size(self, WRITTEN_BYTES: int, EXPECTED_SIZE: int) -> str:
        if WRITTEN_BYTES == 0:
            return "empty_download"

        if EXPECTED_SIZE > 0 and WRITTEN_BYTES != EXPECTED_SIZE:
            return "incomplete_download"

        return ""

# ------------------------------------------------------------------------------
# This function removes a temporary download file when it exists.
#
# 1. "TEMP_PATH" is the temporary output path.
#
# Returns: None.
# ------------------------------------------------------------------------------
    def _cleanup_download_temp_file(self, TEMP_PATH: Path) -> None:
        try:
            TEMP_PATH.unlink()
        except FileNotFoundError:
            return
        except OSError:
            return

# ------------------------------------------------------------------------------
# This function keeps package-style transfer API compatibility.
#
# 1. "REMOTE_PATH" is the canonical relative output path.
# 2. "LOCAL_PATH" is the full destination file path.
#
# Returns: True on success, otherwise False.
# ------------------------------------------------------------------------------
    def download_package_tree(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> bool:
        return self.download_file(REMOTE_PATH, LOCAL_PATH)

# ------------------------------------------------------------------------------
# This function returns the last download failure reason token.
#
# Returns: Short diagnostic token for sync failure reporting.
# ------------------------------------------------------------------------------
    def get_last_download_failure_reason(self) -> str:
        return self._last_download_failure_reason

# ------------------------------------------------------------------------------
# This function reads the pyicloud photos service safely.
#
# Returns: Photos service object when available, otherwise None.
# ------------------------------------------------------------------------------
    def _get_photos_service(self) -> Any | None:
        if self.api is None:
            return None

        return getattr(self.api, "photos", None)

# ------------------------------------------------------------------------------
# This function returns all assets from the main photos collection.
#
# Returns: List of remote asset objects.
# 
# N.B.
# Pyicloud Photos collection names vary across versions, so the lookup tries
# several common collection entry points before giving up.
# ------------------------------------------------------------------------------
    def _read_all_assets(self) -> list[Any]:
        PHOTOS = self._get_photos_service()

        if PHOTOS is None:
            return []

        for CANDIDATE in self._candidate_all_assets(PHOTOS):
            ASSETS = self._materialise_assets(CANDIDATE)
            if ASSETS:
                return ASSETS

        return []

# ------------------------------------------------------------------------------
# This function returns assets from "All Photos" with optional early-stop
# behaviour.
#
# 1. "MANIFEST" is previous sync metadata used to detect unchanged streaks.
#
# Returns: Ordered list of remote asset objects to normalise for this run.
# ------------------------------------------------------------------------------
    def _read_all_assets_until_found(self, MANIFEST: dict[str, dict[str, Any]]) -> list[Any]:
        PHOTOS = self._get_photos_service()

        if PHOTOS is None:
            return []

        ALL_COLLECTION = None
        THRESHOLD = self.config.backup_until_found_count

        for CANDIDATE in self._candidate_all_assets(PHOTOS):
            if CANDIDATE is not None:
                ALL_COLLECTION = CANDIDATE
                break

        if ALL_COLLECTION is None or THRESHOLD < 1:
            return self._read_all_assets()

        RESULT: list[Any] = []
        MATCHED_STREAK = 0
        ALBUM_MAP: dict[str, tuple[str, ...]] = {}

        for INDEX, ASSET in enumerate(self._materialise_asset_iterable(ALL_COLLECTION), start=1):
            RESULT.append(ASSET)
            ENTRY = self._build_remote_entry(ASSET, INDEX, ALBUM_MAP)

            if entry_matches_manifest(ENTRY, MANIFEST):
                MATCHED_STREAK += 1
                if MATCHED_STREAK >= THRESHOLD:
                    break
                continue

            MATCHED_STREAK = 0

        return RESULT

# ------------------------------------------------------------------------------
# This function returns derived album membership keyed by asset identifier.
#
# Returns: Mapping from asset identifier to sanitised album-path tuple.
# 
# N.B.
# Album membership is reduced to relative output paths here so the sync layer
# does not need to understand pyicloud album objects directly.
# ------------------------------------------------------------------------------
    def _read_album_membership(self) -> dict[str, tuple[str, ...]]:
        if not self.config.backup_albums_enabled:
            return {}

        PHOTOS = self._get_photos_service()

        if PHOTOS is None:
            return {}

        ALBUMS = self._normalise_album_mapping(getattr(PHOTOS, "albums", {}))
        MEMBERSHIP: dict[str, list[str]] = {}

        for ALBUM_NAME, ALBUM_NODE in ALBUMS.items():
            if not self._should_include_album(ALBUM_NAME):
                continue

            for ASSET in self._materialise_assets(ALBUM_NODE):
                ASSET_ID = self._asset_identifier(ASSET, 0)
                MEMBERSHIP.setdefault(ASSET_ID, []).append(self._album_relative_path(ALBUM_NAME))

        RESULT: dict[str, tuple[str, ...]] = {}

        for ASSET_ID, PATHS in MEMBERSHIP.items():
            RESULT[ASSET_ID] = tuple(sorted(set(PATHS)))

        return RESULT

# ------------------------------------------------------------------------------
# This function creates a stable remote entry from one pyicloud asset object.
#
# 1. "ASSET" is the pyicloud photo object.
# 2. "INDEX" is a one-based fallback counter used when metadata is sparse.
# 3. "ALBUM_MAP" maps asset IDs to album output paths.
#
# Returns: Normalised "RemoteEntry" for sync planning.
# 
# Behaviour notes:
# 1. The canonical path is always built under the configured library root.
# 2. Album paths are attached as derived metadata only.
# ------------------------------------------------------------------------------
    def _build_remote_entry(
        self,
        ASSET: Any,
        INDEX: int,
        ALBUM_MAP: dict[str, tuple[str, ...]],
    ) -> RemoteEntry:
        ASSET_ID = self._asset_identifier(ASSET, INDEX)
        FILE_NAME = self._asset_file_name(ASSET, ASSET_ID)
        CREATED = self._asset_created(ASSET)
        MODIFIED = self._asset_modified(ASSET, CREATED)
        SIZE = self._asset_size(ASSET)
        CANONICAL_PATH = self._canonical_relative_path(CREATED, FILE_NAME)
        ALBUM_PATHS = ALBUM_MAP.get(ASSET_ID, ())

        return RemoteEntry(
            path=CANONICAL_PATH,
            is_dir=False,
            size=SIZE,
            modified=MODIFIED,
            asset_id=ASSET_ID,
            created=CREATED,
            download_name=FILE_NAME,
            album_paths=ALBUM_PATHS,
        )

# ------------------------------------------------------------------------------
# This function returns candidate "all assets" collections from pyicloud.
#
# 1. "PHOTOS" is the pyicloud photos service object.
#
# Returns: Ordered candidate iterable objects.
# ------------------------------------------------------------------------------
    def _candidate_all_assets(self, PHOTOS: Any) -> tuple[Any, ...]:
        ALBUMS = self._normalise_album_mapping(getattr(PHOTOS, "albums", {}))
        return (
            getattr(PHOTOS, "all", None),
            getattr(PHOTOS, "all_photos", None),
            ALBUMS.get("All Photos"),
            ALBUMS.get("Recents"),
        )

# ------------------------------------------------------------------------------
# This function converts album containers into a predictable mapping.
#
# 1. "ALBUMS" is a pyicloud album container or dictionary.
#
# Returns: Dictionary keyed by album name.
# 
# N.B.
# Some pyicloud versions expose dict-like album containers rather than plain
# dictionaries, so this function flattens those interfaces early.
# ------------------------------------------------------------------------------
    def _normalise_album_mapping(self, ALBUMS: Any) -> dict[str, Any]:
        if isinstance(ALBUMS, dict):
            return ALBUMS

        if hasattr(ALBUMS, "items"):
            try:
                return dict(ALBUMS.items())
            except Exception:
                return {}

        return {}

# ------------------------------------------------------------------------------
# This function materialises pyicloud asset iterables into a list.
#
# 1. "SOURCE" is an asset collection object.
#
# Returns: List of asset objects.
# 
# N.B.
# The function intentionally swallows type mismatches because the caller uses
# an empty list as the uniform "not available" contract.
# ------------------------------------------------------------------------------
    def _materialise_assets(self, SOURCE: Any) -> list[Any]:
        if SOURCE is None:
            return []

        if isinstance(SOURCE, list):
            return SOURCE

        if isinstance(SOURCE, tuple):
            return list(SOURCE)

        try:
            return list(SOURCE)
        except TypeError:
            return []

# ------------------------------------------------------------------------------
# This function yields assets from a pyicloud collection without forcing a
# full list conversion up front.
#
# 1. "SOURCE" is an asset collection object.
#
# Returns: Iterator of asset objects.
# ------------------------------------------------------------------------------
    def _materialise_asset_iterable(self, SOURCE: Any):
        if SOURCE is None:
            return iter(())

        if isinstance(SOURCE, list):
            return iter(SOURCE)

        if isinstance(SOURCE, tuple):
            return iter(SOURCE)

        try:
            return iter(SOURCE)
        except TypeError:
            return iter(())

# ------------------------------------------------------------------------------
# This function decides whether a named album should be backed up.
#
# 1. "ALBUM_NAME" is the raw remote album name.
#
# Returns: True when album output should be generated.
# 
# N.B.
# "All Photos" is excluded because the canonical library tree already covers
# the full asset set and a duplicate album tree would add no value.
# ------------------------------------------------------------------------------
    def _should_include_album(self, ALBUM_NAME: str) -> bool:
        CLEAN_NAME = ALBUM_NAME.strip()

        if not CLEAN_NAME:
            return False

        if CLEAN_NAME == "All Photos":
            return False

        if CLEAN_NAME == "Favourites":
            return self.config.backup_include_favourites

        if CLEAN_NAME == "Favorites":
            return self.config.backup_include_favourites

        if CLEAN_NAME == "Shared":
            return self.config.backup_include_shared_albums

        return True

# ------------------------------------------------------------------------------
# This function returns the stable identifier for one asset.
#
# 1. "ASSET" is the pyicloud asset object.
# 2. "INDEX" is fallback counter when no obvious asset identifier exists.
#
# Returns: Stable identifier string.
# 
# N.B.
# When pyicloud does not expose a durable remote identifier, the fallback hash
# is only intended to keep one local run stable enough for manifesting.
# ------------------------------------------------------------------------------
    def _asset_identifier(self, ASSET: Any, INDEX: int) -> str:
        for ATTRIBUTE in ("id", "photo_guid", "guid", "record_name", "recordName"):
            VALUE = getattr(ASSET, ATTRIBUTE, "")
            TEXT = str(VALUE).strip()
            if TEXT:
                return TEXT

        DIGEST_SOURCE = "|".join(
            [
                self._asset_file_name(ASSET, f"asset-{INDEX:06d}"),
                self._asset_created(ASSET),
                self._asset_modified(ASSET, self._asset_created(ASSET)),
                str(self._asset_size(ASSET)),
                str(INDEX),
            ]
        )
        return hashlib.sha1(DIGEST_SOURCE.encode("utf-8")).hexdigest()

# ------------------------------------------------------------------------------
# This function returns a stable filename for one asset.
#
# 1. "ASSET" is the pyicloud asset object.
# 2. "ASSET_ID" is the stable asset identifier.
#
# Returns: Sanitised filename.
# 
# N.B.
# The fallback name keeps a common image suffix so exported files remain easy
# to inspect even when the remote metadata is sparse.
# ------------------------------------------------------------------------------
    def _asset_file_name(self, ASSET: Any, ASSET_ID: str) -> str:
        for ATTRIBUTE in ("filename", "name"):
            VALUE = getattr(ASSET, ATTRIBUTE, "")
            TEXT = self._sanitize_file_name(str(VALUE).strip())
            if TEXT:
                return TEXT

        return f"{ASSET_ID}.jpg"

# ------------------------------------------------------------------------------
# This function sanitises user-visible filenames for filesystem storage.
#
# 1. "NAME" is the source filename.
#
# Returns: Safe filename string.
# 
# N.B.
# This sanitiser is intentionally conservative and filesystem-oriented. It does
# not try to preserve every original Unicode code point.
# ------------------------------------------------------------------------------
    def _sanitize_file_name(self, NAME: str) -> str:
        CLEAN_NAME = FILE_NAME_SANITISE_PATTERN.sub("_", NAME).strip(" .")

        if CLEAN_NAME:
            return CLEAN_NAME

        return "asset.jpg"

# ------------------------------------------------------------------------------
# This function returns the asset creation timestamp as an ISO string.
#
# 1. "ASSET" is the pyicloud asset object.
#
# Returns: ISO timestamp string.
# ------------------------------------------------------------------------------
    def _asset_created(self, ASSET: Any) -> str:
        for ATTRIBUTE in ("created", "created_at", "date_created"):
            VALUE = getattr(ASSET, ATTRIBUTE, None)
            TEXT = self._datetime_to_iso(VALUE)
            if TEXT:
                return TEXT

        return "1970-01-01T00:00:00+00:00"

# ------------------------------------------------------------------------------
# This function returns the asset modified timestamp as an ISO string.
#
# 1. "ASSET" is the pyicloud asset object.
# 2. "DEFAULT_VALUE" is used when no explicit modified timestamp exists.
#
# Returns: ISO timestamp string.
# ------------------------------------------------------------------------------
    def _asset_modified(self, ASSET: Any, DEFAULT_VALUE: str) -> str:
        for ATTRIBUTE in ("modified", "modified_at", "date_modified"):
            VALUE = getattr(ASSET, ATTRIBUTE, None)
            TEXT = self._datetime_to_iso(VALUE)
            if TEXT:
                return TEXT

        return DEFAULT_VALUE

# ------------------------------------------------------------------------------
# This function converts a datetime-like object into ISO text.
#
# 1. "VALUE" is a datetime-like object or string.
#
# Returns: ISO string when conversion is possible, otherwise empty string.
# ------------------------------------------------------------------------------
    def _datetime_to_iso(self, VALUE: Any) -> str:
        if VALUE is None:
            return ""

        if hasattr(VALUE, "isoformat"):
            try:
                return str(VALUE.isoformat())
            except Exception:
                return ""

        TEXT = str(VALUE).strip()

        if TEXT:
            return TEXT

        return ""

# ------------------------------------------------------------------------------
# This function returns the declared asset size in bytes.
#
# 1. "ASSET" is the pyicloud asset object.
#
# Returns: Non-negative byte count.
# ------------------------------------------------------------------------------
    def _asset_size(self, ASSET: Any) -> int:
        for ATTRIBUTE in ("size", "file_size", "item_size"):
            VALUE = getattr(ASSET, ATTRIBUTE, None)
            if isinstance(VALUE, int) and VALUE >= 0:
                return VALUE

        return 0

# ------------------------------------------------------------------------------
# This function builds the canonical year/month/day output path.
#
# 1. "CREATED" is ISO creation timestamp.
# 2. "FILE_NAME" is the source filename.
#
# Returns: Relative output path under the library root.
# ------------------------------------------------------------------------------
    def _canonical_relative_path(self, CREATED: str, FILE_NAME: str) -> str:
        DATE_TEXT = CREATED.split("T", maxsplit=1)[0]
        YEAR_TEXT, MONTH_TEXT, DAY_TEXT = self._safe_date_parts(DATE_TEXT)
        return "/".join([self.config.backup_root_library, YEAR_TEXT, MONTH_TEXT, DAY_TEXT, FILE_NAME])

# ------------------------------------------------------------------------------
# This function returns safe date parts from an ISO date string.
#
# 1. "DATE_TEXT" is a "YYYY-MM-DD" date-like string.
#
# Returns: Tuple "(year, month, day)".
# ------------------------------------------------------------------------------
    def _safe_date_parts(self, DATE_TEXT: str) -> tuple[str, str, str]:
        PARTS = DATE_TEXT.split("-")

        if len(PARTS) != 3:
            return ("1970", "01", "01")

        YEAR_TEXT, MONTH_TEXT, DAY_TEXT = PARTS

        if not YEAR_TEXT.isdigit():
            YEAR_TEXT = "1970"

        if not MONTH_TEXT.isdigit():
            MONTH_TEXT = "01"

        if not DAY_TEXT.isdigit():
            DAY_TEXT = "01"

        return (YEAR_TEXT.zfill(4), MONTH_TEXT.zfill(2), DAY_TEXT.zfill(2))

# ------------------------------------------------------------------------------
# This function converts an album name into a relative album path.
#
# 1. "ALBUM_NAME" is the source album name.
#
# Returns: Relative output path under the albums root.
# ------------------------------------------------------------------------------
    def _album_relative_path(self, ALBUM_NAME: str) -> str:
        SAFE_NAME = self._sanitize_file_name(ALBUM_NAME).replace("/", "_")
        return "/".join([self.config.backup_root_albums, SAFE_NAME])

# ------------------------------------------------------------------------------
# This function resolves one asset object from a canonical remote path.
#
# 1. "REMOTE_PATH" is the canonical output path generated by this worker.
#
# Returns: Asset object when found, otherwise None.
# 
# N.B.
# This is a linear lookup over the current remote asset list. That is
# acceptable for now because it keeps the client logic simple.
# ------------------------------------------------------------------------------
    def _get_asset_by_remote_path(self, REMOTE_PATH: str) -> Any | None:
        if REMOTE_PATH in self._cached_assets_by_path:
            return self._cached_assets_by_path[REMOTE_PATH]

        self._refresh_listing_cache()
        return self._cached_assets_by_path.get(REMOTE_PATH)

# ------------------------------------------------------------------------------
# This function opens an asset download handle with broad pyicloud
# compatibility.
#
# 1. "ASSET" is the pyicloud asset object.
#
# Returns: Download handle or response object, otherwise None.
# 
# N.B.
# Pyicloud response methods are inconsistent across versions, so this probes a
# few common call shapes before declaring the asset undownloadable.
# ------------------------------------------------------------------------------
    def _open_asset_download(self, ASSET: Any) -> Any | None:
        for METHOD_NAME in ("download", "open", "download_original"):
            METHOD = getattr(ASSET, METHOD_NAME, None)
            if METHOD is None:
                continue

            try:
                return METHOD()
            except TypeError:
                try:
                    return METHOD(stream=True)
                except Exception:
                    continue
            except Exception:
                continue

        return None

# ------------------------------------------------------------------------------
# This function yields bytes from a pyicloud download handle.
#
# 1. "DOWNLOAD_HANDLE" is a response-like object or byte payload.
#
# Returns: Iterator of byte chunks.
# 
# Failure behaviour:
# 1. Supports several response shapes used by pyicloud and requests objects.
# 2. Raises when no supported payload interface exists.
# ------------------------------------------------------------------------------
    def _iter_download_chunks(self, DOWNLOAD_HANDLE: Any):
        if isinstance(DOWNLOAD_HANDLE, bytes):
            yield DOWNLOAD_HANDLE
            return

        if hasattr(DOWNLOAD_HANDLE, "iter_content"):
            for CHUNK in DOWNLOAD_HANDLE.iter_content(
                chunk_size=max(self.config.download_chunk_mib, 1) * 1024 * 1024
            ):
                yield CHUNK
            return

        RAW_STREAM = getattr(DOWNLOAD_HANDLE, "raw", None)

        if RAW_STREAM is not None:
            while True:
                CHUNK = RAW_STREAM.read(max(self.config.download_chunk_mib, 1) * 1024 * 1024)
                if not CHUNK:
                    break
                yield CHUNK
            return

        CONTENT = getattr(DOWNLOAD_HANDLE, "content", None)

        if isinstance(CONTENT, bytes):
            yield CONTENT
            return

        if hasattr(DOWNLOAD_HANDLE, "read"):
            while True:
                CHUNK = DOWNLOAD_HANDLE.read(max(self.config.download_chunk_mib, 1) * 1024 * 1024)
                if not CHUNK:
                    break
                yield CHUNK
            return

        raise UnsupportedDownloadHandleError()
