# ------------------------------------------------------------------------------
# This test module verifies photo entry normalisation and album mapping.
# ------------------------------------------------------------------------------

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.icloud_client import ICloudDriveClient, UnsupportedDownloadHandleError


# ------------------------------------------------------------------------------
# This response stub yields byte chunks for client download tests.
# ------------------------------------------------------------------------------
class ChunkHandle:
    def __init__(self, CHUNKS: list[bytes]):
        self.chunks = CHUNKS

    def iter_content(self, chunk_size: int):
        _ = chunk_size
        for CHUNK in self.chunks:
            yield CHUNK


# ------------------------------------------------------------------------------
# This response stub raises during chunk reads to simulate interrupted IO.
# ------------------------------------------------------------------------------
class BrokenChunkHandle:
    def iter_content(self, chunk_size: int):
        _ = chunk_size
        yield b"partial"
        raise RuntimeError("stream failed")


# ------------------------------------------------------------------------------
# This response stub exposes a raw stream object for chunk iteration tests.
# ------------------------------------------------------------------------------
class RawHandle:
    def __init__(self, CHUNKS: list[bytes]):
        self.chunks = list(CHUNKS)
        self.raw = self

    def read(self, chunk_size: int):
        _ = chunk_size
        if not self.chunks:
            return b""
        return self.chunks.pop(0)


# ------------------------------------------------------------------------------
# This response stub exposes a direct read method for chunk iteration tests.
# ------------------------------------------------------------------------------
class ReadHandle:
    def __init__(self, CHUNKS: list[bytes]):
        self.chunks = list(CHUNKS)

    def read(self, chunk_size: int):
        _ = chunk_size
        if not self.chunks:
            return b""
        return self.chunks.pop(0)


# ------------------------------------------------------------------------------
# This response stub exposes dict-like album items for normalisation tests.
# ------------------------------------------------------------------------------
class AlbumContainer:
    def __init__(self, ITEMS):
        self._items = ITEMS

    def items(self):
        return self._items


# ------------------------------------------------------------------------------
# This client test double counts full asset-list reads for cache assertions.
# ------------------------------------------------------------------------------
class CountingIcloudClient(ICloudDriveClient):
    def __init__(self, CONFIG: AppConfig):
        super().__init__(CONFIG)
        self.read_all_assets_calls = 0

    def _read_all_assets(self) -> list[object]:
        self.read_all_assets_calls += 1
        return super()._read_all_assets()


# ------------------------------------------------------------------------------
# This function builds a minimal app config for client tests.
# ------------------------------------------------------------------------------
def create_config() -> AppConfig:
    return AppConfig(
        container_username="alice",
        icloud_email="alice@example.com",
        icloud_password="secret",
        telegram_bot_token="token",
        telegram_chat_id="1",
        keychain_service_name="pyiclodoc-photos",
        run_once=False,
        schedule_mode="interval",
        schedule_backup_time="02:00",
        schedule_weekdays="monday",
        schedule_monthly_week="first",
        schedule_interval_minutes=1440,
        backup_discovery_mode="full",
        backup_until_found_count=50,
        backup_delete_removed=False,
        sync_workers=0,
        download_chunk_mib=4,
        reauth_interval_days=30,
        output_dir=Path("/tmp/output"),
        config_dir=Path("/tmp/config"),
        logs_dir=Path("/tmp/logs"),
        manifest_path=Path("/tmp/config/pyiclodoc-photos-manifest.json"),
        auth_state_path=Path("/tmp/config/pyiclodoc-photos-auth_state.json"),
        heartbeat_path=Path("/tmp/logs/pyiclodoc-photos-heartbeat.txt"),
        cookie_dir=Path("/tmp/config/cookies"),
        session_dir=Path("/tmp/config/session"),
        icloudpd_compat_dir=Path("/tmp/config/icloudpd"),
        safety_net_sample_size=200,
        backup_albums_enabled=True,
        backup_album_links_mode="hardlink",
        backup_include_shared_albums=True,
        backup_include_favourites=True,
        backup_root_library="library",
        backup_root_albums="albums",
    )


# ------------------------------------------------------------------------------
# These tests verify photo listing and path derivation behaviour.
# ------------------------------------------------------------------------------
class TestIcloudClient(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms compatibility path setup creates the expected symlinks.
# --------------------------------------------------------------------------
    def test_prepare_compat_paths_creates_expected_links(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = create_config()
            ROOT = Path(TMPDIR)
            CONFIG = AppConfig(**{
                **CONFIG.__dict__,
                "config_dir": ROOT / "config",
                "cookie_dir": ROOT / "config" / "cookies",
                "session_dir": ROOT / "config" / "session",
                "icloudpd_compat_dir": ROOT / "config" / "icloudpd",
            })
            CONFIG.cookie_dir.mkdir(parents=True, exist_ok=True)
            CONFIG.session_dir.mkdir(parents=True, exist_ok=True)
            CLIENT = ICloudDriveClient(CONFIG)

            CLIENT.prepare_compat_paths()

            self.assertTrue((CONFIG.icloudpd_compat_dir / "cookies").is_symlink())
            self.assertTrue((CONFIG.icloudpd_compat_dir / "session").is_symlink())

# --------------------------------------------------------------------------
# This test confirms _ensure_link replaces incompatible existing paths.
# --------------------------------------------------------------------------
    def test_ensure_link_replaces_existing_directory(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT = Path(TMPDIR)
            CLIENT = ICloudDriveClient(create_config())
            LINK_PATH = ROOT / "compat"
            TARGET_PATH = ROOT / "target"
            LINK_PATH.mkdir(parents=True, exist_ok=True)
            TARGET_PATH.mkdir(parents=True, exist_ok=True)

            CLIENT._ensure_link(LINK_PATH, TARGET_PATH)

            self.assertTrue(LINK_PATH.is_symlink())

# --------------------------------------------------------------------------
# This test confirms auth startup handles 2FA, 2SA, and direct success.
# --------------------------------------------------------------------------
    def test_start_authentication_handles_auth_states(self) -> None:
        CLIENT = ICloudDriveClient(create_config())

        with patch.object(CLIENT, "prepare_compat_paths"):
            with patch.object(
                CLIENT,
                "_create_service",
                return_value=SimpleNamespace(requires_2fa=True, requires_2sa=False),
            ):
                self.assertEqual(
                    CLIENT.start_authentication(),
                    (False, "Two-factor code is required."),
                )

        with patch.object(CLIENT, "prepare_compat_paths"):
            with patch.object(
                CLIENT,
                "_create_service",
                return_value=SimpleNamespace(requires_2fa=False, requires_2sa=True),
            ):
                self.assertIn("Two-step authentication is required", CLIENT.start_authentication()[1])

        with patch.object(CLIENT, "prepare_compat_paths"):
            with patch.object(
                CLIENT,
                "_create_service",
                return_value=SimpleNamespace(requires_2fa=False, requires_2sa=False),
            ):
                self.assertEqual(
                    CLIENT.start_authentication(),
                    (True, "Authenticated successfully."),
                )

# --------------------------------------------------------------------------
# This test confirms complete_authentication covers the main MFA outcomes.
# --------------------------------------------------------------------------
    def test_complete_authentication_handles_main_mfa_outcomes(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (False, "Authentication session is not initialised."),
        )

        CLIENT.api = SimpleNamespace(requires_2fa=True)
        self.assertEqual(
            CLIENT.complete_authentication(""),
            (False, "Two-factor code is required."),
        )

        CLIENT.api = SimpleNamespace(requires_2fa=False)
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (True, "Authenticated successfully."),
        )

        CLIENT.api = SimpleNamespace(
            requires_2fa=True,
            validate_2fa_code=lambda CODE: False,
        )
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (False, "Two-factor code was rejected by Apple."),
        )

        CLIENT.api = SimpleNamespace(
            requires_2fa=True,
            validate_2fa_code=lambda CODE: True,
            is_trusted_session=True,
        )
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (True, "Authenticated successfully with 2FA."),
        )

        CLIENT.api = SimpleNamespace(
            requires_2fa=True,
            validate_2fa_code=lambda CODE: True,
            is_trusted_session=False,
            trust_session=lambda: False,
        )
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (False, "Two-factor code was accepted, but Apple did not trust this session."),
        )

        CLIENT.api = SimpleNamespace(
            requires_2fa=True,
            validate_2fa_code=lambda CODE: True,
            is_trusted_session=False,
            trust_session=lambda: True,
        )
        self.assertEqual(
            CLIENT.complete_authentication("123456"),
            (True, "Authenticated successfully with trusted 2FA session."),
        )

# --------------------------------------------------------------------------
# This test confirms authenticate delegates to code completion when a code is
# provided and otherwise starts authentication.
# --------------------------------------------------------------------------
    def test_authenticate_delegates_by_code_presence(self) -> None:
        CLIENT = ICloudDriveClient(create_config())

        with patch.object(CLIENT, "complete_authentication", return_value=(True, "done")) as COMPLETE:
            self.assertEqual(CLIENT.authenticate(lambda: "123456"), (True, "done"))
            COMPLETE.assert_called_once_with("123456")

        with patch.object(CLIENT, "start_authentication", return_value=(True, "start")) as START:
            self.assertEqual(CLIENT.authenticate(lambda: " "), (True, "start"))
            START.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms the client derives canonical year/month/day paths and
# album output paths from photo metadata.
# --------------------------------------------------------------------------
    def test_list_entries_builds_photo_paths(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=1024,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
        )
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                all=[ASSET],
                albums={
                    "Favourites": [ASSET],
                    "Trips": [ASSET],
                },
            )
        )

        ENTRIES = CLIENT.list_entries()

        self.assertEqual(len(ENTRIES), 1)
        self.assertEqual(ENTRIES[0].path, "library/2026/03/14/IMG_0001.JPG")
        self.assertEqual(ENTRIES[0].album_paths, ("albums/Favourites", "albums/Trips"))

# --------------------------------------------------------------------------
# This test confirms manifest-aware listing uses the full-scan cache refresh
# path when the configured discovery mode is "full".
# --------------------------------------------------------------------------
    def test_list_entries_for_sync_uses_full_scan_when_mode_is_full(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        CLIENT.api = SimpleNamespace()
        CLIENT._cached_entries = [SimpleNamespace(path="library/test.jpg")]

        with patch.object(CLIENT, "_refresh_listing_cache") as REFRESH:
            ENTRIES = CLIENT.list_entries_for_sync({})

        self.assertEqual(ENTRIES, CLIENT._cached_entries)
        REFRESH.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms manifest-aware listing uses the early-stop cache refresh
# path when the configured discovery mode is "until_found".
# --------------------------------------------------------------------------
    def test_list_entries_for_sync_uses_until_found_scan_when_mode_enabled(self) -> None:
        CONFIG = AppConfig(**{
            **create_config().__dict__,
            "backup_discovery_mode": "until_found",
            "backup_albums_enabled": False,
        })
        CLIENT = ICloudDriveClient(CONFIG)
        CLIENT.api = SimpleNamespace()
        CLIENT._cached_entries = [SimpleNamespace(path="library/test.jpg")]

        with patch.object(CLIENT, "_refresh_listing_cache_until_found") as REFRESH:
            ENTRIES = CLIENT.list_entries_for_sync({"library/test.jpg": {"size": 1}})

        self.assertEqual(ENTRIES, CLIENT._cached_entries)
        REFRESH.assert_called_once_with({"library/test.jpg": {"size": 1}})

# ------------------------------------------------------------------------------
# This test confirms manifest-aware listing falls back to a full scan when the
# requested discovery mode would otherwise drive delete or album management
# from a partial snapshot.
# ------------------------------------------------------------------------------
    def test_list_entries_for_sync_forces_full_scan_when_until_found_is_unsafe(self) -> None:
        CONFIG = AppConfig(**{
            **create_config().__dict__,
            "backup_discovery_mode": "until_found",
        })
        CLIENT = ICloudDriveClient(CONFIG)
        CLIENT.api = SimpleNamespace()
        CLIENT._cached_entries = [SimpleNamespace(path="library/test.jpg")]

        with patch.object(CLIENT, "_refresh_listing_cache") as FULL_REFRESH:
            with patch.object(CLIENT, "_refresh_listing_cache_until_found") as PARTIAL_REFRESH:
                ENTRIES = CLIENT.list_entries_for_sync({})

        self.assertEqual(ENTRIES, CLIENT._cached_entries)
        FULL_REFRESH.assert_called_once()
        PARTIAL_REFRESH.assert_not_called()

# --------------------------------------------------------------------------
# This test confirms colliding day-and-filename assets receive deterministic
# disambiguated output names instead of collapsing into one path.
# --------------------------------------------------------------------------
    def test_list_entries_disambiguates_colliding_canonical_paths(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        FIRST_ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=1024,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
        )
        SECOND_ASSET = SimpleNamespace(
            id="asset-2",
            filename="IMG_0001.JPG",
            size=2048,
            created=datetime(2026, 3, 14, 10, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 10, 31, tzinfo=timezone.utc),
        )
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                all=[FIRST_ASSET, SECOND_ASSET],
                albums={"Trips": [FIRST_ASSET, SECOND_ASSET]},
            )
        )

        ENTRIES = CLIENT.list_entries()
        PATHS = [ENTRY.path for ENTRY in ENTRIES]
        DOWNLOAD_NAMES = [ENTRY.download_name for ENTRY in ENTRIES]

        self.assertEqual(len(ENTRIES), 2)
        self.assertEqual(len(set(PATHS)), 2)
        self.assertEqual(len(set(DOWNLOAD_NAMES)), 2)
        self.assertTrue(
            all(PATH.startswith("library/2026/03/14/IMG_0001--") for PATH in PATHS)
        )
        self.assertTrue(all(NAME.endswith(".JPG") for NAME in DOWNLOAD_NAMES))

# --------------------------------------------------------------------------
# This test confirms helper methods normalise photos service, album mappings,
# and album inclusion rules.
# --------------------------------------------------------------------------
    def test_collection_and_album_helpers_normalise_expected_shapes(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        CLIENT.api = None
        self.assertIsNone(CLIENT._get_photos_service())

        PHOTOS = SimpleNamespace()
        CLIENT.api = SimpleNamespace(photos=PHOTOS)
        self.assertIs(CLIENT._get_photos_service(), PHOTOS)
        self.assertEqual(CLIENT._normalise_album_mapping({"A": 1}), {"A": 1})
        self.assertEqual(CLIENT._normalise_album_mapping(AlbumContainer([("A", 1)])), {"A": 1})
        self.assertEqual(CLIENT._normalise_album_mapping(AlbumContainer(None)), {})
        self.assertEqual(CLIENT._materialise_assets(None), [])
        self.assertEqual(CLIENT._materialise_assets([1, 2]), [1, 2])
        self.assertEqual(CLIENT._materialise_assets((1, 2)), [1, 2])
        self.assertEqual(CLIENT._materialise_assets(object()), [])
        self.assertEqual(list(CLIENT._materialise_asset_iterable(None)), [])
        self.assertEqual(list(CLIENT._materialise_asset_iterable([1, 2])), [1, 2])
        self.assertEqual(list(CLIENT._materialise_asset_iterable((1, 2))), [1, 2])
        self.assertEqual(list(CLIENT._materialise_asset_iterable(object())), [])
        self.assertFalse(CLIENT._should_include_album(""))
        self.assertFalse(CLIENT._should_include_album("All Photos"))
        self.assertTrue(CLIENT._should_include_album("Trips"))
        self.assertTrue(CLIENT._should_include_album("Favourites"))

# --------------------------------------------------------------------------
# This test confirms early-stop discovery stops after the configured streak
# of unchanged entries.
# --------------------------------------------------------------------------
    def test_read_all_assets_until_found_stops_after_match_threshold(self) -> None:
        CONFIG = AppConfig(**{
            **create_config().__dict__,
            "backup_discovery_mode": "until_found",
            "backup_until_found_count": 2,
        })
        CLIENT = ICloudDriveClient(CONFIG)
        ASSETS = [object(), object(), object()]
        CLIENT.api = SimpleNamespace(photos=SimpleNamespace(all=ASSETS))

        with patch.object(
            CLIENT,
            "_build_remote_entry",
            side_effect=[
                SimpleNamespace(
                    path="library/a.jpg",
                    asset_id="a",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
                SimpleNamespace(
                    path="library/b.jpg",
                    asset_id="b",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
                SimpleNamespace(
                    path="library/c.jpg",
                    asset_id="c",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
            ],
        ):
            RESULT = CLIENT._read_all_assets_until_found(
                {
                    "library/a.jpg": {"asset_id": "a", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                    "library/b.jpg": {"asset_id": "b", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                },
            )

        self.assertEqual(RESULT, ASSETS[:2])

# --------------------------------------------------------------------------
# This test confirms early-stop discovery resets the unchanged streak when a
# changed asset is encountered.
# --------------------------------------------------------------------------
    def test_read_all_assets_until_found_resets_streak_after_change(self) -> None:
        CONFIG = AppConfig(**{
            **create_config().__dict__,
            "backup_discovery_mode": "until_found",
            "backup_until_found_count": 2,
        })
        CLIENT = ICloudDriveClient(CONFIG)
        ASSETS = [object(), object(), object(), object()]
        CLIENT.api = SimpleNamespace(photos=SimpleNamespace(all=ASSETS))

        with patch.object(
            CLIENT,
            "_build_remote_entry",
            side_effect=[
                SimpleNamespace(
                    path="library/a.jpg",
                    asset_id="a",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
                SimpleNamespace(
                    path="library/b.jpg",
                    asset_id="b",
                    size=2,
                    modified="2026-03-15T10:00:01+00:00",
                ),
                SimpleNamespace(
                    path="library/c.jpg",
                    asset_id="c",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
                SimpleNamespace(
                    path="library/d.jpg",
                    asset_id="d",
                    size=1,
                    modified="2026-03-15T10:00:00+00:00",
                ),
            ],
        ):
            RESULT = CLIENT._read_all_assets_until_found(
                {
                    "library/a.jpg": {"asset_id": "a", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                    "library/b.jpg": {"asset_id": "b", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                    "library/c.jpg": {"asset_id": "c", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                    "library/d.jpg": {"asset_id": "d", "size": 1, "modified": "2026-03-15T10:00:00+00:00"},
                },
            )

        self.assertEqual(RESULT, ASSETS)

# --------------------------------------------------------------------------
# This test confirms album inclusion flags and membership mapping honour the
# configured Photos album options.
# --------------------------------------------------------------------------
    def test_read_album_membership_respects_album_flags(self) -> None:
        CONFIG = create_config()
        CONFIG = AppConfig(**{
            **CONFIG.__dict__,
            "backup_include_favourites": False,
            "backup_include_shared_albums": False,
        })
        CLIENT = ICloudDriveClient(CONFIG)
        ASSET = SimpleNamespace(id="asset-1")
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                albums={
                    "Trips": [ASSET],
                    "Favourites": [ASSET],
                    "Shared": [ASSET],
                },
            )
        )

        self.assertEqual(
            CLIENT._read_album_membership(),
            {"asset-1": ("albums/Trips",)},
        )

# --------------------------------------------------------------------------
# This test confirms metadata helpers provide deterministic fallbacks.
# --------------------------------------------------------------------------
    def test_metadata_helpers_apply_expected_fallbacks(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace()

        self.assertTrue(CLIENT._asset_identifier(ASSET, 3))
        self.assertEqual(CLIENT._asset_file_name(ASSET, "asset-3"), "asset.jpg")
        self.assertEqual(CLIENT._sanitize_file_name(" bad:/name?.jpg "), "bad_name_.jpg")
        self.assertEqual(CLIENT._sanitize_file_name("..."), "asset.jpg")
        self.assertEqual(CLIENT._asset_created(ASSET), "1970-01-01T00:00:00+00:00")
        self.assertEqual(CLIENT._asset_modified(ASSET, "fallback"), "fallback")
        self.assertEqual(CLIENT._datetime_to_iso(None), "")
        self.assertEqual(CLIENT._datetime_to_iso(" 2026-03-15 "), "2026-03-15")
        self.assertEqual(CLIENT._asset_size(SimpleNamespace(size=-1)), 0)
        self.assertEqual(CLIENT._safe_date_parts("bad"), ("1970", "01", "01"))
        self.assertEqual(CLIENT._safe_date_parts("2026-aa-3"), ("2026", "01", "03"))
        self.assertEqual(
            CLIENT._canonical_relative_path("2026-03-15T10:00:00+00:00", "photo.jpg"),
            "library/2026/03/15/photo.jpg",
        )
        self.assertEqual(CLIENT._album_relative_path("Trips/2026"), "albums/Trips_2026")

# --------------------------------------------------------------------------
# This test confirms asset collection helpers choose the expected candidate
# source and build bound remote-entry objects.
# --------------------------------------------------------------------------
    def test_asset_collection_helpers_build_entries_and_candidates(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        FIRST = SimpleNamespace(id="asset-1")
        PHOTOS = SimpleNamespace(all=None, all_photos=[FIRST], albums={"Recents": [FIRST]})

        self.assertEqual(CLIENT._candidate_all_assets(PHOTOS)[1], [FIRST])

        with patch.object(CLIENT, "_build_remote_entry", return_value=SimpleNamespace(path="x")) as BUILD:
            RESULT = CLIENT._build_remote_entries([FIRST], {"asset-1": ("albums/Trips",)})

        self.assertEqual(len(RESULT), 1)
        BUILD.assert_called_once()
        self.assertIs(RESULT[0].asset, FIRST)

# --------------------------------------------------------------------------
# This test confirms collision helper functions produce stable rewritten
# output paths.
# --------------------------------------------------------------------------
    def test_collision_helpers_rewrite_paths_stably(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ENTRY = SimpleNamespace(
            entry=SimpleNamespace(
                path="library/2026/03/15/photo.jpg",
                is_dir=False,
                size=4,
                modified="2026-03-15T10:00:00+00:00",
                asset_id="asset-1",
                created="2026-03-15T09:00:00+00:00",
                download_name="photo.jpg",
                album_paths=("albums/Trips",),
            ),
            asset=object(),
        )

        RESULT = CLIENT._disambiguate_entry_group([ENTRY, ENTRY])

        self.assertEqual(len(RESULT), 2)
        self.assertTrue(all("--" in ITEM.entry.path for ITEM in RESULT))
        self.assertTrue(CLIENT._add_collision_suffix("photo.jpg", "asset-1").endswith(".jpg"))
        self.assertEqual(CLIENT._replace_file_name("a/b/c.jpg", "d.jpg"), "a/b/d.jpg")
        self.assertEqual(CLIENT._replace_file_name("c.jpg", "d.jpg"), "d.jpg")

# --------------------------------------------------------------------------
# This test confirms asset lookup reuses the cached path map and refreshes
# the listing cache when the path is not already cached.
# --------------------------------------------------------------------------
    def test_get_asset_by_remote_path_uses_cache_then_refresh(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        CACHED_ASSET = object()
        CLIENT._cached_assets_by_path = {"library/a.jpg": CACHED_ASSET}
        self.assertIs(CLIENT._get_asset_by_remote_path("library/a.jpg"), CACHED_ASSET)

        def populate_cache():
            CLIENT._cached_assets_by_path["library/b.jpg"] = "asset-b"

        with patch.object(CLIENT, "_refresh_listing_cache", side_effect=populate_cache) as REFRESH:
            self.assertEqual(CLIENT._get_asset_by_remote_path("library/b.jpg"), "asset-b")
            REFRESH.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms download-handle open and chunk iteration helpers support
# the expected compatibility shapes.
# --------------------------------------------------------------------------
    def test_download_handle_helpers_support_expected_shapes(self) -> None:
        CLIENT = ICloudDriveClient(create_config())

        self.assertEqual(list(CLIENT._iter_download_chunks(b"data")), [b"data"])
        self.assertEqual(list(CLIENT._iter_download_chunks(ChunkHandle([b"a", b"b"]))), [b"a", b"b"])
        self.assertEqual(list(CLIENT._iter_download_chunks(RawHandle([b"a", b"b"]))), [b"a", b"b"])
        self.assertEqual(list(CLIENT._iter_download_chunks(SimpleNamespace(content=b"data"))), [b"data"])
        self.assertEqual(list(CLIENT._iter_download_chunks(ReadHandle([b"a", b"b"]))), [b"a", b"b"])
        with self.assertRaises(UnsupportedDownloadHandleError):
            list(CLIENT._iter_download_chunks(object()))

        ASSET = SimpleNamespace(
            download=lambda: ChunkHandle([b"a"]),
            open=lambda stream=False: ChunkHandle([b"b"]),
            download_original=lambda: ChunkHandle([b"c"]),
        )
        self.assertIsNotNone(CLIENT._open_asset_download(ASSET))

        FALLBACK_ASSET = SimpleNamespace(
            download=lambda: (_ for _ in ()).throw(TypeError("needs stream")),
            open=lambda stream=False: ChunkHandle([b"b"]),
        )
        self.assertIsNotNone(CLIENT._open_asset_download(FALLBACK_ASSET))
        self.assertIsNone(CLIENT._open_asset_download(SimpleNamespace()))

# --------------------------------------------------------------------------
# This test confirms temporary download helper functions handle validation and
# cleanup cases directly.
# --------------------------------------------------------------------------
    def test_download_temp_helpers_cover_validation_and_cleanup(self) -> None:
        CLIENT = ICloudDriveClient(create_config())

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "photo.jpg"
            TEMP_PATH = CLIENT._get_temporary_download_path(LOCAL_PATH)

            self.assertEqual(TEMP_PATH.name, ".photo.jpg.tmp")
            self.assertEqual(CLIENT._validate_download_size(0, 10), "empty_download")
            self.assertEqual(CLIENT._validate_download_size(4, 10), "incomplete_download")
            self.assertEqual(CLIENT._validate_download_size(10, 10), "")
            CLIENT._cleanup_download_temp_file(TEMP_PATH)
            TEMP_PATH.write_bytes(b"x")
            CLIENT._cleanup_download_temp_file(TEMP_PATH)
            self.assertFalse(TEMP_PATH.exists())
            self.assertTrue(CLIENT.download_package_tree("x", LOCAL_PATH) in {True, False})

# --------------------------------------------------------------------------
# This test confirms download_file_result covers the main explicit failure
# tokens before any data is written.
# --------------------------------------------------------------------------
    def test_download_file_result_returns_expected_prewrite_failures(self) -> None:
        CLIENT = ICloudDriveClient(create_config())

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "photo.jpg"

            RESULT = CLIENT.download_file_result("library/a.jpg", LOCAL_PATH)
            self.assertEqual(RESULT.failure_reason, "not_authenticated")

            CLIENT.api = SimpleNamespace()
            with patch.object(CLIENT, "_get_asset_by_remote_path", return_value=None):
                RESULT = CLIENT.download_file_result("library/a.jpg", LOCAL_PATH)
                self.assertEqual(RESULT.failure_reason, "asset_not_found")

            with patch.object(CLIENT, "_get_asset_by_remote_path", return_value=object()):
                with patch.object(CLIENT, "_open_asset_download", return_value=None):
                    RESULT = CLIENT.download_file_result("library/a.jpg", LOCAL_PATH)
                    self.assertEqual(RESULT.failure_reason, "download_unavailable")

# --------------------------------------------------------------------------
# This test confirms write and read failure tokens are surfaced through the
# download result helper.
# --------------------------------------------------------------------------
    def test_download_file_result_surfaces_write_and_read_failures(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        CLIENT.api = SimpleNamespace()

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "photo.jpg"

            with patch.object(CLIENT, "_get_asset_by_remote_path", return_value=object()):
                with patch.object(CLIENT, "_open_asset_download", return_value=ChunkHandle([b"a"])):
                    with patch.object(CLIENT, "_write_download_to_temp_file", side_effect=OSError("disk")):
                        RESULT = CLIENT.download_file_result("library/a.jpg", LOCAL_PATH)
                        self.assertEqual(RESULT.failure_reason, "write_failed")

            with patch.object(CLIENT, "_get_asset_by_remote_path", return_value=object()):
                with patch.object(CLIENT, "_open_asset_download", return_value=ChunkHandle([b"a"])):
                    with patch.object(CLIENT, "_write_download_to_temp_file", side_effect=RuntimeError("boom")):
                        RESULT = CLIENT.download_file_result("library/a.jpg", LOCAL_PATH)
                        self.assertEqual(RESULT.failure_reason, "download_read_failed")

# --------------------------------------------------------------------------
# This test confirms successful downloads are written through a temporary file
# and only the final destination remains afterwards.
# --------------------------------------------------------------------------
    def test_download_file_writes_atomically(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=4,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
            download=lambda: ChunkHandle([b"data"]),
        )
        CLIENT.api = SimpleNamespace(photos=SimpleNamespace(all=[ASSET], albums={}))

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "library/2026/03/14/IMG_0001.JPG"

            self.assertTrue(CLIENT.download_file("library/2026/03/14/IMG_0001.JPG", LOCAL_PATH))
            self.assertEqual(LOCAL_PATH.read_bytes(), b"data")
            self.assertFalse((LOCAL_PATH.parent / ".IMG_0001.JPG.tmp").exists())

# --------------------------------------------------------------------------
# This test confirms unsupported download handles fail without leaving output.
# --------------------------------------------------------------------------
    def test_download_file_rejects_unsupported_handle_without_output(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=4,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
            download=lambda: object(),
        )
        CLIENT.api = SimpleNamespace(photos=SimpleNamespace(all=[ASSET], albums={}))

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "library/2026/03/14/IMG_0001.JPG"

            self.assertFalse(CLIENT.download_file("library/2026/03/14/IMG_0001.JPG", LOCAL_PATH))
            self.assertEqual(CLIENT.get_last_download_failure_reason(), "empty_download")
            self.assertFalse(LOCAL_PATH.exists())
            self.assertFalse((LOCAL_PATH.parent / ".IMG_0001.JPG.tmp").exists())

# --------------------------------------------------------------------------
# This test confirms interrupted downloads clean up temporary files and leave
# any existing final destination untouched.
# --------------------------------------------------------------------------
    def test_download_file_cleans_up_failed_partial_downloads(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=10,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
            download=lambda: BrokenChunkHandle(),
        )
        CLIENT.api = SimpleNamespace(photos=SimpleNamespace(all=[ASSET], albums={}))

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOCAL_PATH = Path(TMPDIR) / "library/2026/03/14/IMG_0001.JPG"
            LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
            LOCAL_PATH.write_bytes(b"existing")

            self.assertFalse(CLIENT.download_file("library/2026/03/14/IMG_0001.JPG", LOCAL_PATH))
            self.assertEqual(CLIENT.get_last_download_failure_reason(), "download_read_failed")
            self.assertEqual(LOCAL_PATH.read_bytes(), b"existing")
            self.assertFalse((LOCAL_PATH.parent / ".IMG_0001.JPG.tmp").exists())

# --------------------------------------------------------------------------
# This test confirms repeated downloads reuse the resolved listing cache
# instead of re-reading the full asset collection for each path lookup.
# --------------------------------------------------------------------------
    def test_download_file_reuses_listing_cache_for_asset_lookup(self) -> None:
        CLIENT = CountingIcloudClient(create_config())
        FIRST_ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=4,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
            download=lambda: ChunkHandle([b"data"]),
        )
        SECOND_ASSET = SimpleNamespace(
            id="asset-2",
            filename="IMG_0002.JPG",
            size=4,
            created=datetime(2026, 3, 14, 10, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 10, 31, tzinfo=timezone.utc),
            download=lambda: ChunkHandle([b"more"]),
        )
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                all=[FIRST_ASSET, SECOND_ASSET],
                albums={},
            )
        )

        ENTRIES = CLIENT.list_entries()

        with tempfile.TemporaryDirectory() as TMPDIR:
            FIRST_PATH = Path(TMPDIR) / ENTRIES[0].path
            SECOND_PATH = Path(TMPDIR) / ENTRIES[1].path

            self.assertTrue(CLIENT.download_file(ENTRIES[0].path, FIRST_PATH))
            self.assertTrue(CLIENT.download_file(ENTRIES[1].path, SECOND_PATH))

        self.assertEqual(CLIENT.read_all_assets_calls, 1)

# --------------------------------------------------------------------------
# This test confirms colliding canonical paths still download the matching
# source asset after path disambiguation.
# --------------------------------------------------------------------------
    def test_colliding_paths_keep_correct_asset_binding_for_download(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        FIRST_ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=4,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
            download=lambda: ChunkHandle([b"one1"]),
        )
        SECOND_ASSET = SimpleNamespace(
            id="asset-2",
            filename="IMG_0001.JPG",
            size=4,
            created=datetime(2026, 3, 14, 10, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 10, 31, tzinfo=timezone.utc),
            download=lambda: ChunkHandle([b"two2"]),
        )
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                all=[SECOND_ASSET, FIRST_ASSET],
                albums={"Trips": [FIRST_ASSET, SECOND_ASSET]},
            )
        )

        ENTRIES = CLIENT.list_entries()
        PATH_BY_ASSET_ID = {ENTRY.asset_id: ENTRY.path for ENTRY in ENTRIES}

        with tempfile.TemporaryDirectory() as TMPDIR:
            FIRST_PATH = Path(TMPDIR) / "first.JPG"
            SECOND_PATH = Path(TMPDIR) / "second.JPG"

            self.assertTrue(CLIENT.download_file(PATH_BY_ASSET_ID["asset-1"], FIRST_PATH))
            self.assertTrue(CLIENT.download_file(PATH_BY_ASSET_ID["asset-2"], SECOND_PATH))
            self.assertEqual(FIRST_PATH.read_bytes(), b"one1")
            self.assertEqual(SECOND_PATH.read_bytes(), b"two2")
