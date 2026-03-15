# ------------------------------------------------------------------------------
# This module manages the healthcheck heartbeat file lifecycle for the worker.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import threading

HEARTBEAT_TOUCH_INTERVAL_SECONDS = 30


# ------------------------------------------------------------------------------
# This function updates the healthcheck heartbeat file timestamp.
#
# 1. "PATH" is the heartbeat file path.
#
# Returns: None.
# ------------------------------------------------------------------------------
def update_heartbeat(PATH: Path) -> None:
    try:
        PATH.parent.mkdir(parents=True, exist_ok=True)
        PATH.touch()
    except OSError:
        return


# ------------------------------------------------------------------------------
# This function starts a daemon heartbeat updater thread.
#
# 1. "PATH" is the heartbeat file path.
#
# Returns: Stop-event used to end the updater loop on process exit.
# ------------------------------------------------------------------------------
def start_heartbeat_updater(PATH: Path) -> threading.Event:
    STOP_EVENT = threading.Event()

    def run_heartbeat_loop() -> None:
        update_heartbeat(PATH)

        while not STOP_EVENT.wait(HEARTBEAT_TOUCH_INTERVAL_SECONDS):
            update_heartbeat(PATH)

    THREAD = threading.Thread(target=run_heartbeat_loop, daemon=True)
    THREAD.start()
    return STOP_EVENT
