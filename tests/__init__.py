# ------------------------------------------------------------------------------
# This package contains unit tests for the iCloud Photos backup worker.
# ------------------------------------------------------------------------------

from app import logger as logger_module


# ------------------------------------------------------------------------------
# This function suppresses test-time console log noise from the worker logger.
#
# Behaviour notes:
# 1. Unit tests still exercise file logging and log formatting paths.
# 2. This affects only the test process because it replaces the module-level
#    "print" name used inside "app.logger".
#
# Returns: None.
# ------------------------------------------------------------------------------
def suppress_logger_console_output(*ARGS, **KWARGS) -> None:
    _ = (ARGS, KWARGS)


logger_module.print = suppress_logger_console_output
