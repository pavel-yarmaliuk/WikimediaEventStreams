"""
Centralized logging setup: console + size-rotated file under ``logs/``.

Each entrypoint calls ``configure_logging("<name>")`` once at startup. Handlers
are installed on the root logger so library modules using
``logging.getLogger(__name__)`` propagate into the same file automatically.

Tunable via env vars: LOG_DIR, LOG_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
_DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_DEFAULT_BACKUP_COUNT = 5

_configured = False


def configure_logging(name: str, *, log_dir: str | None = None) -> logging.Logger:
    """Install console + rotating-file handlers on the root logger (once).

    Returns ``logging.getLogger(name)`` so the caller can log immediately.
    Safe to call more than once per process — subsequent calls are no-ops.
    """
    global _configured

    if not _configured:
        resolved_dir = log_dir or os.environ.get("LOG_DIR", "logs")
        os.makedirs(resolved_dir, exist_ok=True)

        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        max_bytes = int(os.environ.get("LOG_MAX_BYTES", _DEFAULT_MAX_BYTES))
        backup_count = int(os.environ.get("LOG_BACKUP_COUNT", _DEFAULT_BACKUP_COUNT))

        formatter = logging.Formatter(_FORMAT)

        console = logging.StreamHandler()
        console.setFormatter(formatter)

        file_handler = RotatingFileHandler(
            os.path.join(resolved_dir, f"{name}.log"),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        root = logging.getLogger()
        root.setLevel(level)
        root.addHandler(console)
        root.addHandler(file_handler)

        _configured = True

    return logging.getLogger(name)
