"""Point-in-time copies of ``config.yaml``: one directory, one naming scheme, bounded count.

Every writer that wants a "before" copy of the user's config (setup wizard, corrupt-file
snapshot, model migrations) goes through :func:`backup_config`. Copies live in
``<HERMES_HOME>/backups/config/`` — ``backups/`` is already excluded from full backups, so they
never nest — as ``config.yaml.<reason>.<YYYYMMDD-HHMMSS>``. A copy identical to the newest one
for the same reason is skipped, and only the newest ``keep`` per reason survive, so repeated
``hermes setup`` runs or a gateway restarting against broken YAML cannot litter the home dir.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

BACKUPS_SUBDIR = Path("backups") / "config"
DEFAULT_KEEP = 5

# (path, reason) -> (inode, size, mtime_ns) of the config this process already backed up (or found
# identical to its newest copy). A load that misses the config cache for another reason re-reads an
# unchanged file; with ``skip_if_unchanged`` it must not also re-scan the backups dir on a disk that fsyncs
# in hundreds of ms.
_BACKED_UP_STAT: dict[tuple[str, str], tuple[int, int, int]] = {}

# Names earlier code wrote next to config.yaml (setup wizard, corrupt snapshot, xai migration).
# Moved into the backups dir on first use so they stop accumulating in the home root; hand-named
# copies (``config.yaml.bak-my-note``) are the user's and are never touched.
_LEGACY_SIBLING_GLOBS = ("config.yaml.bak.[0-9]*", "config.yaml.corrupt.*.bak", "config.yaml.bak-pre-migrate-*")


def backups_dir(config_path: Path) -> Path:
    return config_path.parent / BACKUPS_SUBDIR


def list_config_backups(config_path: Path, reason: Optional[str] = None) -> list[Path]:
    """Existing backups, newest first; filtered to one *reason* when given."""
    root = backups_dir(config_path)
    if not root.is_dir():
        return []
    prefix = f"{config_path.name}.{reason}." if reason else f"{config_path.name}."
    return sorted((p for p in root.iterdir() if p.is_file() and p.name.startswith(prefix)),
                  key=lambda p: p.name, reverse=True)


def backup_config(config_path: Path, reason: str, *, keep: int = DEFAULT_KEEP,
                  skip_if_unchanged: bool = False) -> Optional[Path]:
    """Copy *config_path* to the backups dir; return the new path, or None when skipped/failed.

    ``skip_if_unchanged``: a file whose inode, size and mtime match what this process already backed up
    for *reason* returns at once, before the directory is created, swept or listed (the per-load
    "good" copy; the writers that need a "before" copy keep the byte comparison).

    Skips when the file is missing/empty, or when the newest backup for *reason* already holds
    identical bytes. Never raises: a failed backup must not block the write it precedes.
    Skipped entirely for a config backend without file tooling (no local copies, D2).
    """
    from hermes_cli.config_backend import supports_file_tooling
    if not supports_file_tooling():
        return None
    try:
        stat = config_path.stat()  # config-reader: ok — file tooling, gated above
        if not config_path.is_file() or stat.st_size == 0:
            return None
        signature = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        memo_key = (str(config_path), reason)
        if skip_if_unchanged and _BACKED_UP_STAT.get(memo_key) == signature:
            return None
        root = backups_dir(config_path)
        root.mkdir(parents=True, exist_ok=True)
        _sweep_legacy_siblings(config_path, root)
        # Copies written before backups were secured kept their source's (often 0644) mode, and a
        # reason that never fires again never rotates them out. Drop only the world bits, as for
        # config.yaml writes: a shared-group home may keep its backups group-readable on purpose.
        from hermes_cli.config_backend import drop_other_access
        for old in list_config_backups(config_path):
            if not old.is_symlink():
                drop_other_access(old)
        existing = list_config_backups(config_path, reason)
        # Not filecmp.cmp: its process-wide cache keys on (size, mtime), so an equal-size edit
        # within one mtime tick of an earlier comparison is reported identical and never backed up.
        if existing and config_path.read_bytes() == existing[0].read_bytes():  # config-reader: ok — file tooling, gated above
            _BACKED_UP_STAT[memo_key] = signature
            return None
        dest = root / f"{config_path.name}.{reason}.{time.strftime('%Y%m%d-%H%M%S')}"
        if dest.is_symlink() or dest.exists():  # never write through a planted link
            return None
        shutil.copy2(config_path, dest)  # config-reader: ok — file tooling, gated above
        # copy2 carries the source mode over, so a config.yaml left 0644 by an editor would yield a
        # world-readable copy of the user's credentials. Apply the same owner-only policy as config.yaml.
        from hermes_cli.config import _secure_file
        _secure_file(dest)
        for stale in [dest, *existing][keep:]:
            stale.unlink(missing_ok=True)
        _BACKED_UP_STAT[memo_key] = signature
        return dest
    except OSError as exc:
        logger.warning("Could not back up %s (%s): %s", config_path, reason, exc)
        return None


def load_newest_good_backup(config_path: Path) -> Optional[dict]:
    """Parse the newest ``good`` backup (the file as it was at the last successful load).

    Returns the raw mapping, or None when there is no usable copy. Older ``good`` copies are not
    tried: a backup that fails to parse means the on-disk copy was damaged after the fact, and
    guessing further back would serve a config the user never saw as current.
    """
    from hermes_cli.config_backend import supports_file_tooling
    if not supports_file_tooling():
        return None
    newest = list_config_backups(config_path, "good")[:1]
    if not newest:
        return None
    try:
        from utils import fast_safe_load
        with newest[0].open(encoding="utf-8") as f:
            data = fast_safe_load(f)
    except Exception as exc:
        logger.warning("Last-known-good backup %s is unreadable: %s", newest[0], exc)
        return None
    return data if isinstance(data, dict) else None


def _sweep_legacy_siblings(config_path: Path, root: Path) -> None:
    from hermes_cli.config import _secure_file
    for pattern in _LEGACY_SIBLING_GLOBS:
        for old in config_path.parent.glob(pattern):
            if not old.is_file() or old.is_symlink():
                continue
            try:
                old.replace(root / old.name)
                _secure_file(root / old.name)  # pre-backups-dir copies were often written 0644
            except OSError as exc:
                logger.debug("Could not move legacy backup %s: %s", old, exc)
