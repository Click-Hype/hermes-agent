"""The per-load ``good`` copy must cost nothing when config.yaml has not changed.

A cache miss that re-reads an unchanged file used to create, sweep and list ``backups/config/`` and chmod
every copy in it (``backup_config`` runs inside ``_load_config_impl``): dozens of syscalls and fsync-bound
directory work per load, on a disk where one fsync takes hundreds of ms.
"""
from pathlib import Path

import pytest

from hermes_cli import config_backups
from hermes_cli.config_backups import backup_config, list_config_backups

TEXT = "model:\n  default: test/one\n"


@pytest.fixture(autouse=True)
def _fresh_memo():
    config_backups._BACKED_UP_STAT.clear()
    yield
    config_backups._BACKED_UP_STAT.clear()


def _forbid_backup_dir_work(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("an unchanged config touched the backups dir")

    monkeypatch.setattr(config_backups, "_sweep_legacy_siblings", boom)
    monkeypatch.setattr(config_backups, "list_config_backups", boom)
    monkeypatch.setattr(Path, "mkdir", boom)


def test_unchanged_config_returns_before_the_backups_dir(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(TEXT, encoding="utf-8")
    first = backup_config(config, "good", skip_if_unchanged=True)
    assert first is not None and first.is_file()

    _forbid_backup_dir_work(monkeypatch)
    assert backup_config(config, "good", skip_if_unchanged=True) is None


def test_changed_config_is_backed_up_again(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(TEXT, encoding="utf-8")
    stamps = iter(["20261127-100000", "20261127-100001"])  # copies are named to the second
    monkeypatch.setattr(config_backups.time, "strftime", lambda _fmt: next(stamps))
    backup_config(config, "good", skip_if_unchanged=True)
    config.write_text(TEXT + "  provider: other\n", encoding="utf-8")

    assert backup_config(config, "good", skip_if_unchanged=True) is not None
    assert len(list_config_backups(config, "good")) == 2


def test_identical_to_newest_copy_is_remembered_too(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(TEXT, encoding="utf-8")
    backup_config(config, "good")  # a copy exists, the memo is not asked for
    config_backups._BACKED_UP_STAT.clear()  # a fresh process
    assert backup_config(config, "good", skip_if_unchanged=True) is None  # identical: scan once, remember

    _forbid_backup_dir_work(monkeypatch)
    assert backup_config(config, "good", skip_if_unchanged=True) is None


def test_the_default_call_still_compares_bytes(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(TEXT, encoding="utf-8")
    assert backup_config(config, "pre-migrate") is not None
    assert backup_config(config, "pre-migrate") is None  # identical bytes: no second copy
    assert len(list_config_backups(config, "pre-migrate")) == 1


def test_memo_is_per_reason(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(TEXT, encoding="utf-8")
    assert backup_config(config, "good", skip_if_unchanged=True) is not None
    assert backup_config(config, "corrupt", skip_if_unchanged=True) is not None
