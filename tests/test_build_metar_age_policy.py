from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys


def _load_build_metar_age_policy_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "infra" / "digitalocean" / "build_metar_age_policy.py"
    module_name = "build_metar_age_policy"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_in_window_includes_terminal_second_fractional_mtime(tmp_path: Path) -> None:
    module = _load_build_metar_age_policy_module()
    start_epoch = 1000.0
    end_epoch = 2000.0

    inside = tmp_path / "inside.json"
    inside.write_text("{}", encoding="utf-8")
    os.utime(inside, (end_epoch + 0.4, end_epoch + 0.4))
    assert module._in_window(inside, start_epoch, end_epoch) is True

    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    os.utime(outside, (end_epoch + 1.4, end_epoch + 1.4))
    assert module._in_window(outside, start_epoch, end_epoch) is False
