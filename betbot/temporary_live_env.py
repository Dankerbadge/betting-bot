from __future__ import annotations

from contextlib import contextmanager
import tempfile
from pathlib import Path
from typing import Iterator


@contextmanager
def temporary_live_env_file(source_env_file: str) -> Iterator[str]:
    source_path = Path(source_env_file)
    contents = source_path.read_text(encoding="utf-8")
    fd, temp_path_str = tempfile.mkstemp(prefix="betbot_live_", suffix=".env")
    temp_path = Path(temp_path_str)
    try:
        with open(fd, "w", encoding="utf-8", closefd=True) as handle:
            handle.write(contents)
            if contents and not contents.endswith("\n"):
                handle.write("\n")
            handle.write("BETBOT_ENABLE_LIVE_ORDERS=1\n")
        yield str(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)
