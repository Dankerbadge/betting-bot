"""Module entry point for `python -m betbot`."""

from __future__ import annotations

from .cli import main


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc))
