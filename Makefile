.PHONY: venv install install-dev lock-dev precommit-install precommit-run clean secrets-check lint typecheck check check-cli test test-unittest test-pytest

VENV ?= .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
PRE_COMMIT := $(VENV)/bin/pre-commit

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.lock.txt
	$(PIP) install -r requirements.txt
	$(PIP) install .

install-dev: install
	$(PIP) install -r requirements-dev.lock.txt

lock-dev: install
	$(PIP) install -r requirements-dev.txt
	$(PIP) freeze | sort | grep -v '^betbot @ ' > requirements-dev.lock.txt

precommit-install:
	$(PRE_COMMIT) install

precommit-run:
	$(PRE_COMMIT) run --all-files

clean:
	rm -rf build dist .pytest_cache .mypy_cache .ruff_cache
	find . -maxdepth 1 -type d -name "*.egg-info" -exec rm -rf {} +
	find betbot tests -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -maxdepth 1 -type d -name "__pycache__" -prune -exec rm -rf {} +

secrets-check:
	$(PYTHON) scripts/check_no_tracked_secrets.py

lint:
	@if [ ! -x "$(RUFF)" ]; then \
		echo "ruff is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(RUFF) check betbot tests scripts

typecheck:
	@if [ ! -x "$(MYPY)" ]; then \
		echo "mypy is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(MYPY) --config-file mypy.ini

test: test-unittest

check: secrets-check check-cli lint typecheck test test-pytest

check-cli:
	$(PYTHON) -m betbot --help >/dev/null
	@if [ ! -x "$(VENV)/bin/betbot" ]; then \
		echo "betbot console script not found. Run: make install"; \
		exit 1; \
	fi
	$(VENV)/bin/betbot --help >/dev/null

test-unittest:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py"

test-pytest:
	@if [ ! -x "$(PYTEST)" ]; then \
		echo "pytest is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(PYTEST) -q
