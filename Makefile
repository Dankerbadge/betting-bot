.PHONY: venv install install-dev lock-dev precommit-install precommit-run clean secrets-check lint typecheck check check-cli ci-local test test-unittest test-pytest test-alpha-core test-profit-readiness test-gatechain test-gatechain-smoke test-trading-confidence

VENV ?= .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
PRE_COMMIT := $(VENV)/bin/pre-commit
GATECHAIN_SMOKE_TESTS := tests/test_gatechain_workflow_sync.py \
	tests/test_bootstrap_ubuntu_2404_script.py \
	tests/test_preflight_temperature_shadow_script.py \
	tests/test_install_systemd_temperature_shadow_script.py \
	tests/test_install_systemd_temperature_recovery_script.py \
	tests/test_install_systemd_temperature_coldmath_hardening_script.py \
	tests/test_set_coldmath_recovery_env_persistence_gate_script.py
GATECHAIN_TESTS := tests/test_bootstrap_ubuntu_2404_script.py \
	tests/test_gatechain_workflow_sync.py \
	tests/test_preflight_temperature_shadow_script.py \
	tests/test_install_systemd_temperature_shadow_script.py \
	tests/test_install_systemd_temperature_recovery_script.py \
	tests/test_install_systemd_temperature_coldmath_hardening_script.py \
	tests/test_set_coldmath_recovery_env_persistence_gate_script.py \
	tests/test_check_temperature_shadow_script.py \
	tests/test_check_temperature_shadow_quick_script.py \
	tests/test_alpha_summary_script.py \
	tests/test_run_temperature_recovery_chaos_check_script.py
ALPHA_CORE_TESTS := tests/test_kalshi_temperature_metar_ingest.py \
	tests/test_kalshi_temperature_weather_pattern.py \
	tests/test_kalshi_temperature_selection_quality.py \
	tests/test_kalshi_temperature_profitability.py \
	tests/test_kalshi_temperature_growth_optimizer.py \
	tests/test_kalshi_temperature_execution_cost_tape.py
PROFIT_READINESS_TESTS := tests/test_decision_matrix_hardening.py \
	tests/test_kalshi_temperature_bankroll_validation.py \
	tests/test_kalshi_temperature_recovery_advisor.py \
	tests/test_kalshi_temperature_recovery_campaign.py \
	tests/test_kalshi_temperature_recovery_loop.py \
	tests/test_kalshi_temperature_profitability.py \
	tests/test_kalshi_temperature_growth_optimizer.py \
	tests/test_kalshi_temperature_selection_quality.py

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

ci-local:
	$(MAKE) install-dev
	$(MAKE) test-gatechain
	$(MAKE) precommit-run
	$(MAKE) check

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

test-alpha-core:
	@if [ ! -x "$(PYTEST)" ]; then \
		echo "pytest is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(PYTEST) -q $(ALPHA_CORE_TESTS)

test-profit-readiness:
	@if [ ! -x "$(PYTEST)" ]; then \
		echo "pytest is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(PYTEST) -q $(PROFIT_READINESS_TESTS)

test-gatechain:
	@if [ ! -x "$(PYTEST)" ]; then \
		echo "pytest is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(PYTEST) -q $(GATECHAIN_TESTS)

test-gatechain-smoke:
	@if [ ! -x "$(PYTEST)" ]; then \
		echo "pytest is not installed in $(VENV). Run: make install-dev"; \
		exit 1; \
	fi
	$(PYTEST) -q $(GATECHAIN_SMOKE_TESTS)

test-trading-confidence:
	$(MAKE) test-alpha-core
	$(MAKE) test-profit-readiness
	$(MAKE) test-gatechain-smoke
