
# ==========================================
# Project configuration
# ==========================================

PYTHON_VERSION := $(shell cat .python-version)
POETRY := poetry

.DEFAULT_GOAL := help

# ==========================================
# Help
# ==========================================

.PHONY: help
help:
	@echo "NYC Taxi Event-Time Reconciliation"
	@echo ""
	@echo "Available commands:"
	@echo "  make bootstrap     Install system tools with Homebrew"
	@echo "  make setup         Configure Python and install dependencies"
	@echo "  make install       Install project dependencies"
	@echo "  make test          Run automated tests"
	@echo "  make lint          Check code quality"
	@echo "  make format        Format Python code"
	@echo "  make check         Run lint and tests"
	@echo "  make kafka-start   Start local infrastructure"
	@echo "  make kafka-stop    Stop local infrastructure"
	@echo "  make clean         Remove local Python environment"

# ==========================================
# System tools
# ==========================================

.PHONY: bootstrap
bootstrap:
	brew update
	brew install pyenv
	brew install poetry
	brew install --cask docker

# ==========================================
# Python environment
# ==========================================

.PHONY: setup
setup:
	@echo "Using Python $(PYTHON_VERSION)"
	pyenv install --skip-existing $(PYTHON_VERSION)
	pyenv local $(PYTHON_VERSION)
	$(POETRY) config virtualenvs.in-project true
	$(POETRY) env use $(PYTHON_VERSION)
	$(POETRY) lock
	$(POETRY) install --with dev

.PHONY: install
install:
	$(POETRY) install --with dev

.PHONY: lock
lock:
	$(POETRY) lock

.PHONY: shell
shell:
	$(POETRY) shell

# ==========================================
# Dataset and reference pipeline
# ==========================================

.PHONY: prepare-dataset
prepare-dataset:
	$(POETRY) run prepare-dataset

.PHONY: build-reference
build-reference:
	$(POETRY) run build-reference

# ==========================================
# Quality assurance
# ==========================================

.PHONY: test
test:
	$(POETRY) run pytest

.PHONY: lint
lint:
	$(POETRY) run ruff check .

.PHONY: format
format:
	$(POETRY) run ruff check --fix .
	$(POETRY) run ruff format .

.PHONY: format-check
format-check:
	$(POETRY) run ruff format --check .

.PHONY: check
check: lint format-check test

# ==========================================
# Infrastructure placeholders
# ==========================================

.PHONY: kafka-start
kafka-start:
	docker compose up -d kafka

.PHONY: kafka-stop
kafka-stop:
	docker compose stop kafka

# ==========================================
# Cleanup
# ==========================================

.PHONY: clean
clean:
	rm -rf .venv
	$(POETRY) env remove --all
