.PHONY: install test test-parser test-config lint format clean venv \
        build serve dev dev-backend dev-frontend

PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
PYTEST  := $(PYTHON) -m pytest
RUFF    := $(PYTHON) -m ruff
PGXLLM  := .venv/bin/pgxllm
HOST    ?= 0.0.0.0
PORT    ?= 8000

# ── Setup ─────────────────────────────────────────────────────
venv:
	python3 -m venv .venv
	$(PIP) install --upgrade pip -q
	$(PIP) install -e ".[dev]" -q

install: venv

# ── Production (single server, one port) ──────────────────────
build:
	cd frontend && npm install --silent && npm run build

serve: build
	$(PGXLLM) web --host $(HOST) --port $(PORT)

# ── Development (hot-reload, two processes) ───────────────────
dev-backend:
	$(PGXLLM) web --reload --host 0.0.0.0 --port 8000

dev-frontend:
	cd frontend && npm run dev

# ── Test ──────────────────────────────────────────────────────
test:
	PYTHONPATH=src $(PYTEST) tests/ -v --tb=short

test-parser:
	PYTHONPATH=src $(PYTEST) tests/parser/ -v

test-config:
	PYTHONPATH=src $(PYTEST) tests/test_config.py -v

test-cov:
	PYTHONPATH=src $(PYTEST) tests/ --cov=pgxllm --cov-report=html --cov-report=term-missing

# ── Lint / Format ──────────────────────────────────────────────
lint:
	$(RUFF) check src/ tests/

lint-fix:
	$(RUFF) check --fix src/ tests/

format:
	$(RUFF) format src/ tests/

# ── ANTLR4 ────────────────────────────────────────────────────
generate-parser:
	@ANTLR_JAR=$$(ls tools/antlr-*.jar 2>/dev/null | head -1); \
	if [ -z "$$ANTLR_JAR" ]; then \
	    echo "ERROR: antlr jar not found in tools/. Download antlr-4.13.2-complete.jar"; \
	    exit 1; \
	fi; \
	java -jar $$ANTLR_JAR \
	    -Dlanguage=Python3 \
	    -visitor -listener \
	    -o src/pgxllm/parser/generated \
	    src/pgxllm/parser/grammar/*.g4; \
	echo "Parser generated in src/pgxllm/parser/generated/"

# ── Clean ─────────────────────────────────────────────────────
clean:
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	find . -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null; true
	rm -rf .pytest_cache htmlcov .coverage
