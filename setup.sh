#!/bin/bash
# pgxllm development environment setup
# Usage: bash setup.sh

set -e

WORKSPACE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "=== pgxllm setup: $WORKSPACE ==="

# ── Python venv ──────────────────────────────────────────────
if [ ! -d "$WORKSPACE/.venv" ]; then
    echo "[1/4] Creating virtual environment..."
    python3 -m venv "$WORKSPACE/.venv"
else
    echo "[1/4] Virtual environment already exists"
fi

source "$WORKSPACE/.venv/bin/activate"

# ── Install dependencies ─────────────────────────────────────
echo "[2/4] Installing dependencies..."
pip install --upgrade pip -q
pip install -e ".[dev]" -q

# ── ANTLR4 jar (optional — for grammar regeneration) ─────────
echo "[3/4] Checking ANTLR4 jar..."
ANTLR_JAR="$WORKSPACE/tools/antlr-4.13.2-complete.jar"
if [ ! -f "$ANTLR_JAR" ]; then
    mkdir -p "$WORKSPACE/tools"
    echo "  Downloading ANTLR4 4.13.2..."
    curl -sL "https://www.antlr.org/download/antlr-4.13.2-complete.jar" \
         -o "$ANTLR_JAR" 2>/dev/null || \
    echo "  (skipped — download antlr-4.13.2-complete.jar manually to tools/)"
else
    echo "  ANTLR4 jar found: $ANTLR_JAR"
fi

# ── .env ─────────────────────────────────────────────────────
echo "[4/4] Checking .env..."
if [ ! -f "$WORKSPACE/.env" ]; then
    cp "$WORKSPACE/.env.example" "$WORKSPACE/.env"
    echo "  Created .env from .env.example — fill in your values"
else
    echo "  .env already exists"
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  source .venv/bin/activate"
echo "  python -m pytest tests/ -v"
echo ""
echo "VSCode: Open folder $WORKSPACE"
echo "  Ctrl+Shift+P → 'Python: Select Interpreter' → .venv/bin/python"
