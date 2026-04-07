#!/bin/bash
# Install sharadar_db_bundle on macOS
# Source repo: https://github.com/Moccazio/sharadar_db_bundle
set -euo pipefail

export PYTHON_VERSION=3.10
export VENV_NAME=zipline-reloaded-venv${PYTHON_VERSION}

# ---------------------------------------------------------------------------
# 1. System dependencies via Homebrew
# ---------------------------------------------------------------------------
if ! command -v brew &>/dev/null; then
    echo "Homebrew not found. Installing..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

brew install python@${PYTHON_VERSION} ta-lib wget unzip git

# ---------------------------------------------------------------------------
# 2. Virtual environment
# ---------------------------------------------------------------------------
PYTHON_BIN="$(brew --prefix python@${PYTHON_VERSION})/bin/python${PYTHON_VERSION}"

if ! command -v virtualenv &>/dev/null; then
    "$PYTHON_BIN" -m pip install --quiet virtualenv
fi

virtualenv -p "$PYTHON_BIN" ~/${VENV_NAME}
source ~/${VENV_NAME}/bin/activate
python -m pip install --upgrade pip wheel

export PYTHON_LIBS=~/${VENV_NAME}/lib/python${PYTHON_VERSION}/site-packages

# ---------------------------------------------------------------------------
# 3. Interactive Brokers TWS API
#    (The "macunix" archive covers both macOS and Linux)
# ---------------------------------------------------------------------------
IB_WORK="$(mktemp -d)"
cd "$IB_WORK"
wget -q https://interactivebrokers.github.io/downloads/twsapi_macunix.1025.01.zip
unzip -q twsapi_macunix.1025.01.zip -d twsapi
cd twsapi/IBJts/source/pythonclient
python setup.py bdist_wheel --quiet
pip install --quiet --upgrade dist/ibapi-10.25.1-py3-none-any.whl
cd /
rm -rf "$IB_WORK"

# ---------------------------------------------------------------------------
# 4. sharadar_db_bundle
# ---------------------------------------------------------------------------
INSTALL_DIR="${PYTHON_LIBS}/sharadar_db_bundle"
if [ -d "$INSTALL_DIR" ]; then
    echo "Removing existing clone at $INSTALL_DIR..."
    rm -rf "$INSTALL_DIR"
fi

git clone https://github.com/Moccazio/sharadar_db_bundle.git "$INSTALL_DIR"
cd "$INSTALL_DIR"

pip install -r requirements.txt
pip install --upgrade --force-reinstall -e .

# ---------------------------------------------------------------------------
# 5. Smoke test
# ---------------------------------------------------------------------------
python test/basic_pipeline_sep_db.py

if [ "$?" -eq 0 ]; then
    echo "INSTALLATION SUCCESSFUL"
else
    echo "INSTALLATION FAILED"
fi
