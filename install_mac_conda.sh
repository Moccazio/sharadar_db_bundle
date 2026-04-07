#!/bin/bash
# Install sharadar_db_bundle on macOS
# Source repo: https://github.com/Moccazio/sharadar_db_bundle
set -euo pipefail

export PYTHON_VERSION=3.10
export CONDA_ENV_NAME=zipline-reloaded-py${PYTHON_VERSION//./}

# ---------------------------------------------------------------------------
# 1. Conda bootstrap
# ---------------------------------------------------------------------------
CONDA_BASE="$(conda info --base 2>/dev/null || true)"
if [ -z "$CONDA_BASE" ]; then
    echo "Conda not found. Installing Miniconda..."
    MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-$(uname -m).sh"
    curl -fsSL "$MINICONDA_URL" -o /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
    rm /tmp/miniconda.sh
    CONDA_BASE="$HOME/miniconda3"
    # Initialise conda for this shell session
    source "$CONDA_BASE/etc/profile.d/conda.sh"
else
    source "$CONDA_BASE/etc/profile.d/conda.sh"
fi

# ---------------------------------------------------------------------------
# 2. Create and activate conda environment
# ---------------------------------------------------------------------------
if conda env list | grep -q "^${CONDA_ENV_NAME} "; then
    echo "Conda env '${CONDA_ENV_NAME}' already exists — reusing."
else
    conda create -y -n "${CONDA_ENV_NAME}" python="${PYTHON_VERSION}"
fi

conda activate "${CONDA_ENV_NAME}"

# Install ta-lib C library and other native tools via conda-forge
conda install -y -c conda-forge ta-lib wget unzip git

python -m pip install --upgrade pip wheel

export PYTHON_LIBS="$(python -c 'import site; print(site.getsitepackages()[0])')"

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
