#!/bin/bash

# Production startup script
# This script:
# 1. Sets up the virtual environment
# 2. Installs dependencies
# 3. Builds TypeScript
# 4. Starts Gunicorn (when run directly, systemd service will use this script)

# Exit on any error
set -e

# Configuration
VENV_DIR="venv"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"

# Ensure we're in the project directory
cd "$PROJECT_DIR"

# Try to pull latest code
echo "Pulling latest code from repository..."
if [ -d .git ]; then
    git pull || echo "Warning: Failed to pull latest code. Continuing with existing codebase..."
else
    echo "Not a git repository. Continuing with existing codebase..."
fi

# Create and activate virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install or upgrade pip
python3 -m pip install --upgrade pip

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install Node.js dependencies and build TypeScript
echo "Installing Node.js dependencies and building TypeScript..."
npm install
npm run build

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Start Gunicorn (this will be used by systemd)
exec /opt/mscopy/venv/bin/gunicorn/gunicorn \
    --workers ${GUNICORN_WORKERS:-3} \
    --worker-class ${GUNICORN_WORKER_CLASS:-gevent} \
    --timeout ${GUNICORN_TIMEOUT:-120} \
    --bind ${HOST:-0.0.0.0}:${PORT:-8000} \
    --access-logfile "$LOG_DIR/access.log" \
    --error-logfile "$LOG_DIR/error.log" \
    --log-level ${LOG_LEVEL:-info} \
    backend:app
