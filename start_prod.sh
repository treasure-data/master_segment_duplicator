#!/bin/bash

# Production startup script with Supervisor
# This script:
# 1. Sets up the virtual environment
# 2. Installs dependencies
# 3. Builds TypeScript
# 4. Starts Supervisor to manage Gunicorn

# Exit on any error
set -e

# Configuration
VENV_DIR="venv"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"

# Ensure we're in the project directory
cd "$PROJECT_DIR"

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

# Start Supervisor
echo "Starting Supervisor..."
supervisord -c supervisord.conf

# Show status
sleep 2
supervisorctl status mscopy

echo "
Production server started with Supervisor!
Use these commands to manage the server:
- supervisorctl status mscopy    : Check status
- supervisorctl stop mscopy      : Stop server
- supervisorctl start mscopy     : Start server
- supervisorctl restart mscopy   : Restart server
- supervisorctl tail mscopy      : View logs
"
