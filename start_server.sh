#!/bin/bash

# Set environment variables
export FLASK_ENV=${FLASK_ENV:-production}
export FLASK_APP=backend.py
export FLASK_SECRET_KEY=$(python3 -c 'import os; print(os.urandom(24).hex())')
export ALLOWED_ORIGIN="*" # Change this in production to your actual domain

# Create and activate virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install or upgrade pip
python3 -m pip install --upgrade pip

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install Node.js dependencies and build TypeScript
echo "Installing Node.js dependencies and building TypeScript..."
npm install
npm run build

# Choose server based on environment
if [ "$FLASK_ENV" = "development" ]; then
    echo "Starting Flask development server..."
    exec python3 backend.py
else
    echo "Starting Gunicorn production server..."
    mkdir -p logs
    exec gunicorn \
        --worker-class gevent \
        --workers 3 \
        --bind 0.0.0.0:8000 \
        --timeout 120 \
        --access-logfile logs/access.log \
        --error-logfile logs/error.log \
        --capture-output \
        --log-level info \
        'backend:app'
fi
