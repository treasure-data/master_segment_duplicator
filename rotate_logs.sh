#!/bin/bash

# Rotate logs script for production environment
# This script:
# 1. Rotates application log (mscopy.log) only
# 2. Compresses old logs
# 3. Keeps last 7 rotations
#
# Note: System logs (access.log, error.log) should be handled by systemd/logrotate

# Configuration
MAX_BACKUPS=7 # Keep 7 days of logs
LOG_DIR="logs"
APP_LOG="mscopy.log"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Ensure logs directory exists
mkdir -p "$LOG_DIR"

# Function to rotate a specific log file
rotate_log() {
    local log_file="$1"
    local base_name=$(basename "$log_file")
    local dir_name=$(dirname "$log_file")

    # Skip if file doesn't exist
    [ ! -f "$log_file" ] && return

    echo "Rotating $log_file..."

    # Rotate existing backups (from .4 to .5, .3 to .4, etc)
    for i in $(seq $((MAX_BACKUPS - 1)) -1 1); do
        if [ -f "${log_file}.$i" ]; then
            mv "${log_file}.$i" "${log_file}.$((i + 1))"
        fi
        if [ -f "${log_file}.$i.gz" ]; then
            mv "${log_file}.$i.gz" "${log_file}.$((i + 1)).gz"
        fi
    done

    # Backup current log
    if [ -f "$log_file" ]; then
        cp "$log_file" "${log_file}.1"
        # Compress the backup
        gzip "${log_file}.1"
        # Clear the current log
        echo "Log rotated at $TIMESTAMP" >"$log_file"
    fi

    # Remove backups older than 7 days
    find "$dir_name" -name "${base_name}.*" -mtime +7 -delete
}

# Rotate only the application log
if [ -f "$APP_LOG" ]; then
    rotate_log "$APP_LOG"
    echo "Log rotation completed for $APP_LOG at $TIMESTAMP"
else
    echo "Application log $APP_LOG not found at $TIMESTAMP"
fi
