"""
backend.py
---------
Flask-based web server that provides a web interface and API endpoints for
the Treasure Data segment copying tool.

This module serves as the bridge between the web frontend and the copier.py script.
It provides:
1. Static file serving for web assets
2. HTML template serving for the main interface
3. Real-time updates via Socket.IO for segment copy operations
4. Reliable message delivery with automatic reconnection
"""

import os
import json
from dataclasses import dataclass
from subprocess import Popen, PIPE
from flask import Flask, render_template, send_from_directory, request
from flask_socketio import SocketIO, emit


# Data model for copy request
@dataclass
class CopyRequest:
    masterSegmentId: str
    apiKey: str
    instance: str
    masterSegmentName: str
    apiKeyOutput: str
    copyAssets: bool = False
    copyDataAssets: bool = False

    @classmethod
    def from_dict(cls, data):
        return cls(
            masterSegmentId=data["masterSegmentId"],
            apiKey=data["apiKey"],
            instance=data["instance"],
            masterSegmentName=data["masterSegmentName"],
            apiKeyOutput=data["apiKeyOutput"],
            copyAssets=bool(data.get("copyAssets", False)),
            copyDataAssets=bool(data.get("copyDataAssets", False)),
        )


# Load configuration
from config import config

# Initialize Flask and Socket.IO
app = Flask(
    __name__, static_folder=config.STATIC_DIR, template_folder=config.TEMPLATE_DIR
)
app.config.from_object(config)

socketio = SocketIO(
    app,
    cors_allowed_origins=config.CORS_ORIGINS,
    async_mode="threading",
    logger=config.DEBUG,
    engineio_logger=config.DEBUG,
)


@app.route("/")
def index():
    """Serve the main HTML interface."""
    return render_template("index.html")


@app.route("/favicon.ico")
def favicon():
    """Serve the favicon."""
    return send_from_directory(app.static_folder, "favicon.ico")


def process_stream(proc, operation_id: str):
    """Process stdout/stderr streams and emit updates via Socket.IO."""
    try:

        def read_stream(stream, is_stderr=False):
            for line in stream:
                if line:
                    text = line.strip()
                    if text:
                        data = {
                            "type": "error" if is_stderr else "progress",
                            "message": f"Error: {text}" if is_stderr else text,
                            "operation_id": operation_id,
                        }
                        socketio.emit("copy_progress", data)

        # Read from both streams
        read_stream(proc.stdout)
        read_stream(proc.stderr, is_stderr=True)

        # Wait for process to complete and capture return code
        return_code = proc.wait()

        # Send final status
        socketio.emit(
            "copy_progress",
            {
                "type": "success" if return_code == 0 else "error",
                "message": (
                    "Process completed successfully!"
                    if return_code == 0
                    else f"Process failed with exit code {return_code}"
                ),
                "operation_id": operation_id,
            },
        )

    except Exception as e:
        error_msg = str(e)
        print(f"Stream processing error: {error_msg}")
        socketio.emit(
            "copy_progress",
            {
                "type": "error",
                "message": f"Error processing stream: {error_msg}",
                "operation_id": operation_id,
            },
        )


@socketio.on("connect")
def handle_connect():
    print("Client connected")


@socketio.on("disconnect")
def handle_disconnect():
    print("Client disconnected")


@socketio.on("start_copy")
def handle_copy_request(data):
    """Handle copy request from client via Socket.IO."""
    try:
        request = CopyRequest.from_dict(data)

        # Create subprocess running the copier script
        proc = Popen(
            [
                "python3",
                "copier.py",
                request.masterSegmentId,
                request.apiKey,
                request.instance,
                request.masterSegmentName,
                request.apiKeyOutput,
                str(request.copyAssets).lower(),
                str(request.copyDataAssets).lower(),
            ],
            stdout=PIPE,
            stderr=PIPE,
            bufsize=1,  # Line buffered
            universal_newlines=True,
        )

        # Generate a unique operation ID
        operation_id = f"copy_{request.masterSegmentId}_{request.masterSegmentName}"

        # Process the output streams
        process_stream(proc, operation_id)

    except Exception as e:
        error_message = f"Failed to start process: {str(e)}"
        emit(
            "copy_progress",
            {"type": "error", "message": error_message, "operation_id": "error"},
        )


if __name__ == "__main__":
    # Using threading mode and disabling reloader for stability
    socketio.run(
        app,
        host="0.0.0.0",  # Allow external connections
        port=8000,  # Use port 8000
        debug=True,
        use_reloader=False,
    )
