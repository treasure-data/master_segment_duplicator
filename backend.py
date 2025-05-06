"""
backend.py
---------
FastAPI-based web server that provides a web interface and API endpoints for 
the Treasure Data segment copying tool.

This module serves as the bridge between the web frontend and the copier.py script.
It provides:
1. Static file serving for web assets
2. HTML template serving for the main interface
3. Streaming API endpoint for segment copy operations
4. Real-time progress updates via server-sent events

Dependencies:
- fastapi: Web framework for API endpoints
- uvicorn: ASGI server implementation
- asyncio: For asynchronous subprocess handling
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import json
import os

app = FastAPI(
    title="Treasure Data Segment Copier",
    description="Web interface for copying Treasure Data segments between environments",
    version="1.0.0"
)

# Serve static files (JavaScript, CSS, images) from the static directory
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def get_form():
    """
    Serves the main HTML interface for the segment copier tool.
    
    Returns:
        str: HTML content of the main interface
    """
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()

async def process_stream(proc):
    """
    Processes stdout/stderr streams from the copier script and yields formatted updates.
    
    This function handles the real-time output from the copier script, formats it
    as JSON messages, and streams it back to the client. It supports different
    message types (progress, error, success) and proper error handling.
    
    Args:
        proc: AsyncIO subprocess object running the copier script
        
    Yields:
        str: JSON-formatted status updates for the client
    """
    async def read_stream(stream, prefix=""):
        """
        Reads and formats output from a subprocess stream.
        
        Args:
            stream: AsyncIO stream (stdout/stderr)
            prefix: Optional prefix for messages (e.g., "Error: " for stderr)
            
        Yields:
            str: JSON-formatted status update
        """
        while True:
            line = await stream.readline()
            if not line:
                break
            line = line.decode().strip()
            if line:
                # Format the message as JSON with type and content
                update = {
                    "type": "progress" if "⚠️" not in line else "error",
                    "message": f"{prefix}{line}"
                }
                yield json.dumps(update) + "\n"

    # Create tasks for reading both stdout and stderr
    stdout_task = asyncio.create_task(read_stream(proc.stdout))
    stderr_task = asyncio.create_task(read_stream(proc.stderr, "Error: "))

    # Wait for both streams to complete
    done, pending = await asyncio.wait(
        [stdout_task, stderr_task],
        return_when=asyncio.ALL_COMPLETED
    )

    # Cancel any pending tasks
    for task in pending:
        task.cancel()

    # Get the return code and send final status
    await proc.wait()
    
    # Send final status message based on exit code
    if proc.returncode == 0:
        yield json.dumps({
            "type": "success",
            "message": "Process completed successfully!"
        }) + "\n"
    else:
        yield json.dumps({
            "type": "error",
            "message": f"Process failed with exit code {proc.returncode}"
        }) + "\n"

@app.post("/submit")
async def submit_form(request: Request):
    """
    Handles form submission and initiates the segment copy process.
    
    This endpoint:
    1. Receives form data from the client
    2. Validates and processes the input
    3. Starts the copier script as a subprocess
    4. Streams real-time updates back to the client
    
    Args:
        request (Request): FastAPI request object containing form data
        
    Returns:
        StreamingResponse: Server-sent events stream with process updates
    """
    try:
        data = await request.json()

        # Extract form fields
        master_segment_id = data.get("masterSegmentId")
        api_key = data.get("apiKey")
        instance = data.get("instance")
        output_segment_id = data.get("outputMasterSegmentId")
        master_segment_name = data.get("masterSegmentName")
        api_key_output = data.get("apiKeyOutput")
        copy_assets = data.get("copyAssets")
        copy_data_assets = data.get("copyDataAssets")

        # Convert boolean flags to strings for CLI
        copy_assets_str = str(copy_assets)
        copy_data_assets_str = str(copy_data_assets)

        # Create subprocess running the copier script
        proc = await asyncio.create_subprocess_exec(
            "python3", "copier.py",
            master_segment_id,
            api_key,
            instance,
            output_segment_id,
            master_segment_name,
            api_key_output,
            copy_assets_str,
            copy_data_assets_str,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Return streaming response with real-time updates
        return StreamingResponse(
            process_stream(proc),
            media_type="text/event-stream"
        )

    except Exception as e:
        # Return immediate error response for startup failures
        return StreamingResponse(
            iter([json.dumps({
                "type": "error",
                "message": f"Failed to start process: {str(e)}"
            }) + "\n"]),
            media_type="text/event-stream"
        )





