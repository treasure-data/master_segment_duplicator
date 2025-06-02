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
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncio
import json
import os
from typing import AsyncGenerator, Optional


# Define request model
class CopyRequest(BaseModel):
    masterSegmentId: str
    apiKey: str
    instance: str
    # outputMasterSegmentId: str
    masterSegmentName: str
    apiKeyOutput: str
    copyAssets: bool = False  # Default value and explicit type
    copyDataAssets: bool = False  # Default value and explicit type

    class Config:
        json_encoders = {
            bool: lambda v: str(v).lower()  # Convert boolean to lowercase string
        }


app = FastAPI(
    title="Treasure Data Segment Copier",
    description="Web interface for copying Treasure Data segments between environments",
    version="1.0.0",
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


@app.get("/favicon.ico")
async def favicon():
    """
    Serves the favicon.ico file from the static directory.

    Returns:
        FileResponse: The favicon file response
    """
    return FileResponse("static/favicon.ico")


async def process_stream(proc) -> AsyncGenerator[str, None]:
    """Process stdout/stderr streams and yield formatted updates."""
    try:

        async def read_stream(stream, is_stderr=False):
            while True:
                try:
                    line = await stream.readline()
                    if not line:
                        break
                    text = line.decode().strip()
                    if text:
                        if is_stderr:
                            yield json.dumps(
                                {"type": "error", "message": f"Error: {text}"}
                            )
                        else:
                            yield json.dumps(
                                {
                                    "type": "progress" if "⚠️" not in text else "error",
                                    "message": text,
                                }
                            )
                except Exception as e:
                    print(f"Stream read error: {e}")
                    break

        # Create separate tasks for reading stdout and stderr
        stdout_gen = read_stream(proc.stdout)
        stderr_gen = read_stream(proc.stderr, is_stderr=True)

        async def merge_streams():
            async def safe_aiter(agen):
                try:
                    async for item in agen:
                        yield item
                except Exception as e:
                    print(f"Stream iteration error: {e}")

            # Merge both streams
            async for result in safe_aiter(stdout_gen):
                yield result
            async for result in safe_aiter(stderr_gen):
                yield result

        async for item in merge_streams():
            yield item

        # Wait for process to complete and capture return code
        return_code = await proc.wait()

        # Send final status after all stream processing is done
        yield json.dumps(
            {
                "type": "success" if return_code == 0 else "error",
                "message": (
                    "Process completed successfully!"
                    if return_code == 0
                    else f"Process failed with exit code {return_code}"
                ),
            }
        )
    except Exception as e:
        error_msg = str(e)
        print(f"Stream processing error: {error_msg}")
        yield json.dumps(
            {"type": "error", "message": f"Error processing stream: {error_msg}"}
        )


# @app.get("/submit")
# async def submit_form(
#     request: Request,
#     masterSegmentId: str,
#     apiKey: str,
#     instance: str,
#     # outputMasterSegmentId: str,
#     masterSegmentName: str,
#     apiKeyOutput: str,
#     copyAssets: bool,
#     copyDataAssets: bool,
# ):
#     """
#     Handles form submission via query parameters and streams responses.

#     Args:
#         request: FastAPI request object
#         masterSegmentId: ID of the master segment
#         apiKey: API key for authentication
#         instance: Instance name
#         outputMasterSegmentId: ID of the output master segment
#         masterSegmentName: Name of the master segment
#         apiKeyOutput: API key for the output environment
#         copyAssets: Flag to copy assets
#         copyDataAssets: Flag to copy data assets

#     Returns:
#         StreamingResponse: Real-time progress updates
#     """
#     try:
#         # Create subprocess running the copier script
#         proc = await asyncio.create_subprocess_exec(
#             "python3",
#             "copier.py",
#             masterSegmentId,
#             apiKey,
#             instance,
#             # outputMasterSegmentId,
#             masterSegmentName,
#             apiKeyOutput,
#             str(copyAssets).lower(),
#             str(copyDataAssets).lower(),
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE,
#         )

#         async def event_generator():
#             async for data in process_stream(proc):
#                 yield f"data: {data}\n\n"

#         # Return streaming response with proper SSE format
#         return StreamingResponse(
#             event_generator(),
#             media_type="text/event-stream",
#             headers={
#                 "Cache-Control": "no-cache",
#                 "Connection": "keep-alive",
#             },
#         )

#     except Exception as e:
#         error_message = f"Failed to start process: {str(e)}"
#         error_json = json.dumps({"type": "error", "message": error_message})
#         # Return immediate error response for startup failures
#         return StreamingResponse(
#             iter([f"data: {error_json}\n\n"]), media_type="text/event-stream"
#         )


@app.post("/submit")
async def submit_form(request: CopyRequest):
    """
    Handles form submission via POST request and streams responses.

    Args:
        request (CopyRequest): The copy request parameters

    Returns:
        StreamingResponse: Real-time progress updates
    """
    try:
        # Create subprocess running the copier script
        proc = await asyncio.create_subprocess_exec(
            "python3",
            "copier.py",
            request.masterSegmentId,
            request.apiKey,
            request.instance,
            # request.outputMasterSegmentId,
            request.masterSegmentName,
            request.apiKeyOutput,
            str(
                request.copyAssets
            ).lower(),  # Ensure boolean is converted to lowercase string
            str(
                request.copyDataAssets
            ).lower(),  # Ensure boolean is converted to lowercase string
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def event_generator():
            async for data in process_stream(proc):
                yield f"data: {data}\n\n"

        # Return streaming response with proper SSE format
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    except Exception as e:
        error_message = f"Failed to start process: {str(e)}"
        error_json = json.dumps({"type": "error", "message": error_message})
        # Return immediate error response for startup failures
        return StreamingResponse(
            iter([f"data: {error_json}\n\n"]), media_type="text/event-stream"
        )
