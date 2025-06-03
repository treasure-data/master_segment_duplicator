import logging
import os
import sys
from datetime import datetime

# Get the absolute path to the project directory
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(PROJECT_DIR, "mscopy.log")


class OutputLogger:
    def __init__(self, original_stream, filename):
        self.original_stream = original_stream
        self.filename = filename

    def write(self, message):
        # Write to original stream first
        self.original_stream.write(message)
        self.original_stream.flush()

        # Only log non-empty messages
        if message.strip():
            with open(self.filename, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                f.write(f"{timestamp} - {message}")
                f.flush()

    def flush(self):
        self.original_stream.flush()


def setup_logging():
    """Configure logging for all modules"""
    # Create log directory if it doesn't exist
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    root_logger.handlers = []

    # Create handlers
    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    console_handler = logging.StreamHandler()

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Set up stdout/stderr logging (simpler approach)
    sys.stdout = OutputLogger(sys.stdout, LOG_FILE)
    sys.stderr = OutputLogger(sys.stderr, LOG_FILE)

    # Test the logging configuration
    logger = logging.getLogger(__name__)
    logger.info("Logging initialized")
