#!/usr/bin/env python3
"""
td_connector.py
--------------
Module for handling Treasure Data to Treasure Data data asset operations.

This module provides functionality to:
1. Establish connections to Treasure Data environments
2. Extract data references from segments
3. Deploy workflows to Treasure Data

Dependencies:
- requests: For HTTP operations
- urllib3: For retry functionality
- os, subprocess, tempfile, shutil, logging, tarfile, io, uuid: For workflow deployment
"""

import requests
from typing import Set, Tuple
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import os
import subprocess
import tempfile
import shutil
import logging
import tarfile
import io
import uuid
from logger_config import setup_logging

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)


class TDConnector:
    """
    Manages connections to Treasure Data environments.

    Attributes:
        base_url (str): Base URL for the Treasure Data API
        src_apikey (str): API key for the source environment
        dst_apikey (str): API key for the destination environment
        src_session (requests.Session): Session for source environment requests
        dst_session (requests.Session): Session for destination environment requests
    """

    def __init__(self, base_url: str, src_apikey: str, dst_apikey: str):
        self.base_url = base_url
        self.src_apikey = src_apikey
        self.dst_apikey = dst_apikey

        # Configure sessions with retries
        self.src_session = self._create_session()
        self.dst_session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Creates and configures a requests Session with retry logic."""
        session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def _make_request(
        self,
        session: requests.Session,
        method: str,
        endpoint: str,
        apikey: str,
        **kwargs,
    ) -> dict:
        """Makes an API request with proper headers and error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {"Authorization": f"TD1 {apikey}", "Content-Type": "application/json"}
        headers.update(kwargs.pop("headers", {}))

        response = session.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def get_segment_data_references(self, segment_id: str) -> Set[Tuple[str, str]]:
        """
        Extracts database and table references from a segment.
        Looks for parentDatabaseName and parentTableName in the segment JSON.

        Args:
            segment_id (str): ID of the segment to analyze

        Returns:
            Set[Tuple[str, str]]: Set of (database, table) tuples referenced by the segment
        """
        try:
            segment = self._make_request(
                self.src_session, "GET", f"audiences/{segment_id}", self.src_apikey
            )

            refs = set()

            # Helper function to process segment data
            def process_segment_data(data):
                try:
                    if isinstance(data, dict):
                        # Check for direct parentDatabase and parentTable references
                        db_name = data.get("parentDatabaseName")
                        table_name = data.get("parentTableName")

                        if db_name:
                            if not isinstance(db_name, str):
                                logger.warning(
                                    f"Invalid database name format: {db_name}"
                                )
                                return
                            if table_name and not isinstance(table_name, str):
                                logger.warning(
                                    f"Invalid table name format: {table_name}"
                                )
                                return
                            refs.add(
                                (db_name, table_name) if table_name else (db_name, None)
                            )

                        # Look for deep nested references
                        if "rule" in data:
                            rule = data["rule"]
                            if isinstance(rule, dict):
                                if "source" in rule:
                                    source = rule["source"]
                                    if isinstance(source, dict):
                                        db = source.get("database")
                                        table = source.get("table")
                                        if db:
                                            refs.add(
                                                (db, table) if table else (db, None)
                                            )

                        # Recursively process all dictionary values
                        for value in data.values():
                            process_segment_data(value)
                    elif isinstance(data, list):
                        # Recursively process all list items
                        for item in data:
                            process_segment_data(item)
                except Exception as e:
                    logger.error(f"Error processing segment data: {str(e)}")
                    # Continue processing other parts even if one fails

            # Start processing from the root
            process_segment_data(segment)

            if not refs:
                logger.info("No parent database/table references found in segment")
                print("ℹ️  No parent database/table references found in segment")
            else:
                logger.info(f"Found {len(refs)} data references")

            return refs

        except Exception as e:
            logger.error(f"Error getting data references: {str(e)}")
            print(f"⚠️  Error getting data references: {str(e)}")
            return set()


def clone_github_repo(repo_url: str, branch: str = "main") -> str:
    """
    Clones a GitHub repository into a temporary directory with improved error handling.

    Args:
        repo_url (str): URL of the GitHub repository
        branch (str): Branch name to clone (default: 'main')

    Returns:
        str: Path to the cloned repository

    Raises:
        Exception: If cloning fails
    """
    temp_dir = tempfile.mkdtemp(prefix="td_clone_")
    logger.info(f"Created temporary directory for git clone: {temp_dir}")

    try:
        # Check if git is installed
        try:
            subprocess.run(["git", "--version"], check=True, capture_output=True)
        except (subprocess.SubprocessError, FileNotFoundError):
            raise Exception("Git is not installed or not available in PATH")

        # Attempt the clone with detailed output
        process = subprocess.run(
            ["git", "clone", "-b", branch, repo_url, temp_dir],
            capture_output=True,
            text=True,
        )

        if process.returncode != 0:
            raise Exception(f"Git clone failed: {process.stderr}")

        # Verify the clone succeeded
        if not os.path.exists(os.path.join(temp_dir, ".git")):
            raise Exception("Git clone appeared to succeed but repository is invalid")

        logger.info(f"Successfully cloned repository to {temp_dir}")
        return temp_dir

    except Exception as e:
        # Clean up on failure
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(
                    f"Cleaned up temporary directory after failed clone: {temp_dir}"
                )
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up temporary directory: {cleanup_error}")

        error_msg = f"Failed to clone repository: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def find_digdag_project_dir(repo_path, project_dir=None):
    """
    Find the directory containing Digdag workflow files.
    If project_dir is specified, verify it exists and contains .dig files.
    Otherwise, find all directories with .dig files.

    Args:
        repo_path (str): Path to the repository
        project_dir (str, optional): Specific project directory

    Returns:
        list: List of project directories containing .dig files
    """
    if project_dir:
        full_path = os.path.join(repo_path, project_dir)
        if not os.path.isdir(full_path):
            raise FileNotFoundError(f"Project directory not found: {full_path}")

        # Check if the directory contains .dig files
        dig_files = [f for f in os.listdir(full_path) if f.endswith(".dig")]
        if not dig_files:
            raise FileNotFoundError(
                f"No .dig files found in project directory: {full_path}"
            )

        return [full_path]

    # Find all directories with .dig files
    project_dirs = set()
    for root, _, files in os.walk(repo_path):
        if any(f.endswith(".dig") for f in files):
            project_dirs.add(root)

    if not project_dirs:
        raise FileNotFoundError(
            "No directories with .dig files found in the repository"
        )

    logger.info(f"Found {len(project_dirs)} Digdag project directories")
    return list(project_dirs)


def create_project_archive(project_dir, project_name=None):
    """
    Create a tar.gz archive of the Digdag project directory.

    Args:
        project_dir (str): Path to the project directory
        project_name (str, optional): Name to use for the project

    Returns:
        tuple: (project_name, archive_bytes)
    """
    if not project_name:
        project_name = os.path.basename(project_dir)

    # Create a BytesIO object to hold the archive
    archive_buffer = io.BytesIO()

    # Create a tar.gz archive
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
        # Add all files in the project directory to the archive
        for root, _, files in os.walk(project_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, project_dir)
                tar.add(file_path, arcname=arcname)

    # Get the archive as bytes
    archive_buffer.seek(0)
    archive_bytes = archive_buffer.getvalue()

    logger.info(
        f"Created project archive for {project_name} ({len(archive_bytes)} bytes)"
    )
    return project_name, archive_bytes


def upload_project_to_td(
    project_name, archive_bytes, td_api_key, region, revision=None
):
    """
    Upload a Digdag project to Treasure Data.

    Args:
        project_name (str): Name of the project
        archive_bytes (bytes): Project archive as bytes
        td_api_key (str): Treasure Data API key
        region (str): Treasure Data Region of Instance
        revision (str, optional): Specific revision to use

    Returns:
        dict: API response
    """
    if region == "us":
        base_url = "https://api-workflow.treasuredata.com/api/projects"
    elif region == "eu":
        base_url = "https://api-workflow.eu01.treasuredata.com/api/projects"
    elif region == "jp":
        base_url = "https://api-workflow.treasuredata.co.jp/api/projects"
    elif region == "kr":
        base_url = "https://api-workflow.ap02.treasuredata.com/api/projects"
    else:
        raise ValueError(f"Unsupported region: {region}")

    if not revision:
        revision = str(uuid.uuid4())

    # Build the URL
    url = f"{base_url}?project={project_name}"
    if revision:
        url += f"&revision={revision}"

    headers = {"Authorization": f"TD1 {td_api_key}", "Content-Type": "application/gzip"}

    try:
        logger.info(f"Uploading project {project_name} to Treasure Data")
        response = requests.put(url, headers=headers, data=archive_bytes)
        response.raise_for_status()

        # Just return the status code if no JSON response
        try:
            return response.json()
        except ValueError:
            return {
                "status": "success",
                "status_code": response.status_code,
                "text": response.text,
            }

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
