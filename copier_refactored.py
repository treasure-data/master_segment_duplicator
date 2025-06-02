#!/usr/bin/env python3
"""
Treasure Data Segment Copier
----------------------------
A utility for copying segments and data assets between Treasure Data CDP environments.

This tool allows for:
1. Copying data assets between Treasure Data environments
2. Copying parent segments with their attributes
3. Copying folder structures and hierarchies
4. Copying segments with their rules and attributes
5. Copying journeys

Usage:
    python copier.py <src_parent_id> <src_api_key> <instance> <dst_parent_id> <dst_parent_name>
                    <dst_api_key> <copy_assets_flag> <copy_data_assets_flag>

Example:
    python copier.py 1234567890 td1_api_key_source US 9876543210 "New Segment" td1_api_key_dest true true
"""

import sys
import os
import time
import json
import yaml
import shutil
import tarfile
import io
import uuid
import datetime
import logging
from typing import Set, Tuple, List, Dict, Any, Optional, Union
from pathlib import Path

import requests
import networkx as nx
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# Configuration Constants
# -----------------------

# Set up logging
logging.basicConfig(
    filename="poc_hub.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# API Configuration
TD_MIME = "application/vnd.treasuredata.v1+json"
MAX_RETRIES = 40
RETRY_BACKOFF = 3
API_RATE_LIMIT = 2  # requests per second
WORKFLOW_TIMEOUT = 7200  # 2 hours in seconds
INITIAL_POLL_INTERVAL = 30  # Start with 30 seconds
MAX_POLL_INTERVAL = 300  # Max 5 minutes between checks

# Region-specific API endpoints
REGION_CDP = {
    "US": "https://api-cdp.treasuredata.com",
    "EMEA": "https://api-cdp.eu01.treasuredata.com",
    "Japan": "https://api-cdp.treasuredata.co.jp",
    "Korea": "https://api-cdp.ap02.treasuredata.com",
}

REGION_API = {
    "us": "api.treasuredata.com",
    "eu": "api.eu01.treasuredata.com",
    "jp": "api.treasuredata.co.jp",
    "kr": "api.ap02.treasuredata.com",
}

REGION_WORKFLOW = {
    "us": "api-workflow.treasuredata.com",
    "eu": "api-workflow.eu01.treasuredata.com",
    "jp": "api-workflow.treasuredata.co.jp",
    "kr": "api-workflow.ap02.treasuredata.com",
}

# VS Copy All workflow configuration
VS_COPY_REPO = "https://github.com/treasure-data/vs_copy_all.git"
WORKFLOW_NAME = "vs_copy_all"
WORKFLOW_POLL_INTERVAL = 60  # seconds to wait between workflow status checks


class RateLimiter:
    """
    Implements rate limiting for API calls to prevent throttling.

    Attributes:
        calls_per_second (int): Maximum number of allowed API calls per second
        last_call (float): Timestamp of the last API call
    """

    def __init__(self, calls_per_second: int = API_RATE_LIMIT):
        self.calls_per_second = calls_per_second
        self.last_call = 0

    def wait(self):
        """
        Implements the rate limiting logic by forcing appropriate delays between calls.
        Ensures that calls are spaced out to meet the rate limit requirements.
        """
        now = time.time()
        time_since_last = now - self.last_call
        if time_since_last < 1.0 / self.calls_per_second:
            time.sleep((1.0 / self.calls_per_second) - time_since_last)
        self.last_call = time.time()


class TDClient:
    """
    Client for interacting with Treasure Data API with built-in rate limiting and retries.

    Attributes:
        base_url (str): Base URL for the API endpoint
        api_key (str): API key for authentication
        rate_limiter (RateLimiter): Rate limiting component
        session (requests.Session): Session object with retry configuration
    """

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.rate_limiter = RateLimiter()

        # Configure session with retries for transient failures
        self.session = requests.Session()
        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],  # Common transient errors
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def request(self, method: str, path: str, **kwargs) -> dict:
        """
        Makes a rate-limited API request with proper error handling.

        Args:
            method (str): HTTP method (GET, POST, PUT, etc.)
            path (str): API endpoint path
            **kwargs: Additional arguments passed to requests.request()

        Returns:
            dict: Parsed JSON response

        Raises:
            requests.exceptions.RequestException: For any API request failures
        """
        self.rate_limiter.wait()
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Authorization": f"TD1 {self.api_key}", "Content-Type": TD_MIME}
        headers.update(kwargs.pop("headers", {}))

        try:
            response = self.session.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            print(f"⚠️  API request failed: {e}")
            if hasattr(e, "response") and hasattr(e.response, "text"):
                logger.error(f"Response: {e.response.text}")
                print(f"Response: {e.response.text}")
            raise


class TDConnector:
    """
    Manages connections to Treasure Data environments and extracts data references.

    This is a wrapper around TDClient that adds specific functionality for
    retrieving segment data references.
    """

    def __init__(self, base_url: str, src_apikey: str, dst_apikey: str):
        self.src_client = TDClient(base_url, src_apikey)
        self.dst_client = TDClient(base_url, dst_apikey)

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
            segment = self.src_client.request("GET", f"audiences/{segment_id}")
            refs = set()

            # Helper function to process segment data
            def process_segment_data(data):
                if isinstance(data, dict):
                    # Check for direct parentDatabase and parentTable references
                    db_name = data.get("parentDatabaseName")
                    table_name = data.get("parentTableName")

                    if db_name:
                        if not isinstance(db_name, str):
                            logger.warning(f"Invalid database name format: {db_name}")
                            return
                        if table_name and not isinstance(table_name, str):
                            logger.warning(f"Invalid table name format: {table_name}")
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
                                        refs.add((db, table) if table else (db, None))

                    # Recursively process all dictionary values
                    for value in data.values():
                        process_segment_data(value)
                elif isinstance(data, list):
                    # Recursively process all list items
                    for item in data:
                        process_segment_data(item)

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


def setup_clients(base: str, src_key: str, dst_key: str) -> tuple:
    """
    Sets up source and destination TDClient instances for API interactions.

    Args:
        base (str): Base URL for the API endpoint
        src_key (str): API key for the source environment
        dst_key (str): API key for the destination environment

    Returns:
        tuple: A tuple containing source and destination TDClient instances
    """
    return (TDClient(base, src_key), TDClient(base, dst_key))


def td2td_connection_create(
    con_name: str,
    con_description: str,
    src_api_key: str,
    dest_api_key: str,
    dest_url: str,
    region: str = "us",
) -> dict:
    """
    Creates a connection between two Treasure Data instances.

    Args:
        con_name (str): Name for the connection
        con_description (str): Description for the connection
        src_api_key (str): Source API key
        dest_api_key (str): Destination API key
        dest_url (str): Destination URL
        region (str, optional): Region code (us, eu, jp, kr). Defaults to "us".

    Returns:
        dict: Connection details from API response
    """
    url = f"https://{REGION_API[region]}/v4/connections"

    payload = json.dumps(
        {
            "name": con_name,
            "description": con_description,
            "type": "treasure_data",
            "settings": {"api_key": dest_api_key, "api_hostname": dest_url},
            "shared": False,
            "user": None,
            "permissions": {"update": True, "destroy": True},
        }
    )

    headers = {
        "Authorization": f"TD1 {src_api_key}",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code not in (200, 201):
        raise Exception(
            f"Request failed with status code: {response.status_code}, Error: {response.text}"
        )

    logger.info(f"Created TD2TD connection: {con_name}")
    print(response.text)
    return response.json()


class WorkflowManager:
    """
    Manages Treasure Data workflow operations for data transfer.

    This class handles creating, deploying, and monitoring workflows for
    copying data between Treasure Data instances.
    """

    def __init__(self, region: str, src_api_key: str):
        """
        Initialize the workflow manager.

        Args:
            region (str): Region code (us, eu, jp, kr)
            src_api_key (str): Source API key for authentication
        """
        self.region = region
        self.src_api_key = src_api_key
        self.base_url = f"https://{REGION_WORKFLOW[region]}"

    def create_config_yaml(
        self,
        connection_name: str,
        src_database: str,
        dest_database: str,
        folder: str = "vs_copy_all",
        filename: str = "config.yml",
    ) -> str:
        """
        Creates and saves a YAML configuration file for data transfer.

        Args:
          connection_name (str): The name of the TD2TD connection
          src_database (str): The name of the source database
          dest_database (str): The name of the destination database
          folder (str): The folder where the YAML file should be saved
          filename (str): The name of the YAML file

        Returns:
          str: The path to the saved YAML file
        """
        config = {
            "connection_name": connection_name,
            "src_database": src_database,
            "dest_database": dest_database,
            "mode": "replace",
            "copy_all_table": True,
        }

        # Create the folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)

        # Create the full file path
        filepath = os.path.join(folder, filename)

        # Save the configuration to the YAML file
        with open(filepath, "w") as yaml_file:
            yaml.dump(config, yaml_file, default_flow_style=False)

        logger.info(f"Created config YAML at {filepath}")
        return filepath

    def create_workflow_definition(
        self, folder: str = "vs_copy_all", filename: str = "vs_copy_all.dig"
    ) -> str:
        """
        Creates the Digdag workflow definition file.

        Args:
            folder (str): Folder to save the workflow file
            filename (str): Name of the workflow file

        Returns:
            str: Path to the created workflow file
        """
        workflow = """
_export:
  database: ${src_database}
  !include : 'config.yml'

+create:
  if>: ${copy_all_table}
  _do:
    +get_table_name:
      td_for_each>:
      query: "select table_name from information_schema.tables where table_schema = '${src_database}'"
      _parallel: true
      _do:
        +copy_data:
          td>:
          query: 'select * from "${src_database}"."${td.each.table_name}"'
          result_connection: ${connection_name}
          result_settings:
            user_database_name: "${dest_database}"
            user_table_name: "${td.each.table_name}"
            mode: ${mode}
  _else_do:
    +get_table_name:
      for_each>:
        table : ${tables_info}
      _parallel: true
      _do:
        +copy_data:
          td>:
          query: 'select * from "${src_database}"."${table.table_name}" where SUBSTRING(CAST(FROM_UNIXTIME(${table.date_column}) AS VARCHAR),1,10) between ${table.date_range}'
          result_connection: ${connection_name}
          result_settings:
            user_database_name: "${dest_database}"
            user_table_name: "${table.table_name}"
            mode: ${mode}
"""

        # Create the folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)

        # Create the full file path
        filepath = os.path.join(folder, filename)

        # Save the workflow to the file
        with open(filepath, "w") as yaml_file:
            yaml_file.write(workflow)

        logger.info(f"Created workflow definition at {filepath}")
        return filepath

    def create_project_archive(
        self, project_dir: str, project_name: str = None
    ) -> tuple:
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
        print(
            f"Created project archive for {project_name} ({len(archive_bytes)} bytes)"
        )
        return project_name, archive_bytes

    def upload_project(
        self, project_name: str, archive_bytes: bytes, revision: str = None
    ) -> dict:
        """
        Upload a Digdag project to Treasure Data.

        Args:
            project_name (str): Name of the project
            archive_bytes (bytes): Project archive as bytes
            revision (str, optional): Specific revision to use

        Returns:
            dict: API response
        """
        if not revision:
            revision = str(uuid.uuid4())

        # Build the URL
        url = f"{self.base_url}/api/projects?project={project_name}"
        if revision:
            url += f"&revision={revision}"

        headers = {
            "Authorization": f"TD1 {self.src_api_key}",
            "Content-Type": "application/gzip",
        }

        try:
            logger.info(f"Uploading project {project_name} to Treasure Data")
            print(f"Uploading project {project_name} to Treasure Data")
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
            print(f"API request failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            raise

    def get_workflow(self, project_id: str, workflow_name: str = "vs_copy_all") -> dict:
        """
        Get workflow details from a project.

        Args:
            project_id (str): Project ID
            workflow_name (str, optional): Workflow name. Defaults to "vs_copy_all".

        Returns:
            dict: Workflow details with workflow_id and project_id
        """
        url = f"{self.base_url}/api/projects/{project_id}/workflows/{workflow_name}"
        headers = {
            "Authorization": f"TD1 {self.src_api_key}",
            "Content-Type": "application/json",
        }

        response = requests.request("GET", url, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f"Request failed with status code: {response.status_code}, Error: {response.text}"
            )

        logger.info(f"Retrieved workflow details for {workflow_name}")
        print(f"Retrieved workflow: {url}")
        print(f"Response: {response.text}")
        res_json = response.json()
        return {
            "workflow_id": res_json.get("id"),
            "project_id": res_json.get("project").get("id"),
        }

    def run_workflow(self, workflow_id: str) -> dict:
        """
        Run a workflow by ID.

        Args:
            workflow_id (str): Workflow ID to run

        Returns:
            dict: API response containing run details
        """
        now = datetime.datetime.now()
        date_time_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/api/attempts"
        payload = json.dumps(
            {"workflowId": workflow_id, "sessionTime": date_time_str, "params": {}}
        )

        headers = {
            "Authorization": f"TD1 {self.src_api_key}",
            "Content-Type": "application/json",
        }

        response = requests.request("PUT", url, headers=headers, data=payload)
        if response.status_code != 200:
            raise Exception(
                f"Request failed with status code: {response.status_code}, Error: {response.text}"
            )

        logger.info(f"Started workflow run: {workflow_id}")
        print(f"Started workflow run: {workflow_id}")
        print(response.text)
        return response.json()

    def get_workflow_status(self, run_id: str) -> dict:
        """
        Get the status of a workflow run.

        Args:
            run_id (str): Workflow run ID

        Returns:
            dict: Status details
        """
        url = f"{self.base_url}/api/attempts/{run_id}"
        headers = {
            "Authorization": f"TD1 {self.src_api_key}",
            "Content-Type": "application/json",
        }

        response = requests.request("GET", url, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f"Request failed with status code: {response.status_code}, Error: {response.text}"
            )

        logger.info(f"Retrieved status for workflow run: {run_id}")
        return response.json()

    def deploy_workflow(self, project_dir: str, project_name: str) -> dict:
        """
        Deploy a workflow project to Treasure Data.

        Args:
            project_dir (str): Directory containing the workflow project
            project_name (str): Name for the project

        Returns:
            dict: Deployment results
        """
        try:
            # Create project archive
            _, archive_bytes = self.create_project_archive(project_dir, project_name)

            # Upload to Treasure Data
            result = self.upload_project(project_name, archive_bytes)

            logger.info(f"Successfully deployed project: {project_name}")
            print(f"Successfully deployed project: {project_name}")
            print(f"Result: {result}")
            return result

        except Exception as e:
            logger.error(f"Error deploying workflow: {e}")
            print(f"Error deploying workflow: {e}")
            raise
        finally:
            # Clean up temporary directory
            try:
                if os.path.exists(project_dir):
                    shutil.rmtree(project_dir)
                    logger.info(f"Cleaned up directory: {project_dir}")
                    print(f"Cleaned up directory: {project_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up directory {project_dir}: {e}")
                print(f"Error cleaning up directory {project_dir}: {e}")


def create_td_copy_workflow(
    wf_manager: WorkflowManager,
    connection_name: str,
    src_database: str,
    dest_database: str,
    folderpath: str = "vs_copy_all",
) -> tuple:
    """
    Create a Treasure Data copy workflow.

    Args:
        wf_manager (WorkflowManager): Workflow manager instance
        connection_name (str): TD2TD connection name
        src_database (str): Source database name
        dest_database (str): Destination database name
        folderpath (str, optional): Folder for the workflow. Defaults to "vs_copy_all".

    Returns:
        tuple: (yml_filepath, dig_filepath, folderpath)
    """
    # Create project directory
    project_dir = os.path.join(os.getcwd(), folderpath)
    os.makedirs(project_dir, exist_ok=True)

    # Create workflow files
    yml_filepath = wf_manager.create_config_yaml(
        connection_name,
        src_database,
        dest_database,
        folder=project_dir,
        filename="config.yml",
    )

    dig_filepath = wf_manager.create_workflow_definition(
        folder=project_dir, filename="vs_copy_all.dig"
    )

    return yml_filepath, dig_filepath, project_dir


def post_journey_folder(data: dict, dst_client: TDClient) -> None:
    """
    Post journey data to the destination environment.

    Args:
        data (dict): Journey data to post
        dst_client (TDClient): Destination client
    """
    url_path = "entities/journeys"
    journeys = data.get("data", [])

    for journey in journeys:
        try:
            response = dst_client.request("POST", url_path, json={"data": [journey]})

            if not (200 <= response.get("status", 200) < 300):
                logger.warning(
                    f"Request failed for journey {journey.get('id', '')}: {response}"
                )
                print(f"Request failed for journey {journey.get('id', '')}: {response}")

        except Exception as e:
            logger.error(f"Error posting journey: {e}")
            print(f"Error posting journey: {e}")


def copy_data_assets(
    src_parent: str, src_key: str, dst_key: str, dest_url: str, region: str = "us"
) -> None:
    """
    Copies data assets using workflows. Runs a separate workflow for each database
    found in the source parent segment.

    Args:
        src_parent (str): Source parent segment ID
        src_key (str): API key for source environment
        dst_key (str): API key for destination environment
        dest_url (str): Destination URL
        region (str, optional): Region code. Defaults to "us".
    """
    print("\n⏩ Initializing data assets copy...")
    logger.info("Starting data assets copy")

    now = datetime.datetime.now()
    int_timestamp = int(now.timestamp())
    connection_name = f"mscopy_td2td_{int_timestamp}"

    try:
        # Initialize workflow manager
        wf_manager = WorkflowManager(region, src_key)

        # Get data references from segment
        connector = TDConnector(REGION_CDP[region.upper()], src_key, dst_key)

        # Create connection between instances
        td2td_connection_create(
            connection_name,
            f"Auto generated connector for MS copy",
            src_key,
            dst_key,
            dest_url,
            region,
        )

        # Get data references from segment
        data_refs = connector.get_segment_data_references(src_parent)

        if not data_refs:
            print("ℹ️ No data references found in the segment")
            return

        # Group references by database
        databases = {}
        for db, table in sorted(data_refs):
            if db not in databases:
                databases[db] = []
            if table:
                databases[db].append(table)

        print(f"\n📊 Found {len(databases)} databases to copy:")
        workflows = []  # Track all running workflows

        # Start all workflows
        for i, (db, tables) in enumerate(databases.items()):
            print(f"  • Database: {db} ({len(tables)} tables)")
            for table in tables:
                print(f"    - {table}")

            # Create workflow files
            _, _, project_dir = create_td_copy_workflow(
                wf_manager, connection_name, db, db, folderpath=f"vs_copy_all_{db}"
            )

            # Deploy workflow
            project_result = wf_manager.deploy_workflow(
                project_dir, f"ms_segment_copy_{int_timestamp}_{i}"
            )

            project_id = project_result.get("id")

            # Get workflow details
            workflow_info = wf_manager.get_workflow(project_id)
            workflow_id = workflow_info.get("workflow_id")

            # Run workflow
            run_info = wf_manager.run_workflow(workflow_id)
            run_id = run_info.get("id")

            # Track workflow
            workflows.append(
                {
                    "run_id": run_id,
                    "db_name": db,
                    "start_time": time.time(),
                    "poll_interval": INITIAL_POLL_INTERVAL,
                    "consecutive_errors": 0,
                    "status": "running",
                }
            )

            print(f"   Started workflow {run_id} for database {db}")

        # Monitor all workflows with exponential backoff
        start_time = time.time()
        completed = 0
        failed = 0

        while workflows and time.time() - start_time < WORKFLOW_TIMEOUT:
            for wf in workflows[:]:  # Use slice to allow removal during iteration
                try:
                    # Check if workflow has timed out
                    if time.time() - wf["start_time"] > WORKFLOW_TIMEOUT:
                        print(f"⚠️ Workflow for {wf['db_name']} timed out")
                        workflows.remove(wf)
                        failed += 1
                        continue

                    # Only poll if enough time has passed
                    if time.time() - wf.get("last_check", 0) < wf["poll_interval"]:
                        continue

                    # Get workflow status
                    run_dict = wf_manager.get_workflow_status(wf["run_id"])
                    status = run_dict.get("status")
                    wf["last_check"] = time.time()
                    wf["consecutive_errors"] = 0

                    print(f"   • Database {wf['db_name']}: {status}")

                    # Handle completed workflow
                    if status in ["success", "completed"]:
                        print(f"✅ Workflow for {wf['db_name']} completed successfully")
                        workflows.remove(wf)
                        completed += 1
                    # Handle failed workflow
                    elif status in ["error", "killed", "failed"]:
                        print(f"❌ Workflow for {wf['db_name']} failed: {status}")
                        workflows.remove(wf)
                        failed += 1
                    # Handle running workflow - adjust poll interval
                    else:
                        # Use exponential backoff for polling
                        wf["poll_interval"] = min(
                            wf["poll_interval"] * 1.5, MAX_POLL_INTERVAL
                        )
                        logger.info(
                            f"Workflow for {wf['db_name']} still running, next check in {wf['poll_interval']:.1f}s"
                        )

                except requests.exceptions.RequestException as e:
                    wf["consecutive_errors"] += 1
                    print(
                        f"⚠️ Error checking status for {wf['db_name']} (attempt {wf['consecutive_errors']}/{MAX_RETRIES}): {str(e)}"
                    )

                    if wf["consecutive_errors"] >= MAX_RETRIES:
                        print(
                            f"❌ Max retries reached for {wf['db_name']}, marking as failed"
                        )
                        workflows.remove(wf)
                        failed += 1
                    else:
                        # Reduce poll interval when errors occur to retry sooner
                        wf["poll_interval"] = INITIAL_POLL_INTERVAL

            # Short sleep to prevent tight loop
            if workflows:
                time.sleep(1)

        # Handle remaining workflows that hit the global timeout
        if workflows:
            print(
                f"\n❌ Global timeout reached after {WORKFLOW_TIMEOUT/3600:.1f} hours"
            )
            failed += len(workflows)

        # Print final status
        total = completed + failed
        print(f"\n📊 Final Status:")
        print(f"   • Total workflows: {total}")
        print(f"   • Completed successfully: {completed}")
        print(f"   • Failed: {failed}")

        if failed > 0:
            raise Exception(f"Data copy failed for {failed} out of {total} databases")

        print(f"\n✅ Data assets copy completed for all databases")

    except Exception as e:
        logger.error(f"Error during data assets copy: {str(e)}")
        print(f"\n❌ Error during data assets copy: {str(e)}")
        raise


def copy_folders_segments(
    src_client: TDClient, dst_client: TDClient, src_parent: str, dst_parent: str
) -> None:
    """
    Copies folder structures and segments from source to destination environment.

    Args:
        src_client (TDClient): Source TDClient instance
        dst_client (TDClient): Destination TDClient instance
        src_parent (str): Source parent segment ID
        dst_parent (str): Destination parent segment ID
    """
    print("⏩  Copying folders & segments...")
    logger.info(f"Starting folder and segment copy from {src_parent} to {dst_parent}")

    start_time = time.time()
    timestamp_suffix = f"_copy_{int(time.time())}"

    # Tracking containers for reporting
    skipped_segments = []
    failed_segments = []
    skipped_journeys = []
    failed_journeys = []

    try:
        # Get source root folder
        src_root = src_client.request("GET", f"entities/parent_segments/{src_parent}")[
            "data"
        ]["relationships"]["parentSegmentFolder"]["data"]["id"]

        # Get destination root folder
        dst_root = dst_client.request("GET", f"entities/parent_segments/{dst_parent}")[
            "data"
        ]["relationships"]["parentSegmentFolder"]["data"]["id"]

        # Fetch all entities
        entities = src_client.request("GET", f"entities/by-folder/{src_root}?depth=32")[
            "data"
        ]

        # Maps to track copied items
        folders_map = {src_root: dst_root}
        segments_map = {}

        # Copy folders first (topological order)
        folder_entities = [e for e in entities if e["type"] == "folder-segment"]
        if folder_entities:
            print("\n  📂 Copying folders...")

            # Build dependency graph
            g = nx.DiGraph()
            for e in folder_entities:
                parent = e["relationships"]["parentFolder"]["data"]
                if parent:
                    g.add_edge(e["id"], parent["id"])
                else:
                    g.add_node(e["id"])

            # Process folders in topological order (reversed to start from root)
            for i, fid in enumerate(reversed(list(nx.topological_sort(g))), 1):
                if fid == src_root:  # skip root
                    continue

                try:
                    # Get folder entity
                    ent = next(e for e in folder_entities if e["id"] == fid)
                    parent_src = ent["relationships"]["parentFolder"]["data"]["id"]

                    # Update folder reference to use destination ID
                    ent["relationships"]["parentFolder"]["data"]["id"] = folders_map[
                        parent_src
                    ]

                    original_name = ent["attributes"]["name"]
                    try:
                        # Try with original name first
                        result = dst_client.request(
                            "POST", "entities/folders", json=ent
                        )
                    except requests.exceptions.RequestException as e:
                        if (
                            hasattr(e.response, "status_code")
                            and e.response.status_code == 400
                            and "Name has already been taken" in e.response.text
                        ):
                            # If name conflict, add suffix and retry
                            ent["attributes"][
                                "name"
                            ] = f"{original_name}{timestamp_suffix}"
                            print(
                                f"    Note: Renaming folder to {ent['attributes']['name']} due to name conflict"
                            )
                            result = dst_client.request(
                                "POST", "entities/folders", json=ent
                            )
                        else:
                            # For other errors, propagate
                            raise

                    # Store mapping from source to destination ID
                    new_id = result["data"]["id"]
                    folders_map[fid] = new_id
                    print(
                        f"    [{i}/{len(folder_entities)}] {ent['attributes']['name']}  →  {new_id}"
                    )

                except Exception as e:
                    logger.error(f"Failed to copy folder {fid}: {str(e)}")
                    print(f"    ⚠️ Failed to copy folder {fid}: {str(e)}")
                    # Skip this folder but continue with others

        # Copy journeys (outside folders, e.g. in root or orphaned)
        journey_entities = [e for e in entities if e["type"].startswith("journey")]
        if journey_entities:
            print("\n🧭 Copying journeys...")
            for i, journey in enumerate(journey_entities, 1):
                try:
                    # Determine the source parent folder ID
                    parent_src = None
                    if (
                        "relationships" in journey
                        and "parentFolder" in journey["relationships"]
                        and "data" in journey["relationships"]["parentFolder"]
                        and "id" in journey["relationships"]["parentFolder"]["data"]
                    ):
                        parent_src = journey["relationships"]["parentFolder"]["data"][
                            "id"
                        ]
                    else:
                        skipped_journeys.append(
                            journey.get(
                                "id",
                                journey.get("attributes", {}).get("name", "unknown"),
                            )
                        )
                        print(
                            f"    ⚠️ Skipping journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: missing parentFolder"
                        )
                        continue

                    # Map to destination folder ID
                    if parent_src not in folders_map:
                        skipped_journeys.append(
                            journey.get(
                                "id",
                                journey.get("attributes", {}).get("name", "unknown"),
                            )
                        )
                        print(
                            f"    ⚠️ Skipping journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: parent folder not found in destination"
                        )
                        continue

                    # Update journey with destination folder ID
                    journey["relationships"]["parentFolder"]["data"]["id"] = (
                        folders_map[parent_src]
                    )

                    # Post journey to destination
                    post_journey_folder({"data": [journey]}, dst_client)
                    print(
                        f"    [{i}/{len(journey_entities)}] Copied journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))} to folder {folders_map[parent_src]}"
                    )

                except Exception as e:
                    failed_journeys.append(
                        journey.get(
                            "id", journey.get("attributes", {}).get("name", "unknown")
                        )
                    )
                    logger.error(f"Failed to copy journey: {str(e)}")
                    print(
                        f"    ⚠️ Failed to copy journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: {str(e)}"
                    )

            print(
                f"    Copied {len(journey_entities) - len(failed_journeys) - len(skipped_journeys)} journeys"
            )

        # Copy segments next (handle dependencies)
        segment_entities = [e for e in entities if e["type"].startswith("segment")]
        if segment_entities:
            print("\n  🔖 Copying segments...")

            # Build dependency graph
            dep = {e["id"]: [] for e in segment_entities}
            for e in segment_entities:
                if "rule" in e["attributes"]:
                    for c in e["attributes"]["rule"].get("conditions", []):
                        for s in c.get("conditions", []):
                            if s.get("type") == "Reference":
                                ref_id = s.get("value", {}).get("segmentId")
                                if ref_id and ref_id in dep:
                                    dep[e["id"]].append(ref_id)

            # Process segments in topological order
            for i, sid in enumerate(
                reversed(list(nx.topological_sort(nx.DiGraph(dep)))), 1
            ):
                try:
                    ent = next(e for e in segment_entities if e["id"] == sid)
                    original_name = ent["attributes"]["name"]

                    # Update folder reference and audience ID
                    parent_src = ent["relationships"]["parentFolder"]["data"]["id"]
                    if parent_src not in folders_map:
                        print(
                            f"    ⚠️ Skipping segment {original_name} due to missing parent folder"
                        )
                        skipped_segments.append(
                            f"{original_name} (missing parent folder)"
                        )
                        continue

                    ent["relationships"]["parentFolder"]["data"]["id"] = folders_map[
                        parent_src
                    ]
                    ent["attributes"]["audienceId"] = dst_parent

                    # Update any segment references
                    has_missing_refs = False
                    if "rule" in ent["attributes"]:
                        try:
                            for c in ent["attributes"]["rule"].get("conditions", []):
                                for s in c.get("conditions", []):
                                    if s.get("type") == "Reference":
                                        ref = s.get("value", {})
                                        if (
                                            "segmentId" in ref
                                            and ref["segmentId"] in segments_map
                                        ):
                                            ref["segmentId"] = segments_map[
                                                ref["segmentId"]
                                            ]
                                        else:
                                            has_missing_refs = True
                        except Exception as e:
                            logger.error(
                                f"Error updating references for {original_name}: {str(e)}"
                            )
                            print(
                                f"    ⚠️ Error updating references for {original_name}: {str(e)}"
                            )
                            has_missing_refs = True

                    try:
                        # Try with original name first
                        result = dst_client.request(
                            "POST", "entities/segments", json=ent
                        )
                    except requests.exceptions.RequestException as e:
                        if (
                            hasattr(e.response, "status_code")
                            and e.response.status_code == 400
                        ):
                            error_text = (
                                e.response.text if hasattr(e.response, "text") else ""
                            )
                            if "Name has already been taken" in error_text:
                                # If name conflict, add suffix and retry
                                ent["attributes"][
                                    "name"
                                ] = f"{original_name}{timestamp_suffix}"
                                print(
                                    f"    Note: Renaming segment to {ent['attributes']['name']} due to name conflict"
                                )
                                result = dst_client.request(
                                    "POST", "entities/segments", json=ent
                                )
                            elif "Referencing predictive segment" in error_text:
                                print(
                                    f"    Note: Skipping segment {original_name} as it references a predictive segment"
                                )
                                skipped_segments.append(
                                    f"{original_name} (predictive segment)"
                                )
                                continue
                            else:
                                # For other 400 errors, log and continue
                                error_msg = error_text if error_text else str(e)
                                logger.error(
                                    f"Failed to copy segment {original_name}: {error_msg}"
                                )
                                print(
                                    f"    ⚠️ Failed to copy segment {original_name}: {error_msg}"
                                )
                                failed_segments.append(original_name)
                                continue
                        else:
                            raise

                    # Store mapping from source to destination ID
                    new_id = result["data"]["id"]
                    segments_map[sid] = new_id
                    status = "with missing refs" if has_missing_refs else "→"
                    print(
                        f"    [{i}/{len(segment_entities)}] {ent['attributes']['name']} {status} {new_id}"
                    )

                except Exception as e:
                    logger.error(f"Failed to copy segment {original_name}: {str(e)}")
                    print(f"    ⚠️ Failed to copy segment {original_name}: {str(e)}")
                    failed_segments.append(original_name)
                    continue  # Continue with next segment

        # Print summary
        duration = time.time() - start_time
        print(f"\n✅  Folders & segments copy finished in {duration:.1f}s")
        print(f"   • Copied {len(folders_map)-1} folders")
        print(f"   • Copied {len(segments_map)} segments")
        if skipped_segments:
            print(f"   • Skipped {len(skipped_segments)} segments:")
            for name in skipped_segments:
                print(f"     - {name}")
        if failed_segments:
            print(f"   • Failed to copy {len(failed_segments)} segments:")
            for name in failed_segments:
                print(f"     - {name}")
        if skipped_journeys:
            print(f"   • Skipped {len(skipped_journeys)} journeys:")
            for name in skipped_journeys:
                print(f"     - {name}")
        if failed_journeys:
            print(f"   • Failed to copy {len(failed_journeys)} journeys:")
            for name in failed_journeys:
                print(f"     - {name}")
        print()

    except Exception as e:
        logger.error(f"Error during folders/segments copy: {str(e)}")
        print(f"⚠️  Error during folders/segments copy: {str(e)}\n")
        if len(segments_map) > 0:
            print("   Some segments were copied successfully before the error occurred")
            print(f"   • Successfully copied {len(segments_map)} segments")
        raise


def main():
    """
    Main entry point for the segment copy process.
    Parses CLI arguments and orchestrates the copy operations.
    """
    if len(sys.argv) < 9:
        print(
            "Usage: python copier.py "
            "<src_parent_id> <src_api_key> "
            "<instance> "
            "<dst_parent_id> <dst_parent_name> <dst_api_key> "
            "<copy_assets_flag> <copy_data_assets_flag>"
        )
        sys.exit(1)

    # Parse arguments
    (
        src_parent,
        src_key,
        instance,
        dst_parent,
        dst_name,
        dst_key,
        copy_assets_flag,
        copy_data_assets_flag,
    ) = sys.argv[1:]

    # Ensure flags are processed as strings before converting to bool
    copy_assets = str(copy_assets_flag).lower() == "true"
    should_copy_data_assets = str(copy_data_assets_flag).lower() == "true"
    base = REGION_CDP.get(instance, REGION_CDP["US"])

    print(
        f"\n🚀 Starting segment copy process at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"📍 Configuration Summary:")
    print(f"   • Region: {instance}")
    print(f"   • API Endpoint: {base}")
    print(f"   • Source Segment ID: {src_parent}")
    print(f"   • Destination: {dst_parent} ('{dst_name}')")
    print(f"   • Copy Assets: {'Yes' if copy_assets else 'No'}")
    print(f"   • Copy Data: {'Yes' if should_copy_data_assets else 'No'}")

    # Map instance to region code
    if instance == "US":
        region = "us"
        dest_url = "api.treasuredata.com"
    elif instance == "EMEA":
        region = "eu"
        dest_url = "api.eu01.treasuredata.com"
    elif instance == "Japan":
        region = "jp"
        dest_url = "api.treasuredata.co.jp"
    elif instance == "Korea":
        region = "kr"
        dest_url = "api.ap02.treasuredata.com"
    else:
        print(f"⚠️ Unknown region: {instance}. Defaulting to US.")
        region = "us"
        dest_url = "api.treasuredata.com"

    try:
        print("\n⚙️  Initializing API clients...")
        src_client, dst_client = setup_clients(base, src_key, dst_key)
        print("✅ API clients initialized successfully")

        # 1. Copy data assets first if requested
        if should_copy_data_assets:
            print("\n📊 Starting data assets copy phase...")
            copy_data_assets(src_parent, src_key, dst_key, dest_url, region=region)
            print("✅ Data assets copy phase completed")
        else:
            print("\nℹ️  Skipping data assets phase (copy_data_assets=False)")

        # 2. Get & copy parent segment
        print("\n🔄 Starting parent segment copy phase...")
        print("   • Fetching source parent segment...")
        orig = src_client.request("GET", f"audiences/{src_parent}")
        print("   • Source parent segment retrieved successfully")

        # 3. Create destination parent segment
        print("   • Creating destination parent segment...")
        orig["id"], orig["name"] = dst_parent, dst_name
        dst_client.request("PUT", f"audiences/{dst_parent}", json=orig)
        print("✅ Parent segment copy phase completed")

        # 4. Copy folders & segments if requested
        if copy_assets:
            print("\n📁 Starting folder and segment copy phase...")
            copy_folders_segments(src_client, dst_client, src_parent, dst_parent)
            print("✅ Folder and segment copy phase completed")
        else:
            print("\nℹ️  Skipping folders/segments phase (copy_assets=False)")

        print("\n✨ Segment copy process completed successfully!")
        print("   All requested operations have been performed without errors.")

    except Exception as e:
        logger.error(f"Error occurred during copy process: {str(e)}")
        print(f"\n❌ Error occurred during copy process:")
        print(f"   {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
