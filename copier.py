#!/usr/bin/env python3
"""
copier.py
---------
A comprehensive tool for copying Treasure Data audience segments between environments.

This module provides functionality to:
1. Copy parent segments between Treasure Data environments
2. Copy associated folder structures and child segments
3. Copy related data assets (databases and tables) if requested
4. Handle segment dependencies and maintain referential integrity

Architecture:
- Uses TDConnector (from td_connector.py) for data asset operations
- Implements rate limiting for API calls
- Handles retries for transient failures
- Maintains topological ordering for segment dependencies

CLI Usage:
  python copier.py \\
         <src_parent_id> <src_api_key> \\
         <instance> \\
         <dst_parent_id> <dst_parent_name> <dst_api_key> \\
         <copy_assets_flag> <copy_data_assets_flag>

Dependencies:
- networkx: For dependency graph management
- requests: For HTTP operations
- td_connector: Custom module for data asset operations
"""
import sys, json, time, requests, networkx as nx
import subprocess
import os
import tempfile
import shutil
import logging
import tarfile
import io
import uuid
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from td_connector import (
    TDConnector,
    clone_github_repo,
    find_digdag_project_dir,
    create_project_archive,
    upload_project_to_td,
)
import requests
import json
import datetime
import yaml
import os
import shutil
import os
import tarfile
import io
import uuid
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
from parent_segment_api import ps_check_and_update


# Set up logging
logging.basicConfig(
    filename="poc_hub.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# API Configuration Constants
TD_MIME = "application/vnd.treasuredata.v1+json"
MAX_RETRIES = 40  # Increased from 3 to 40
RETRY_BACKOFF = 3  # Keep backoff factor at 3
API_RATE_LIMIT = 2  # requests per second
WORKFLOW_TIMEOUT = 7200  # 2 hours in seconds
INITIAL_POLL_INTERVAL = 30  # Start with 30 seconds
MAX_POLL_INTERVAL = 300  # Max 5 minutes between checks

# Region-specific API endpoints
REGION = {
    "US": "https://api-cdp.treasuredata.com",
    "EMEA": "https://api-cdp.eu01.treasuredata.com",
    "Japan": "https://api-cdp.treasuredata.co.jp",
    "Korea": "https://api-cdp.ap02.treasuredata.com",
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
            print(f"⚠️  API request failed: {e}")
            if hasattr(e.response, "text"):
                print(f"Response: {e.response.text}")
            raise


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


def deploy_vs_copy_workflow(client: TDClient, workflow_dir: str) -> None:
    """
    Deploys the VS Copy All workflow to Treasure Data.

    Args:
        client (TDClient): TDClient instance for API calls
        workflow_dir (str): Directory containing the workflow files
    """
    print("\n⏩ Deploying VS Copy All workflow...")
    try:
        # Map region from API endpoint to workflow region
        region_map = {
            "treasuredata.com": "us",
            "eu01.treasuredata.com": "eu",
            "treasuredata.co.jp": "jp",
            "ap02.treasuredata.com": "kr",
        }

        region = "us"  # Default to US
        for endpoint, reg in region_map.items():
            if endpoint in client.base_url:
                region = reg
                break

        project_name = WORKFLOW_NAME

        # Clone the repository if not exists
        repo_path = clone_github_repo(VS_COPY_REPO, "main")

        try:
            # Find project directories
            project_dirs = find_digdag_project_dir(repo_path, workflow_dir)

            # Create and upload project archive
            _, archive_bytes = create_project_archive(project_dirs[0], project_name)

            # Upload to Treasure Data
            result = upload_project_to_td(
                project_name, archive_bytes, client.api_key, region
            )

            # Verify workflow exists via API
            try:
                workflow_info = client.request("GET", f"v1/projects")
                if not any(wf.get("name") == WORKFLOW_NAME for wf in workflow_info):
                    raise Exception(
                        f"Workflow {WORKFLOW_NAME} not found after deployment"
                    )
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Warning: Could not verify workflow deployment: {str(e)}")

            print("✅ Workflow deployed successfully")
            return result

        finally:
            # Clean up temporary directory
            if os.path.exists(repo_path):
                shutil.rmtree(repo_path)

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to deploy workflow: {error_msg}")
        raise


def run_vs_copy_workflow(
    client: TDClient, data_refs: Set[Tuple[str, str]], src_key: str, dst_key: str
) -> None:
    """
    Runs the VS Copy All workflow with the identified tables.

    Args:
        client (TDClient): TDClient instance
        data_refs (Set[Tuple[str, str]]): Set of (database, table) tuples to copy
        src_key (str): Source API key
        dst_key (str): Destination API key
    """
    print("\n⏩ Starting VS Copy All workflow...")
    try:
        # Prepare workflow parameters
        params = {
            "src_apikey": src_key,
            "dst_apikey": dst_key,
            "databases": [],
        }

        # Group tables by database
        db_tables = {}
        for db, table in data_refs:
            if db not in db_tables:
                db_tables[db] = []
            if table:  # Some refs might be database-only
                db_tables[db].append(table)

        # Format parameters for workflow
        for db, tables in db_tables.items():
            params["databases"].append(
                {
                    "name": db,
                    "tables": (
                        tables if tables else ["*"]
                    ),  # Use * if no specific tables
                }
            )

        # Start workflow using new API endpoint structure
        try:
            # First try the v1 endpoint
            result = client.request(
                "POST", f"v1/projects/{WORKFLOW_NAME}/start", json={"params": params}
            )
        except requests.exceptions.RequestException:
            # Fall back to legacy endpoint if v1 fails
            try:
                result = client.request(
                    "POST", f"projects/{WORKFLOW_NAME}/start", json={"params": params}
                )
            except requests.exceptions.RequestException as e:
                print(f"❌ Both v1 and legacy workflow endpoints failed")
                raise e

        session_id = result.get("id") or result.get("job_id")
        if not session_id:
            raise Exception("Failed to get workflow session/job ID")

        print(f"✅ Workflow started with ID: {session_id}")

        # Monitor workflow progress using appropriate endpoint
        endpoint = f"v1/projects/{WORKFLOW_NAME}/sessions/{session_id}/status"
        legacy_endpoint = f"projects/{WORKFLOW_NAME}/jobs/{session_id}/status"

        start_time = time.time()
        poll_interval = INITIAL_POLL_INTERVAL
        consecutive_errors = 0

        while True:
            # Check for timeout
            if time.time() - start_time > WORKFLOW_TIMEOUT:
                raise Exception(
                    f"Workflow timed out after {WORKFLOW_TIMEOUT/3600:.1f} hours"
                )

            try:
                try:
                    status = client.request("GET", endpoint)
                except requests.exceptions.RequestException:
                    status = client.request("GET", legacy_endpoint)

                consecutive_errors = 0  # Reset error count on success
                state = status.get("state") or status.get("status", "unknown")
                print(f"   • Workflow status: {state}")

                if state in ["success", "completed"]:
                    print("✅ Data copy completed successfully")
                    break
                elif state in ["error", "killed", "failed"]:
                    error_msg = status.get("error") or status.get(
                        "message", "Unknown error"
                    )
                    raise Exception(
                        f"Workflow failed with state: {state} - {error_msg}"
                    )

                # Increase poll interval with exponential backoff
                poll_interval = min(poll_interval * 1.5, MAX_POLL_INTERVAL)

            except requests.exceptions.RequestException as e:
                consecutive_errors += 1
                if consecutive_errors >= MAX_RETRIES:
                    print(
                        f"❌ Failed to check workflow status after {MAX_RETRIES} retries"
                    )
                    raise

                print(
                    f"⚠️ Error checking status (attempt {consecutive_errors}/{MAX_RETRIES}): {str(e)}"
                )
                # Use shorter poll interval when errors occur
                poll_interval = INITIAL_POLL_INTERVAL

            time.sleep(poll_interval)

    except Exception as e:
        print(f"❌ Workflow execution failed: {str(e)}")
        raise


def td2td_connection_create(
    con_name, con_description, src_api_key, dest_api_key, dest_url, region="us"
):
    # region based logic
    region_map = {
        "us": "api.treasuredata.com",
        "eu": "api.eu01.treasuredata.com",
        "jp": "api.treasuredata.co.jp",
        "kr": "api.ap02.treasuredata.com",
    }
    url = f"https://{region_map[region]}/v4/connections"

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
    if response.status_code not in (200, 201):  # Check if status code is not 200 (OK)
        raise Exception(
            f"Request failed with status code: {response.status_code}, Error: {response.text}"
        )

    print(response.text)
    return response.json()


def create_config_yaml(
    connection_name,
    src_database,
    dest_database,
    folder="vs_copy_all",
    filename="config.yml",
):
    """
    Creates and saves a YAML configuration file for data transfer.

    Args:
      src_database: The name of the source database.
      dest_database: The name of the destination database.
      folder: The folder where the YAML file should be saved (defaults to current directory).
      filename: The name of the YAML file (defaults to 'transfer_config.yml').

    Returns:
      The path to the saved YAML file.
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

    return filepath


def create_wf_dig(folder="vs_copy_all", filename="vs_copy_all.dig"):
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

    # Save the workflow to the YAML file
    with open(filepath, "w") as yaml_file:
        yaml_file.write(workflow)

    return filepath


from pathlib import Path


def get_project_folder():
    """Returns the absolute path to the project folder."""
    # current_file_path = os.path.abspath(__file__)
    # project_folder = os.path.dirname(os.path.dirname(current_file_path))
    return Path(os.getcwd()).parent.parent.resolve()


def create_td_copy_wf(
    connection_name, src_database, dest_database, folderpath="vs_copy_all"
):
    dir = get_project_folder()
    folderpath = os.path.join(dir, folderpath)
    yml_filepath = create_config_yaml(
        connection_name,
        src_database,
        dest_database,
        folder=folderpath,
        filename="config.yml",
    )
    dig_filepath = create_wf_dig(folder=folderpath, filename="vs_copy_all.dig")
    return yml_filepath, dig_filepath, folderpath


def delete_folder(folder_path):
    """
    Deletes a folder and its contents.

    Args:
        folder_path: The path to the folder you want to delete.
    """
    try:
        shutil.rmtree(folder_path)  # Use shutil.rmtree to recursively delete the folder
        print(f"Folder '{folder_path}' and its contents deleted successfully.")
    except OSError as e:
        print(f"Error deleting folder '{folder_path}': {e}")


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

    print(f"Created project archive for {project_name} ({len(archive_bytes)} bytes)")
    return project_name, archive_bytes


def upload_project_to_td(
    project_name,
    archive_bytes,
    td_api_key,
    region,
    revision=None,
):
    """
    Upload a Digdag project to Treasure Data.

    Args:
        project_name (str): Name of the project
        archive_bytes (bytes): Project archive as bytes
        td_api_key (str): Treasure Data API key
        region (str): Treasure Data Region of Intance
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

    if not revision:
        revision = str(uuid.uuid4())
    # Build the URL
    url = f"{base_url}?project={project_name}"
    if revision:
        url += f"&revision={revision}"

    headers = {"Authorization": f"TD1 {td_api_key}", "Content-Type": "application/gzip"}

    try:
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
        print(f"API request failed: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        raise


def deploy_wf_gh(td_apikey, region, project_dir, project_name):
    try:
        # Upload each project
        results = []

        # Create project archive
        _, archive_bytes = create_project_archive(project_dir, project_name)

        # Upload to Treasure Data
        result = upload_project_to_td(
            project_name,
            archive_bytes,
            td_apikey,
            # revision,
            region,
        )

        results.append({"project": project_name, "result": result})

        print(f"Successfully uploaded {len(results)} project(s)")
        for result in results:
            print(f"Project {result['project']}: {result['result']}")
        return results

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        # Clean up temporary directory
        delete_folder(project_dir)


def run_project_wf_td(workflow_id, td_api_key, region):
    """
    Upload a Digdag project to Treasure Data.

    Args:
        project_name (str): Name of the project
        archive_bytes (bytes): Project archive as bytes
        td_api_key (str): Treasure Data API key
        region (str): Treasure Data Region of Intance
        revision (str, optional): Specific revision to use


    Returns:
        dict: API response
    """
    if region == "us":
        base_url = "api-workflow.treasuredata.com"
    elif region == "eu":
        base_url = "api-workflow.eu01.treasuredata.com"
    elif region == "jp":
        base_url = "api-workflow.treasuredata.co.jp"

    now = datetime.datetime.now()
    date_time_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"https://{base_url}/api/attempts"
    print(url)

    payload = json.dumps(
        {"workflowId": workflow_id, "sessionTime": date_time_str, "params": {}}
    )
    headers = {"Authorization": f"TD1 {td_api_key}", "Content-Type": "application/json"}

    response = requests.request("PUT", url, headers=headers, data=payload)
    if response.status_code != 200:  # Check if status code is not 200 (OK)
        raise Exception(
            f"Request failed with status code: {response.status_code}, Error: {response.text}"
        )

    print(response.text)
    return response.json()


def get_project_wf_td(project_id, td_api_key, region, workflow_name="vs_copy_all"):
    """
    Upload a Digdag project to Treasure Data.

    Args:
        project_name (str): Name of the project
        archive_bytes (bytes): Project archive as bytes
        td_api_key (str): Treasure Data API key
        region (str): Treasure Data Region of Intance
        revision (str, optional): Specific revision to use


    Returns:
        dict: API response
    """
    if region == "us":
        base_url = "api-workflow.treasuredata.com"
    elif region == "eu":
        base_url = "api-workflow.eu01.treasuredata.com"
    elif region == "jp":
        base_url = "api-workflow.treasuredata.co.jp"

    url = f"https://{base_url}/api/projects/{project_id}/workflows/{workflow_name}"
    print(url)

    headers = {"Authorization": f"TD1 {td_api_key}", "Content-Type": "application/json"}

    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:  # Check if status code is not 200 (OK)
        raise Exception(
            f"Request failed with status code: {response.status_code}, Error: {response.text}"
        )

    print("reponse:", response.text)
    res_json = response.json()
    return {
        "workflow_id": res_json.get("id"),
        "project_id": res_json.get("project").get("id"),
    }


def get_project_wf_status(workflow_run_id, td_api_key, region):
    """
    Upload a Digdag project to Treasure Data.

    Args:
        project_name (str): Name of the project
        archive_bytes (bytes): Project archive as bytes
        td_api_key (str): Treasure Data API key
        region (str): Treasure Data Region of Intance
        revision (str, optional): Specific revision to use


    Returns:
        dict: API response
    """
    if region == "us":
        base_url = "api-workflow.treasuredata.com"
    elif region == "eu":
        base_url = "api-workflow.eu01.treasuredata.com"
    elif region == "jp":
        base_url = "api-workflow.treasuredata.co.jp"

    now = datetime.datetime.now()
    date_time_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"https://{base_url}/api/attempts/{workflow_run_id}"
    print(url)

    headers = {"Authorization": f"TD1 {td_api_key}", "Content-Type": "application/json"}

    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:  # Check if status code is not 200 (OK)
        raise Exception(
            f"Request failed with status code: {response.status_code}, Error: {response.text}"
        )

    print(response.text)
    return response.json()


def post_journey_folder(data, dst_client):
    # if region == "us":
    #     base_url = "api-cdp.treasuredata.com"
    # elif region == "eu":
    #     base_url = "api-cdp.eu01.treasuredata.com"
    # elif region == "jp":
    #     base_url = "api-cdp.treasuredata.co.jp"
    # elif region == "kr":
    #     base_url = "api-cdp.ap02.treasuredata.com"

    # now = datetime.datetime.now()
    # date_time_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # url = f"https://{base_url}/entities/journeys"
    # print(url)

    # headers = {
    #     "Authorization": f"TD1 {td_api_key}",
    #     "Content-Type": "application/vnd.treasuredata.v1+json",
    # }

    # payload = json.dumps(data)
    # for p in payload.get("data", []):
    #     try:
    #         response = requests.request("POST", url, headers=headers, data=p)
    #         if response.status_code not in (200, 201):
    #             print(
    #                 f"Request failed with status code: {response.status_code}, Error: {response.text}"
    #             )
    #     except Exception as e:
    #         print(f"Error posting journey: {e}")
    url_path = "entities/journeys"
    journeys = data.get("data", [])
    for journey in journeys:
        try:
            response = dst_client.request("POST", url_path, json={"data": [journey]})
            if not (200 <= response.get("status", 200) < 300):
                print(f"Request failed for journey {journey.get('id', '')}: {response}")
        except Exception as e:
            print(f"Error posting journey: {e}")


def copy_data_assets(
    src_parent: str, src_key: str, dst_key: str, dest_url: str, region="us"
) -> None:
    """
    Copies data assets using VS Copy All workflow. Runs a separate workflow for each database
    found in the source parent segment.

    Args:
        base (str): Base URL for the API endpoint
        src_parent (str): Source parent segment ID
        src_key (str): API key for source environment
        dst_parent (str): Destination parent segment ID
        dst_key (str): API key for destination environment
    """
    print("\n⏩ Initializing data assets copy...")
    now = datetime.datetime.now()
    int_timestamp = int(now.timestamp())

    connection_name = f"mscopy_td2td_{int_timestamp}"

    REGION = {
        "us": "https://api-cdp.treasuredata.com",
        "eu": "https://api-cdp.eu01.treasuredata.com",
        "jp": "https://api-cdp.treasuredata.co.jp",
        "kr": "https://api-cdp.ap02.treasuredata.com",
    }

    try:
        # Get data references from segment
        connector = TDConnector(REGION[region], src_key, dst_key)
        td2td_connection_create(
            connection_name,
            f"Auto generated connector for MS copy",
            src_key,
            dst_key,
            dest_url,
            region,
        )

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

            yml_filepath, dig_filepath, folderpath = create_td_copy_wf(
                connection_name, db, db, folderpath="vs_copy_all"
            )
            project_dict = deploy_wf_gh(
                src_key, region, folderpath, f"ms_segment_copy_{int_timestamp}_{i}"
            )
            project_id = project_dict[0].get("result").get("id")

            ref_dict = get_project_wf_td(project_id, src_key, region)
            workflow_id = ref_dict.get("workflow_id")
            project_id = ref_dict.get("project_id")

            run_dict = run_project_wf_td(workflow_id, src_key, region)
            run_id = run_dict.get("id")

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
                    if time.time() - wf["start_time"] > WORKFLOW_TIMEOUT:
                        print(
                            f"❌ Workflow for {wf['db_name']} timed out after {WORKFLOW_TIMEOUT/3600:.1f} hours"
                        )
                        workflows.remove(wf)
                        failed += 1
                        continue

                    # Only poll if enough time has passed
                    if time.time() - wf.get("last_check", 0) < wf["poll_interval"]:
                        continue

                    run_dict = get_project_wf_status(wf["run_id"], src_key, region)
                    status = run_dict.get("status")
                    wf["last_check"] = time.time()
                    wf["consecutive_errors"] = (
                        0  # Reset error count on successful check
                    )

                    print(f"   • Database {wf['db_name']}: {status}")

                    if status in ["success", "completed"]:
                        print(f"✅ Copy completed for database {wf['db_name']}")
                        workflows.remove(wf)
                        completed += 1
                    elif status in ["error", "killed", "failed"]:
                        error_msg = run_dict.get("error") or run_dict.get(
                            "message", "Unknown error"
                        )
                        print(f"❌ Workflow failed for {wf['db_name']}: {error_msg}")
                        workflows.remove(wf)
                        failed += 1
                    else:
                        # Increase poll interval with exponential backoff
                        wf["poll_interval"] = min(
                            wf["poll_interval"] * 1.5, MAX_POLL_INTERVAL
                        )

                except requests.exceptions.RequestException as e:
                    wf["consecutive_errors"] += 1
                    print(
                        f"⚠️ Error checking status for {wf['db_name']} (attempt {wf['consecutive_errors']}/{MAX_RETRIES}): {str(e)}"
                    )

                    if wf["consecutive_errors"] >= MAX_RETRIES:
                        print(
                            f"❌ Failed to check workflow status for {wf['db_name']} after {MAX_RETRIES} retries"
                        )
                        workflows.remove(wf)
                        failed += 1
                    else:
                        # Reduce poll interval when errors occur to retry sooner
                        wf["poll_interval"] = INITIAL_POLL_INTERVAL

            # Short sleep to prevent tight loop
            if workflows:
                time.sleep(1)

        if workflows:  # Any remaining workflows hit the global timeout
            print(
                f"\n❌ Global timeout reached after {WORKFLOW_TIMEOUT/3600:.1f} hours"
            )
            failed += len(workflows)

        total = completed + failed
        print(f"\n📊 Final Status:")
        print(f"   • Total workflows: {total}")
        print(f"   • Completed successfully: {completed}")
        print(f"   • Failed: {failed}")

        if failed > 0:
            raise Exception(f"Data copy failed for {failed} out of {total} databases")

        print(f"\n✅ Data assets copy completed for all databases")

    except Exception as e:
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
    start_time = time.time()
    timestamp_suffix = f"_copy_{int(time.time())}"
    skipped_segments = []  # Track skipped segments
    failed_segments = []  # Track failed segments
    skipped_journeys = []  # Track skipped journeys
    failed_journeys = []  # Track failed journeys

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

        folders_map = {src_root: dst_root}
        segments_map = {}

        # Copy folders first (topological order)
        folder_entities = [e for e in entities if e["type"] == "folder-segment"]
        if folder_entities:
            print("\n  📂 Copying folders...")
            g = nx.DiGraph()

            for e in folder_entities:
                parent = e["relationships"]["parentFolder"]["data"]
                if parent:
                    g.add_edge(e["id"], parent["id"])
                else:
                    g.add_node(e["id"])

            for i, fid in enumerate(reversed(list(nx.topological_sort(g))), 1):
                if fid == src_root:  # skip root
                    continue

                try:
                    ent = next(e for e in folder_entities if e["id"] == fid)
                    parent_src = ent["relationships"]["parentFolder"]["data"]["id"]
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
                            raise

                    new_id = result["data"]["id"]
                    folders_map[fid] = new_id
                    print(
                        f"    [{i}/{len(folder_entities)}] {ent['attributes']['name']}  →  {new_id}"
                    )
                except Exception as e:
                    print(f"    ⚠️ Failed to copy folder {fid}: {str(e)}")
                    # Skip this folder but continue with others

        # Copy journeys (outside folders, e.g. in root or orphaned)
        # journey_entities = [e for e in entities if e["type"].startswith("journey")]
        # if journey_entities:
        #     print("\n🧭 Copying journeys...")
        #     for i, journey in enumerate(journey_entities, 1):
        #         try:
        #             # Determine the source parent folder ID
        #             parent_src = None
        #             if (
        #                 "relationships" in journey
        #                 and "parentFolder" in journey["relationships"]
        #                 and "data" in journey["relationships"]["parentFolder"]
        #                 and "id" in journey["relationships"]["parentFolder"]["data"]
        #             ):
        #                 parent_src = journey["relationships"]["parentFolder"]["data"][
        #                     "id"
        #                 ]
        #             else:
        #                 skipped_journeys.append(
        #                     journey.get(
        #                         "id",
        #                         journey.get("attributes", {}).get("name", "unknown"),
        #                     )
        #                 )
        #                 print(
        #                     f"    ⚠️ Skipping journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: missing parentFolder"
        #                 )
        #                 continue

        #             # Map to destination folder ID
        #             if parent_src not in folders_map:
        #                 skipped_journeys.append(
        #                     journey.get(
        #                         "id",
        #                         journey.get("attributes", {}).get("name", "unknown"),
        #                     )
        #                 )
        #                 print(
        #                     f"    ⚠️ Skipping journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: parent folder not found in destination"
        #                 )
        #                 continue

        #             journey["relationships"]["parentFolder"]["data"]["id"] = (
        #                 folders_map[parent_src]
        #             )

        #             # Post journey to destination
        #             post_journey_folder({"data": [journey]}, dst_client)
        #             print(
        #                 f"    [{i}/{len(journey_entities)}] Copied journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))} to folder {folders_map[parent_src]}"
        #             )

        #         except Exception as e:
        #             failed_journeys.append(
        #                 journey.get(
        #                     "id",
        #                     journey.get("attributes", {}).get("name", "unknown"),
        #                 )
        #             )
        #             print(
        #                 f"    ⚠️ Failed to copy journey {journey.get('id', journey.get('attributes', {}).get('name', 'unknown'))}: {str(e)}"
        #             )
        #     print(
        #         f"    Copied {len(journey_entities) - len(failed_journeys) - len(skipped_journeys)} journeys"
        #     )

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
                                dep[e["id"]].append(s["id"])

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
                                        if s["id"] not in segments_map:
                                            print(
                                                f"    ⚠️ Warning: Referenced segment {s['id']} not found for {original_name}, skipping reference"
                                            )
                                            has_missing_refs = True
                                        else:
                                            s["id"] = segments_map[s["id"]]
                        except Exception as e:
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
                                print(
                                    f"    ⚠️ Failed to copy segment {original_name}: {error_msg}"
                                )
                                failed_segments.append(original_name)
                                continue
                        else:
                            raise

                    new_id = result["data"]["id"]
                    segments_map[sid] = new_id
                    status = "with missing refs" if has_missing_refs else "→"
                    print(
                        f"    [{i}/{len(segment_entities)}] {ent['attributes']['name']} {status} {new_id}"
                    )

                except Exception as e:
                    print(f"    ⚠️ Failed to copy segment {original_name}: {str(e)}")
                    failed_segments.append(original_name)
                    continue  # Continue with next segment

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
        # dst_parent,
        dst_name,
        dst_key,
        copy_assets_flag,
        copy_data_assets_flag,
    ) = sys.argv[1:]

    # Ensure flags are processed as strings before converting to bool
    copy_assets = str(copy_assets_flag).lower() == "true"
    should_copy_data_assets = str(copy_data_assets_flag).lower() == "true"
    base = REGION.get(instance, REGION["US"])

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
    if instance == "US":
        region = "us"
        dest_url = "api.treasuredata.com"
    elif instance == "EMEA":
        region = "eu"
        dest_url = "api.eu01.treasuredata.com"
    elif instance == "JAPAN":
        region = "jp"
        dest_url = "api.treasuredata.co.jp"
    elif instance == "KOREA":
        region = "kr"
        dest_url = "api.ap02.treasuredata.com"
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
        # orig["id"], orig["name"] = dst_parent, dst_name
        orig["id"], orig["name"] = None, dst_name
        # dst_client.request("PUT", f"audiences/{dst_parent}", json=orig)
        ps_check_and_update(dst_client, json.dumps(orig))
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
        print(f"\n❌ Error occurred during copy process:")
        print(f"   {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
