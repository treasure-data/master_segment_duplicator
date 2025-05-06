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
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from td_connector import TDConnector

# API Configuration Constants
TD_MIME = "application/vnd.treasuredata.v1+json"
MAX_RETRIES = 3
RETRY_BACKOFF = 2
API_RATE_LIMIT = 2  # requests per second

# Region-specific API endpoints
REGION = {
    "US"   : "https://api-cdp.treasuredata.com",
    "EMEA" : "https://api-cdp.eu01.treasuredata.com",
    "Japan": "https://api-cdp.treasuredata.co.jp",
    "Korea": "https://api-cdp.ap02.treasuredata.com"
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
            status_forcelist=[429, 500, 502, 503, 504]  # Common transient errors
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
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
        headers = {
            "Authorization": f"TD1 {self.api_key}",
            "Content-Type": TD_MIME
        }
        headers.update(kwargs.pop('headers', {}))
        
        try:
            response = self.session.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            print(f"⚠️  API request failed: {e}")
            if hasattr(e.response, 'text'):
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
    return (
        TDClient(base, src_key),
        TDClient(base, dst_key)
    )

def deploy_vs_copy_workflow(client: TDClient, workflow_dir: str) -> None:
    """
    Deploys the VS Copy All workflow to Treasure Data.
    
    Args:
        client (TDClient): TDClient instance for API calls
        workflow_dir (str): Directory containing the workflow files
    """
    print("\n⏩ Deploying VS Copy All workflow...")
    try:
        # Clone the repository if not exists
        if not os.path.exists(workflow_dir):
            subprocess.run(["git", "clone", VS_COPY_REPO, workflow_dir], check=True)
        
        # Push workflow to Treasure Data
        subprocess.run([
            "td", "wf", "push", WORKFLOW_NAME,
            "--project", workflow_dir,
            "--wait"
        ], check=True)
        print("✅ Workflow deployed successfully")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to deploy workflow: {str(e)}")
        raise

def run_vs_copy_workflow(client: TDClient, 
                        data_refs: Set[Tuple[str, str]],
                        src_key: str,
                        dst_key: str) -> None:
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
            params["databases"].append({
                "name": db,
                "tables": tables if tables else ["*"]  # Use * if no specific tables
            })
        
        # Start workflow
        result = client.request(
            'POST',
            f"workflows/{WORKFLOW_NAME}/sessions",
            json={"params": params}
        )
        
        session_id = result.get("id")
        if not session_id:
            raise Exception("Failed to get workflow session ID")
            
        print(f"✅ Workflow started with session ID: {session_id}")
        
        # Monitor workflow progress
        while True:
            status = client.request(
                'GET',
                f"workflows/{WORKFLOW_NAME}/sessions/{session_id}/status"
            )
            
            state = status.get("state", "unknown")
            print(f"   • Workflow status: {state}")
            
            if state == "success":
                print("✅ Data copy completed successfully")
                break
            elif state in ["error", "killed"]:
                raise Exception(f"Workflow failed with state: {state}")
            
            time.sleep(WORKFLOW_POLL_INTERVAL)
            
    except Exception as e:
        print(f"❌ Workflow execution failed: {str(e)}")
        raise

def copy_data_assets(base: str,
                    src_parent: str, src_key: str,
                    dst_parent: str, dst_key: str) -> None:
    """
    Copies data assets using VS Copy All workflow.
    
    Args:
        base (str): Base URL for the API endpoint
        src_parent (str): Source parent segment ID
        src_key (str): API key for source environment
        dst_parent (str): Destination parent segment ID
        dst_key (str): API key for destination environment
    """
    print("\n⏩ Initializing data assets copy...")
    
    try:
        # Initialize TD client
        client = TDClient(base, src_key)
        
        # Get data references from segment
        connector = TDConnector(base, src_key, dst_key)
        data_refs = connector.get_segment_data_references(src_parent)
        
        if not data_refs:
            print("ℹ️ No data references found in the segment")
            return
            
        print(f"\n📊 Found {len(data_refs)} data references:")
        for db, table in sorted(data_refs):
            if table:
                print(f"  • Table: {db}.{table}")
            else:
                print(f"  • Database: {db}")
        
        # Create temporary directory for workflow
        workflow_dir = os.path.join(os.getcwd(), "vs_copy_all_temp")
        
        try:
            # Deploy workflow
            deploy_vs_copy_workflow(client, workflow_dir)
            
            # Run workflow with identified tables
            run_vs_copy_workflow(client, data_refs, src_key, dst_key)
            
        finally:
            # Cleanup
            if os.path.exists(workflow_dir):
                subprocess.run(["rm", "-rf", workflow_dir], check=True)
        
    except Exception as e:
        print(f"\n❌ Error during data assets copy: {str(e)}")
        raise

def copy_folders_segments(src_client: TDClient, dst_client: TDClient,
                        src_parent: str, dst_parent: str) -> None:
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

    try:
        # Get source root folder
        src_root = src_client.request(
            'GET', 
            f"entities/parent_segments/{src_parent}"
        )["data"]["relationships"]["parentSegmentFolder"]["data"]["id"]
        
        # Get destination root folder
        dst_root = dst_client.request(
            'GET',
            f"entities/parent_segments/{dst_parent}"
        )["data"]["relationships"]["parentSegmentFolder"]["data"]["id"]

        # Fetch all entities
        entities = src_client.request(
            'GET',
            f"entities/by-folder/{src_root}?depth=32"
        )["data"]

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
                    
                ent = next(e for e in entities if e["id"] == fid)
                parent_src = ent["relationships"]["parentFolder"]["data"]["id"]
                ent["relationships"]["parentFolder"]["data"]["id"] = folders_map[parent_src]
                
                result = dst_client.request('POST', "entities/folders", json=ent)
                new_id = result["data"]["id"]
                folders_map[fid] = new_id
                print(f"    [{i}/{len(folder_entities)}] {ent['attributes']['name']}  →  {new_id}")

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

            for i, sid in enumerate(reversed(list(nx.topological_sort(nx.DiGraph(dep)))), 1):
                ent = next(e for e in entities if e["id"] == sid)
                
                # Update folder reference and audience ID
                parent_src = ent["relationships"]["parentFolder"]["data"]["id"]
                ent["relationships"]["parentFolder"]["data"]["id"] = folders_map[parent_src]
                ent["attributes"]["audienceId"] = dst_parent

                # Update any segment references
                if "rule" in ent["attributes"]:
                    for c in ent["attributes"]["rule"].get("conditions", []):
                        for s in c.get("conditions", []):
                            if s.get("type") == "Reference":
                                s["id"] = segments_map[s["id"]]

                result = dst_client.request('POST', "entities/segments", json=ent)
                new_id = result["data"]["id"]
                segments_map[sid] = new_id
                print(f"    [{i}/{len(segment_entities)}] {ent['attributes']['name']}  →  {new_id}")

        duration = time.time() - start_time
        print(f"\n✅  Folders & segments copy finished in {duration:.1f}s")
        print(f"   • Copied {len(folders_map)-1} folders")
        print(f"   • Copied {len(segments_map)} segments\n")

    except Exception as e:
        print(f"⚠️  Error during folders/segments copy: {str(e)}\n")
        raise

def main():
    """
    Main entry point for the segment copy process.
    Parses CLI arguments and orchestrates the copy operations.
    """
    if len(sys.argv) < 9:
        print("Usage: python copier.py "
              "<src_parent_id> <src_api_key> "
              "<instance> "
              "<dst_parent_id> <dst_parent_name> <dst_api_key> "
              "<copy_assets_flag> <copy_data_assets_flag>")
        sys.exit(1)

    # Parse arguments
    (src_parent, src_key,
     instance,
     dst_parent, dst_name, dst_key,
     copy_assets_flag,
     copy_data_assets_flag) = sys.argv[1:]

    copy_assets = copy_assets_flag.lower() == "true"
    copy_data_assets = copy_data_assets_flag.lower() == "true"
    base = REGION.get(instance, REGION["US"])

    print(f"\n🚀 Starting segment copy process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Region: {instance}  |  Base URL: {base}")
    print(f"   Source ID: {src_parent}")
    print(f"   Destination: {dst_parent} ({dst_name})")
    
    try:
        src_client, dst_client = setup_clients(base, src_key, dst_key)
        
        # 1. Copy data assets first if requested
        if copy_data_assets:
            copy_data_assets(base, src_parent, src_key, dst_parent, dst_key)
        else:
            print("\nℹ️  Skipping data assets (copy_data_assets=False)")

        # 2. Get & copy parent segment
        print("\n⏩ Getting source parent segment...")
        orig = src_client.request('GET', f"audiences/{src_parent}")
        print("✅  Source parent segment retrieved.")

        # 3. Create destination parent segment
        print("\n⏩ Creating destination parent segment...")
        orig["id"], orig["name"] = dst_parent, dst_name
        dst_client.request('PUT', f"audiences/{dst_parent}", json=orig)
        print("✅  Destination parent segment created.")

        # 4. Copy folders & segments if requested
        if copy_assets:
            copy_folders_segments(src_client, dst_client, src_parent, dst_parent)
        else:
            print("\nℹ️  Skipping folders/segments (copy_assets=False)")

        print("\n✨ Segment copy process completed successfully!\n")

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
