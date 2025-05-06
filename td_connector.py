#!/usr/bin/env python3
"""
td_connector.py
--------------
Module for handling Treasure Data to Treasure Data data asset operations.

This module provides functionality to:
1. Establish connections to Treasure Data environments
2. Extract data references from segments

Dependencies:
- requests: For HTTP operations
- urllib3: For retry functionality
"""

import requests
from typing import Set, Tuple
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

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
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session
        
    def _make_request(self, session: requests.Session, method: str, endpoint: str, 
                     apikey: str, **kwargs) -> dict:
        """Makes an API request with proper headers and error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"TD1 {apikey}",
            "Content-Type": "application/json"
        }
        headers.update(kwargs.pop('headers', {}))
        
        response = session.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def get_segment_data_references(self, segment_id: str) -> Set[Tuple[str, str]]:
        """
        Extracts database and table references from a segment.
        
        Args:
            segment_id (str): ID of the segment to analyze
            
        Returns:
            Set[Tuple[str, str]]: Set of (database, table) tuples referenced by the segment
        """
        try:
            segment = self._make_request(
                self.src_session, 'GET', f'audiences/{segment_id}',
                self.src_apikey
            )
            
            refs = set()
            if 'dataReferences' in segment:
                for ref in segment['dataReferences']:
                    db = ref.get('database')
                    table = ref.get('table')
                    if db:
                        refs.add((db, table) if table else (db, None))
            
            return refs
            
        except Exception as e:
            print(f"⚠️  Error getting data references: {str(e)}")
            return set()