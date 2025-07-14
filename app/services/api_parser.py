import httpx
import polars as pl
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

import httpx
import polars as pl
import json
import logging
from typing import Dict, Any, Optional
from .file_parser import FileParser

logger = logging.getLogger(__name__)

class ApiParser:
    """Parse data from remote APIs using Polars native methods"""
    
    @staticmethod
    async def parse_api_data(
        endpoint: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        credentials: Optional[Dict[str, Any]] = None,
        data_path: Optional[str] = None,
        preview: bool = False,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Parse data from API endpoint using Polars native JSON processing"""
        
        try:
            # Build headers with authentication
            request_headers = {"Accept": "application/json"}
            if headers:
                request_headers.update(headers)
            
            if credentials:
                ApiParser._add_auth_headers(request_headers, credentials)
            
            # Make API request
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.request(method, endpoint, headers=request_headers)
                response.raise_for_status()
                
                # Check response size using content-length header
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB limit
                    raise ValueError("API response too large (exceeds 50MB)")
                
                data = response.json()
            
            # Navigate to target data using reusable path traversal
            target_data = FileParser.extract_nested_data(data, data_path)
            
            if not isinstance(target_data, list):
                raise ValueError("API response is not an array. Check your data path configuration.")
            
            # Apply preview limiting before Polars processing
            if preview and len(target_data) > limit:
                target_data = target_data[:limit]
                was_truncated = True
            else:
                was_truncated = False
            
            # Use Polars native JSON processing via temp file
            if target_data:
                df = FileParser.json_to_polars_via_temp(target_data, preview, limit)
                return {
                    "success": True,
                    "data": df.to_dicts(),
                    "metadata": {
                        "row_count": len(df),
                        "column_count": len(df.columns),
                        "columns": df.columns,
                        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                        "was_truncated": was_truncated,
                        "polars_streaming": True,
                        "parsing_engine": "polars_api_json",
                        "api_endpoint": endpoint,
                        "data_path": data_path
                    }
                }
            else:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {
                        "row_count": 0, 
                        "column_count": 0,
                        "polars_streaming": True,
                        "parsing_engine": "polars_api_json"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error parsing API data from {endpoint}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0}
            }
    
    @staticmethod
    def _add_auth_headers(headers: Dict[str, str], credentials: Dict[str, Any]) -> None:
        """Add authentication headers with proper error handling"""
        cred_type = credentials.get("type")
        cred_value = credentials.get("value")
        
        if not cred_value:
            logger.warning("Empty credential value provided")
            return
        
        try:
            if cred_type == "bearer":
                headers["Authorization"] = f"Bearer {cred_value}"
            elif cred_type == "api-key":
                try:
                    parsed = json.loads(cred_value)
                    header_name = parsed.get("header", "X-API-Key")
                    headers[header_name] = parsed["apiKey"]
                except (json.JSONDecodeError, KeyError):
                    # Fallback to simple API key
                    headers["X-API-Key"] = cred_value
            elif cred_type == "basic":
                try:
                    parsed = json.loads(cred_value)
                    username = parsed["username"]
                    password = parsed["password"]
                    import base64
                    encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
                    headers["Authorization"] = f"Basic {encoded}"
                except (json.JSONDecodeError, KeyError):
                    # Assume the value is already base64 encoded
                    headers["Authorization"] = f"Basic {cred_value}"
            else:
                logger.warning(f"Unknown credential type: {cred_type}")
                
        except Exception as e:
            logger.error(f"Error processing credentials: {str(e)}")
            # Don't fail the request due to auth issues, just log and continue