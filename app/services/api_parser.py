import httpx
import polars as pl
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ApiParser:
    """Parse data from remote APIs"""
    
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
        """Parse data from API endpoint"""
        
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
                
                # Check response size
                if hasattr(response, 'headers') and 'content-length' in response.headers:
                    size = int(response.headers['content-length'])
                    if size > 50 * 1024 * 1024:  # 50MB limit
                        raise ValueError("API response too large (exceeds 50MB)")
                
                data = response.json()
            
            # Navigate to target data using path
            target_data = data
            if data_path:
                for part in data_path.split('.'):
                    if target_data and isinstance(target_data, dict) and part in target_data:
                        target_data = target_data[part]
                    else:
                        raise ValueError(f"Data path '{data_path}' not found in API response")
            
            if not isinstance(target_data, list):
                raise ValueError("API response is not an array. Check your data path configuration.")
            
            # Apply preview limiting
            if preview and len(target_data) > limit:
                target_data = target_data[:limit]
                was_truncated = True
            else:
                was_truncated = False
            
            # Convert to DataFrame for consistency
            if target_data:
                df = pl.DataFrame(target_data)
                return {
                    "success": True,
                    "data": df.to_dicts(),
                    "metadata": {
                        "row_count": len(df),
                        "column_count": len(df.columns),
                        "columns": df.columns,
                        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                        "was_truncated": was_truncated
                    }
                }
            else:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {"row_count": 0, "column_count": 0}
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
        """Add authentication headers based on credential type"""
        cred_type = credentials.get("type")
        cred_value = credentials.get("value")
        
        if not cred_value:
            return
        
        if cred_type == "bearer":
            headers["Authorization"] = f"Bearer {cred_value}"
        elif cred_type == "api-key":
            try:
                import json
                parsed = json.loads(cred_value)
                header_name = parsed.get("header", "X-API-Key")
                headers[header_name] = parsed["apiKey"]
            except (json.JSONDecodeError, KeyError):
                headers["X-API-Key"] = cred_value
        elif cred_type == "basic":
            try:
                import json
                import base64
                parsed = json.loads(cred_value)
                username = parsed["username"]
                password = parsed["password"]
                encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"
            except (json.JSONDecodeError, KeyError):
                headers["Authorization"] = f"Basic {cred_value}"