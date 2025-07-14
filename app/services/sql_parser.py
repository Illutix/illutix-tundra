import httpx
import polars as pl
import json
import logging
from typing import Dict, Any, Optional
from .file_parser import FileParser

logger = logging.getLogger(__name__)

class SqlParser:
    """Execute SQL queries and parse results using Polars native methods"""
    
    @staticmethod
    async def execute_query(
        endpoint: str,
        database: str,
        query: str,
        credentials: Optional[Dict[str, Any]] = None,
        query_limit: Optional[int] = None,
        preview: bool = False,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Execute SQL query and return results using Polars native JSON processing"""
        
        try:
            # Build headers with authentication
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            if credentials:
                SqlParser._add_auth_headers(headers, credentials)
            
            # Modify query for preview or limits
            final_query = query
            if preview:
                final_query = SqlParser._add_limit_to_query(query, limit)
            elif query_limit:
                final_query = SqlParser._add_limit_to_query(query, query_limit)
            
            # Execute query
            request_body = {
                "query": final_query,
                "database": database
            }
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(endpoint, headers=headers, json=request_body)
                response.raise_for_status()
                
                data = response.json()
            
            # Parse SQL response into standardized format
            rows = SqlParser._extract_rows_from_response(data)
            
            # Use Polars native JSON processing for consistent handling
            if rows:
                df = FileParser.json_to_polars_via_temp(rows, preview, limit)
                return {
                    "success": True,
                    "data": df.to_dicts(),
                    "metadata": {
                        "row_count": len(df),
                        "column_count": len(df.columns),
                        "columns": df.columns,
                        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                        "was_truncated": preview and len(df) == limit,
                        "polars_streaming": True,
                        "parsing_engine": "polars_sql_json",
                        "sql_endpoint": endpoint,
                        "database": database,
                        "query_modified": final_query != query
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
                        "parsing_engine": "polars_sql_json"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error executing SQL query: {str(e)}")
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
            logger.warning("Empty credential value provided for SQL endpoint")
            return
        
        try:
            if cred_type == "bearer":
                headers["Authorization"] = f"Bearer {cred_value}"
            elif cred_type == "api-key":
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
                    # Assume already base64 encoded
                    headers["Authorization"] = f"Basic {cred_value}"
            else:
                logger.warning(f"Unknown SQL credential type: {cred_type}")
                
        except Exception as e:
            logger.error(f"Error processing SQL credentials: {str(e)}")
    
    @staticmethod
    def _add_limit_to_query(query: str, limit: int) -> str:
        """Add LIMIT clause to SQL query if not present"""
        trimmed_query = query.strip()
        
        # Check if LIMIT already exists (case insensitive)
        if 'limit' in trimmed_query.lower():
            return query
        
        if trimmed_query.endswith(';'):
            return f"{trimmed_query[:-1]} LIMIT {limit};"
        else:
            return f"{trimmed_query} LIMIT {limit}"
    
    @staticmethod
    def _extract_rows_from_response(data: Any) -> list:
        """Extract rows from various SQL response formats"""
        
        if isinstance(data, list):
            # Direct array of row objects
            return data
        elif isinstance(data, dict):
            # Try common SQL response formats
            for key in ["rows", "results", "data", "records"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            
            # If it's a single result object, wrap in array
            if any(isinstance(v, (str, int, float, bool)) for v in data.values()):
                return [data]
            
            # Empty result
            return []
        else:
            logger.warning(f"Unexpected SQL response format: {type(data)}")
            return []