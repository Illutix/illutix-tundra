import httpx
import polars as pl
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class SqlParser:
    """Execute SQL queries and parse results"""
    
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
        """Execute SQL query and return results"""
        
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
            
            # Parse SQL response
            rows, columns = SqlParser._parse_sql_response(data)
            
            # Convert to DataFrame for consistency
            if rows:
                df = pl.DataFrame(rows)
                return {
                    "success": True,
                    "data": df.to_dicts(),
                    "metadata": {
                        "row_count": len(df),
                        "column_count": len(df.columns),
                        "columns": df.columns,
                        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                        "was_truncated": preview and len(df) == limit
                    }
                }
            else:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {"row_count": 0, "column_count": 0}
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
        """Add authentication headers"""
        cred_type = credentials.get("type")
        cred_value = credentials.get("value")
        
        if not cred_value:
            return
        
        if cred_type == "bearer":
            headers["Authorization"] = f"Bearer {cred_value}"
        elif cred_type == "api-key":
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
    
    @staticmethod
    def _add_limit_to_query(query: str, limit: int) -> str:
        """Add LIMIT clause to SQL query"""
        trimmed_query = query.strip()
        if trimmed_query.lower().find('limit') != -1:
            return query
        
        if trimmed_query.endswith(';'):
            return f"{trimmed_query[:-1]} LIMIT {limit};"
        else:
            return f"{trimmed_query} LIMIT {limit}"
    
    @staticmethod
    def _parse_sql_response(data: Any) -> tuple[list, list]:
        """Parse SQL response into rows and columns"""
        rows = []
        
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("rows") or data.get("results") or data.get("data") or []
        
        columns = list(rows[0].keys()) if rows else []
        return rows, columns