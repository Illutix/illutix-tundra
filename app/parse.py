from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

class DataSourceConfig(BaseModel):
    """Data source configuration from client"""
    id: str
    type: str  # 'file', 'sql', 'api'
    
    # File config
    signed_url: Optional[str] = None
    format: Optional[str] = None
    
    # SQL config  
    endpoint: Optional[str] = None
    database: Optional[str] = None
    query: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    
    # API config
    api_endpoint: Optional[str] = None
    method: Optional[str] = "GET"
    headers: Optional[Dict[str, str]] = None
    data_path: Optional[str] = None

class ParseRequest(BaseModel):
    """Request to parse data"""
    preview: bool = False
    limit: int = Field(default=1000, le=10000, description="Maximum rows to return")

class ParseResponse(BaseModel):
    """Response from data parsing"""
    success: bool
    data: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}
    error: Optional[str] = None

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    version: str = "1.0.0"