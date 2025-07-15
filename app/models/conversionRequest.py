from pydantic import BaseModel, Field, HttpUrl
from typing import Any, Dict, List, Optional

class ConversionRequest(BaseModel):
    """Unified request model for all conversion types"""
    
    # Required for all conversions
    output_url: HttpUrl = Field(..., description="Signed PUT URL for parquet output")
    
    # File conversion fields
    source_url: Optional[HttpUrl] = Field(None, description="Signed GET URL for source file")
    format: Optional[str] = Field(None, description="Source file format (csv, json, tsv, geojson)")
    
    # API conversion fields  
    api_endpoint: Optional[HttpUrl] = Field(None, description="API endpoint URL")
    api_method: Optional[str] = Field("GET", description="HTTP method")
    api_headers: Optional[Dict[str, str]] = Field(None, description="Additional HTTP headers")
    api_data_path: Optional[str] = Field(None, description="JSON path to data array")
    credentials_id: Optional[str] = Field(None, description="Stored credentials ID")
    
    # SQL conversion fields
    sql_endpoint: Optional[HttpUrl] = Field(None, description="SQL API endpoint")
    sql_database: Optional[str] = Field(None, description="Database name")
    sql_query: Optional[str] = Field(None, description="SQL query to execute")
    
    class Config:
        json_encoders = {
            HttpUrl: str
        }

class ConversionMetadata(BaseModel):
    """Metadata about the converted dataset"""
    rows: int = Field(..., description="Number of rows in the dataset")
    columns: int = Field(..., description="Number of columns in the dataset")
    schema: Dict[str, Any] = Field(..., description="Column schema information")
    file_size_mb: float = Field(..., description="Output parquet file size in MB")
    processing_time_seconds: float = Field(..., description="Time taken to process")
    source_type: str = Field(..., description="Type of source data (file, api, sql)")

class ConversionResponse(BaseModel):
    """Response from conversion operations"""
    success: bool = Field(..., description="Whether conversion succeeded")
    metadata: ConversionMetadata = Field(..., description="Dataset metadata")
    error: Optional[str] = Field(None, description="Error message if conversion failed")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")

class ServiceInfo(BaseModel):
    """Service capabilities and configuration"""
    service: str
    version: str
    capabilities: Dict[str, Any]
    limits: Dict[str, Any]
    features: Dict[str, Any]