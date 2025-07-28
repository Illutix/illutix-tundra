from pydantic import BaseModel, Field, HttpUrl
from typing import Any, Dict, List, Optional
from enum import Enum

class FileFormat(str, Enum):
    csv = "csv"
    json = "json"
    tsv = "tsv"
    geojson = "geojson"


class HTTPMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"

class FileConversionRequest(BaseModel):
    """Request model for file conversions"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    source_url: HttpUrl = Field(..., description="Signed GET URL for source file")
    format: FileFormat = Field(..., description="Source file format (csv, json, tsv, geojson)")

class ApiConversionRequest(BaseModel):
    """Request model for API conversions"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    api_endpoint: HttpUrl = Field(..., description="API endpoint URL")
    api_method: HTTPMethod = Field(HTTPMethod.GET, description="HTTP method")
    api_headers: Optional[Dict[str, str]] = Field(None, description="Additional HTTP headers")
    api_data_path: Optional[str] = Field(None, description="JSON path to data array")

class SqlConversionRequest(BaseModel):
    """Request model for SQL conversions"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    sql_endpoint: HttpUrl = Field(..., description="SQL API endpoint")
    sql_database: str = Field(..., description="Database name")
    sql_query: str = Field(..., description="SQL query to execute")


class ConversionMetadata(BaseModel):
    """Metadata about the converted dataset"""
    rows: int = Field(..., description="Number of rows in the dataset")
    columns: int = Field(..., description="Number of columns in the dataset")
    column_schema: Dict[str, Any] = Field(..., description="Column schema information")
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