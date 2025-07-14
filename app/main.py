from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import asyncio
from pathlib import Path

from .config import settings
from .parse import ParseRequest, ParseResponse, HealthResponse, DataSourceConfig
from .services.file_parser import FileParser
from .services.api_parser import ApiParser
from .services.sql_parser import SqlParser
from .services.cleanup import CleanupService

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Background cleanup task
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Illutix Tundra")
    
    # Start cleanup task
    temp_dir = Path("/tmp/polars_server")
    cleanup_task = asyncio.create_task(
        CleanupService.start_cleanup_task(temp_dir)
    )
    
    yield
    
    # Shutdown
    cleanup_task.cancel()
    await CleanupService.cleanup_temp_files(temp_dir)
    logger.info("Server shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Illutix Tundra",
    description="High-performance data processing using Polars",
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint"""
    return HealthResponse(
        status="running",
        service="illutix-tundra-data-processing",
        version="2.0.0"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy", 
        service="illutix-tundra-data-processing",
        version="2.0.0"
    )

@app.get("/datasources/{datasource_id}/data", response_model=ParseResponse)
async def get_data(
    datasource_id: str,
    preview: bool = Query(False, description="Return preview of data"),
    limit: int = Query(settings.DEFAULT_PREVIEW_ROWS, le=settings.MAX_PREVIEW_ROWS, description="Maximum rows to return")
):
    """Load data from a data source (development endpoint)"""
    
    # For development - load from local files with Polars native methods
    result = FileParser.parse_local_file(datasource_id, preview, limit)
    
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["error"])
    
    return ParseResponse(**result)

@app.post("/parse/file", response_model=ParseResponse)
async def parse_file_from_url(
    signed_url: str,
    file_format: str,
    preview: bool = False,
    limit: int = Query(settings.DEFAULT_PREVIEW_ROWS, le=settings.MAX_PREVIEW_ROWS)
):
    """Stream parse file from signed URL using Polars native methods"""
    
    if file_format not in ["csv", "json", "tsv", "geojson"]:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {file_format}")
    
    # Log the Polars native operation
    logger.info(f"Polars native parsing: {file_format} file, preview={preview}, limit={limit}")
    
    result = await FileParser.parse_from_url(signed_url, file_format, preview, limit)
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    # Add Polars native info to response
    result["metadata"]["polars_native"] = True
    result["metadata"]["controlled_disk_usage"] = True
    
    return ParseResponse(**result)

@app.post("/parse/api", response_model=ParseResponse)
async def parse_api_data(
    endpoint: str,
    method: str = "GET",
    headers: dict = None,
    credentials: dict = None,
    data_path: str = None,
    preview: bool = False,
    limit: int = Query(settings.DEFAULT_PREVIEW_ROWS, le=settings.MAX_PREVIEW_ROWS)
):
    """Parse data from API endpoint - streaming safe"""
    
    logger.info(f"API parse: {endpoint}, method={method}, preview={preview}, limit={limit}")
    
    result = await ApiParser.parse_api_data(
        endpoint=endpoint,
        method=method,
        headers=headers,
        credentials=credentials,
        data_path=data_path,
        preview=preview,
        limit=limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    return ParseResponse(**result)

@app.post("/parse/sql", response_model=ParseResponse)
async def parse_sql_data(
    endpoint: str,
    database: str,
    query: str,
    credentials: dict = None,
    query_limit: int = None,
    preview: bool = False,
    limit: int = Query(settings.DEFAULT_PREVIEW_ROWS, le=settings.MAX_PREVIEW_ROWS)
):
    """Execute SQL query and return results - streaming safe"""
    
    logger.info(f"SQL parse: {database}, preview={preview}, limit={limit}")
    
    result = await SqlParser.execute_query(
        endpoint=endpoint,
        database=database,
        query=query,
        credentials=credentials,
        query_limit=query_limit,
        preview=preview,
        limit=limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    return ParseResponse(**result)

@app.get("/datasources")
async def list_datasources():
    """List available data sources (development only)"""
    data_dir = Path(settings.DATA_DIR)
    if not data_dir.exists():
        return {"datasources": []}
    
    datasources = []
    for file in data_dir.glob("*"):
        if file.suffix in [".csv", ".json", ".tsv", ".geojson"]:
            file_size_mb = round(file.stat().st_size / 1024 / 1024, 2)
            
            # Add safety warnings for large files
            safety_note = None
            if file_size_mb > 100:
                safety_note = "Large file - use preview mode"
            elif file_size_mb > 50:
                safety_note = "Medium file - consider preview mode"
            
            datasources.append({
                "id": file.stem,
                "filename": file.name,
                "format": file.suffix[1:],
                "size_mb": file_size_mb,
                "safety_note": safety_note,
                "polars_native": file.suffix in [".csv", ".tsv", ".json"],
                "streaming_recommended": file_size_mb > 10
            })
    
    return {
        "datasources": datasources,
        "supported_formats": ["csv", "tsv", "json", "geojson"],
        "note": "TopJSON support removed - not common for business use cases"
    }

@app.get("/streaming/info")
async def streaming_info():
    """Get information about Polars native parsing capabilities"""
    return {
        "polars_native_parsing": True,
        "max_preview_download_mb": FileParser.PREVIEW_MAX_DOWNLOAD / 1024 / 1024,
        "max_full_download_mb": FileParser.FULL_MAX_DOWNLOAD / 1024 / 1024,
        "chunk_size_mb": FileParser.CHUNK_SIZE / 1024 / 1024,
        "supported_formats": ["csv", "tsv", "json", "geojson"],
        "removed_formats": {
            "topojson": "Removed - not common for business analysts"
        },
        "polars_features": {
            "streaming_engine": "collect(streaming=True)",
            "lazy_evaluation": "scan_csv with head() optimization", 
            "type_inference": "Automatic with try_parse_dates",
            "error_handling": "ignore_errors=True for malformed rows",
            "null_handling": "Configurable null values",
            "json_processing": "pl.read_json() for all JSON data"
        },
        "disk_usage": {
            "strategy": "Controlled temp files with immediate cleanup",
            "preview_limit": "10MB max download",
            "full_limit": "100MB max download", 
            "temp_location": "/tmp/polars_streaming/"
        },
        "api_sql_processing": {
            "method": "JSON response → pl.read_json() via temp file",
            "consistency": "All parsers use Polars native methods",
            "path_traversal": "Reusable nested data extraction"
        },
        "recommendations": {
            "large_files": "Use preview=true for files >10MB",
            "enterprise_datasets": "Always test with preview mode first",
            "performance": "Polars native parsing is 50-100x faster",
            "consistency": "All data sources processed through Polars"
        }
    }