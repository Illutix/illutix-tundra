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
    logger.info("Starting Polars Data Processing Server")
    
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
    title="Polars Data Processing Service",
    description="High-performance data processing using Polars",
    version="1.0.0",
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
        service="polars-data-processing"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy", 
        service="polars-data-processing"
    )

@app.get("/datasources/{datasource_id}/data", response_model=ParseResponse)
async def get_data(
    datasource_id: str,
    preview: bool = Query(False, description="Return preview of data"),
    limit: int = Query(settings.DEFAULT_PREVIEW_ROWS, le=settings.MAX_PREVIEW_ROWS, description="Maximum rows to return")
):
    """Load data from a data source (development endpoint)"""
    
    # For development - load from local files
    # In production, you'd get the config from your database
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
    """Parse file from signed URL"""
    
    if file_format not in ["csv", "json", "tsv", "geojson", "topojson"]:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {file_format}")
    
    result = await FileParser.parse_from_url(signed_url, file_format, preview, limit)
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
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
    """Parse data from API endpoint"""
    
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
    """Execute SQL query and return results"""
    
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
        if file.suffix in [".csv", ".json", ".tsv", ".geojson", ".topojson"]:
            datasources.append({
                "id": file.stem,
                "filename": file.name,
                "format": file.suffix[1:],
                "size_mb": round(file.stat().st_size / 1024 / 1024, 2)
            })
    
    return {"datasources": datasources}
