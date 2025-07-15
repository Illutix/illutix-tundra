from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from .config import settings
from .models import ConversionRequest, ConversionResponse, HealthResponse
from .services.file_converter import FileConverter
from .services.api_converter import ApiConverter  
from .services.sql_converter import SqlConverter

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting Illutix Tundra Data Conversion Service")
    yield
    # Shutdown
    logger.info("🛑 Illutix Tundra shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Illutix Tundra",
    description="High-performance data conversion service - Files to Parquet",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=False,  # No credentials needed for conversion service
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint"""
    return HealthResponse(
        status="running",
        service="illutix-tundra",
        version="1.0.0"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy", 
        service="illutix-tundra",
        version="1.0.0"
    )

@app.post("/convert/file", response_model=ConversionResponse)
async def convert_file(request: ConversionRequest):
    """Convert file from R2 source to parquet format"""
    
    logger.info(f"📄 Converting file: {request.format} → parquet")
    
    try:
        result = await FileConverter.convert(
            source_url=request.source_url,
            output_url=request.output_url,
            file_format=request.format
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])
        
        logger.info(f"✅ File conversion complete: {result['metadata']['rows']} rows")
        return ConversionResponse(**result)
        
    except Exception as e:
        logger.error(f"❌ File conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

@app.post("/convert/api", response_model=ConversionResponse)
async def convert_api_data(request: ConversionRequest):
    """Fetch data from API and convert to parquet format"""
    
    logger.info(f"🔗 Converting API data: {request.api_endpoint}")
    
    try:
        result = await ApiConverter.convert(
            endpoint=request.api_endpoint,
            credentials_id=request.credentials_id,
            output_url=request.output_url,
            method=request.api_method,
            headers=request.api_headers,
            data_path=request.api_data_path
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])
        
        logger.info(f"✅ API conversion complete: {result['metadata']['rows']} rows")
        return ConversionResponse(**result)
        
    except Exception as e:
        logger.error(f"❌ API conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"API conversion failed: {str(e)}")

@app.post("/convert/sql", response_model=ConversionResponse) 
async def convert_sql_data(request: ConversionRequest):
    """Execute SQL query and convert results to parquet format"""
    
    logger.info(f"💾 Converting SQL data: {request.sql_database}")
    
    try:
        result = await SqlConverter.convert(
            endpoint=request.sql_endpoint,
            database=request.sql_database,
            query=request.sql_query,
            credentials_id=request.credentials_id,
            output_url=request.output_url
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])
        
        logger.info(f"✅ SQL conversion complete: {result['metadata']['rows']} rows")
        return ConversionResponse(**result)
        
    except Exception as e:
        logger.error(f"❌ SQL conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"SQL conversion failed: {str(e)}")

@app.get("/info")
async def service_info():
    """Get service capabilities and limits"""
    return {
        "service": "illutix-tundra",
        "version": "1.0.0",
        "capabilities": {
            "file_formats": ["csv", "tsv", "json", "geojson"],
            "output_format": "parquet",
            "max_file_size_mb": 500,
            "supported_sources": ["file", "api", "sql"]
        },
        "limits": {
            "max_processing_time_minutes": 10,
            "max_memory_usage_gb": 2,
            "max_rows_processed": 10_000_000
        },
        "features": {
            "polars_native": True,
            "streaming_processing": True,
            "automatic_schema_inference": True,
            "optimized_parquet_output": True
        }
    }