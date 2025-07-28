# Illutix Tundra - Data Conversion Service

High-performance data conversion service using Python + Polars to convert various data formats to optimized Parquet files. Designed for enterprise-scale datasets with Supabase JWT authentication.

## Overview

Illutix Tundra is a specialized conversion service that transforms data from multiple sources into optimized Parquet format for efficient storage and querying. The service handles files, API endpoints, and SQL queries, processing them through Polars for maximum performance.

## Features

- **Multi-source conversion**: Files (CSV, TSV, JSON, GeoJSON), APIs, and SQL queries
- **Polars-powered**: Lightning-fast data processing and transformation
- **Parquet optimization**: Compressed, columnar storage with statistics
- **Business-friendly GeoJSON**: Automatic transformation for visualization use cases
- **Production-ready**: Built for enterprise scale with proper limits and error handling
- **Secure**: Supabase JWT authentication for protected endpoints
- **Cloud-native**: Designed for Cloud Run with automatic scaling

## Architecture

```
illutix-tundra/
├── app/
│   ├── main.py                    # FastAPI app + routing
│   ├── config.py                  # Environment configuration  
│   ├── models/
│   │   └── conversionRequest.py   # Pydantic request/response models
│   └── services/                  # Core conversion logic
│       ├── file_converter.py      # R2 file → Parquet conversion
│       ├── api_converter.py       # API data → Parquet conversion
│       └── sql_converter.py       # SQL results → Parquet conversion
├── requirements.txt
├── openapi.yaml                   # API Gateway configuration
└── README.md
```

## API Endpoints

### Public Endpoints
- `GET /` - Root endpoint
- `GET /health` - Health check
- `GET /info` - Service capabilities and limits

### Protected Endpoints (Supabase JWT required)
- `POST /convert/file` - Convert files from R2 to Parquet
- `POST /convert/api` - Fetch API data and convert to Parquet  
- `POST /convert/sql` - Execute SQL queries and convert results to Parquet

### API Gateway Proxy
- `GET /convert/{proxy}` - Proxied GET requests with authentication
- `POST /convert/{proxy}` - Proxied POST requests with authentication

## Data Sources

### File Conversion
Supports conversion from R2 storage URLs:
- **CSV/TSV**: Automatic type inference, null handling
- **JSON**: Direct parsing with Polars
- **GeoJSON**: Business-friendly transformation with coordinate analysis

### API Data
Fetches data from REST APIs:
- Configurable HTTP methods and headers
- JSON path extraction for nested data
- Authentication support (placeholder for credential integration)

### SQL Queries
Executes queries via SQL APIs:
- Automatic safety limits for large result sets
- Multiple response format support
- Database connection abstraction

## Processing Limits

- **File size**: 500MB maximum
- **API responses**: 100MB maximum  
- **SQL responses**: 100MB maximum
- **Processing time**: 10 minutes maximum
- **Memory usage**: 2GB maximum
- **Concurrent conversions**: 5 maximum

## Configuration

Key environment variables:
- `ENV`: Runtime environment (dev/staging/production)
- `MAX_FILE_SIZE_MB`: File size limit (default: 500)
- `MAX_PROCESSING_TIME_MINUTES`: Processing timeout (default: 10)
- `LOG_LEVEL`: Logging verbosity (default: INFO)

CORS origins are automatically configured based on environment:
- Production: `illutix.com` domains only
- Non-production: Includes `localhost:3000`

## Output Format

All conversions produce optimized Parquet files with:
- **Compression**: Snappy for balance of speed and size
- **Row groups**: 50,000 rows for optimal query performance
- **Statistics**: Column-level metadata for query optimization
- **Schema**: Detailed field information and type mapping

## Development

### Local Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
```

### Testing Endpoints
```bash
# Health check
curl http://localhost:8080/health

# Service info
curl http://localhost:8080/info
```

## Deployment

The service uses automated CI/CD with GitHub Actions and Cloud Run. Every push to `main` automatically creates a new Cloud Run revision.
## Authentication

Protected endpoints require Supabase JWT tokens:

Tokens should be included in the `Authorization` header as `Bearer {token}`.

## Error Handling

The service provides detailed error responses with:
- HTTP status codes for different error types
- Descriptive error messages for debugging
- Comprehensive logging for monitoring and troubleshooting
