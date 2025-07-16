# Polars Data Processing Service

High-performance data processing service using Python + Polars for enterprise-scale datasets.

## Features

- **Polars-powered**: Lightning-fast CSV/TSV/JSON processing
- **Business-friendly GeoJSON**: Automatic transformation for visualization
- **Preview mode**: Fast data exploration and testing
- **Clean architecture**: Maintainable, modular codebase
- **Production-ready**: Docker, Cloud Run deployment

## Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Create data directory and add test files
mkdir data
# Copy your data files to ./data/

# Run the service
uvicorn app.main:app --reload

# Service available at http://localhost:8000
```

### API Usage

```bash
# Health check
curl http://localhost:8000/health

# List available datasets (dev)
curl http://localhost:8000/datasources

# Get preview data
curl "http://localhost:8000/datasources/your-file/data?preview=true&limit=100"

# Parse from signed URL
curl -X POST "http://localhost:8000/parse/file" \
  -H "Content-Type: application/json" \
  -d '{
    "signed_url": "https://...",
    "file_format": "csv",
    "preview": true,
    "limit": 100
  }'

# Parse API data
curl -X POST "http://localhost:8000/parse/api" \
  -H "Content-Type: application/json" \
  -d '{
    "endpoint": "https://api.example.com/data",
    "method": "GET",
    "preview": true,
    "limit": 100
  }'

# Execute SQL query
curl -X POST "http://localhost:8000/parse/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "endpoint": "https://sql-api.example.com",
    "database": "analytics",
    "query": "SELECT * FROM sales",
    "preview": true,
    "limit": 100
  }'
```

## Deployment

### Docker

```bash
# Build
docker build -t polars-data-service .

# Run
docker run -p 8000:8000 polars-data-service
```

### Google Cloud Run

```bash
# Replace YOUR_PROJECT_ID with your actual project ID
PROJECT_ID="your-project-id"

# Build and deploy
gcloud builds submit --tag gcr.io/$PROJECT_ID/polars-data-service
gcloud run deploy polars-data-service --image gcr.io/$PROJECT_ID/polars-data-service --platform managed
```

## Environment Variables

- `ENV`: `development` or `production`
- `ALLOWED_ORIGINS`: List of allowed CORS origins (e.g., `["https://yourdomain.com","https://www.yourdomain.com"]`)
- `MAX_PREVIEW_ROWS`: Maximum rows for preview (default: 10000)
- `DEFAULT_PREVIEW_ROWS`: Default preview size (default: 1000)

## Architecture

```
polars-server/
├── app/
│   ├── main.py              # FastAPI app + routing
│   ├── config.py            # Environment configuration
│   ├── models/              # Pydantic schemas
│   │   └── parse.py
│   └── services/            # Core business logic
│       ├── file_parser.py   # File processing with Polars
│       ├── api_parser.py    # Remote API data fetching
│       ├── sql_parser.py    # SQL query execution
│       └── cleanup.py       # Temporary file cleanup
├── requirements.txt
├── Dockerfile
└── cloudrun.yaml           # Cloud Run deployment config
```

## Integration with Next.js

```typescript
// Updated DataEngine for Polars service
class DataEngine {
    constructor(config) {
        this.config = config;
        this.baseUrl = process.env.NEXT_PUBLIC_POLARS_SERVICE_URL || 'http://localhost:8000';
    }
    
    async load(options = {}) {
        const endpoint = this.getEndpoint();
        const body = this.buildRequestBody(options);
        
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        
        return await response.json();
    }
    
    private getEndpoint() {
        switch (this.config.type) {
            case 'file': return `${this.baseUrl}/parse/file`;
            case 'api': return `${this.baseUrl}/parse/api`;
            case 'sql': return `${this.baseUrl}/parse/sql`;
        }
    }
}