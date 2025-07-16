import polars as pl
import httpx
import json
import time
import logging
from io import BytesIO
from typing import Dict, Any
from ..models.conversionRequest import ConversionMetadata

logger = logging.getLogger(__name__)

class FileConverter:
    """Production file converter - R2 to Parquet only"""
    
    # Processing limits for production safety
    MAX_DOWNLOAD_SIZE = 500 * 1024 * 1024  # 500MB max file size
    TIMEOUT_SECONDS = 600  # 10 minutes max processing
    
    @staticmethod
    async def convert(
        source_url: str,
        output_url: str, 
        file_format: str
    ) -> Dict[str, Any]:
        """Convert file from R2 source URL to parquet at output URL"""
        
        start_time = time.time()
        
        try:
            logger.info(f"🔄 Starting conversion: {file_format} → parquet")
            
            # 1. Download source file from R2
            file_content = await FileConverter._download_file(source_url)
            
            # 2. Parse with Polars based on format
            df = FileConverter._parse_file(file_content, file_format)
            
            # 3. Convert to parquet
            parquet_buffer = FileConverter._convert_to_parquet(df)
            
            # 4. Upload parquet to R2
            await FileConverter._upload_parquet(output_url, parquet_buffer)
            
            # 5. Generate metadata
            processing_time = time.time() - start_time
            file_size_mb = len(parquet_buffer) / 1024 / 1024
            
            metadata = ConversionMetadata(
                rows=len(df),
                columns=len(df.columns),
                schema=FileConverter._generate_schema(df),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="file"
            )
            
            logger.info(f"✅ Conversion successful: {len(df)} rows, {file_size_mb:.2f}MB")
            
            return {
                "success": True,
                "metadata": metadata.dict()
            }
            
        except Exception as e:
            logger.error(f"❌ Conversion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    @staticmethod
    async def _download_file(source_url: str) -> bytes:
        """Download file from R2 with size and timeout limits"""
        
        async with httpx.AsyncClient(timeout=FileConverter.TIMEOUT_SECONDS) as client:
            async with client.stream("GET", source_url) as response:
                response.raise_for_status()
                
                # Check content length
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > FileConverter.MAX_DOWNLOAD_SIZE:
                    raise ValueError(f"File too large: {int(content_length)/1024/1024:.1f}MB exceeds 500MB limit")
                
                # Download with size checking
                content = b""
                async for chunk in response.aiter_bytes(chunk_size=8*1024*1024):  # 8MB chunks
                    content += chunk
                    if len(content) > FileConverter.MAX_DOWNLOAD_SIZE:
                        raise ValueError("File size exceeds 500MB limit during download")
                
                logger.info(f"📥 Downloaded {len(content)/1024/1024:.2f}MB from R2")
                return content
    
    @staticmethod
    def _parse_file(content: bytes, file_format: str) -> pl.DataFrame:
        """Parse file content with Polars native methods"""
        
        try:
            if file_format == "csv":
                return pl.read_csv(
                    BytesIO(content),
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True
                )
            
            elif file_format == "tsv":
                return pl.read_csv(
                    BytesIO(content),
                    separator='\t',
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True
                )
            
            elif file_format == "json":
                return pl.read_json(BytesIO(content))
            
            elif file_format == "geojson":
                # Parse GeoJSON and convert to business-friendly format
                geo_data = json.loads(content.decode('utf-8'))
                return FileConverter._process_geojson(geo_data)
            
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
        except Exception as e:
            raise ValueError(f"Failed to parse {file_format} file: {str(e)}")
    
    @staticmethod
    def _process_geojson(geo_data: dict) -> pl.DataFrame:
        """Convert GeoJSON to business-friendly tabular format"""
        
        if geo_data.get("type") != "FeatureCollection":
            raise ValueError("Invalid GeoJSON: Expected FeatureCollection")
        
        features = geo_data.get("features", [])
        business_data = []
        
        for i, feature in enumerate(features):
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            
            row = {
                "feature_id": i + 1,
                "name": properties.get("name") or properties.get("NAME") or f"Feature {i + 1}",
                "geometry_type": geometry.get("type", "Unknown"),
                "area_type": FileConverter._classify_geo_feature(geometry.get("type")),
                **properties,
                "coordinate_count": FileConverter._count_coordinates(geometry),
                "has_interior_rings": FileConverter._has_interior_rings(geometry),
                "_geometry": json.dumps(geometry),  # Store as JSON string
                "_feature_index": i
            }
            business_data.append(row)
        
        if not business_data:
            # Return empty DataFrame with standard structure
            return pl.DataFrame({
                "feature_id": [],
                "name": [],
                "geometry_type": [],
                "area_type": []
            })
        
        return pl.DataFrame(business_data)
    
    @staticmethod
    def _convert_to_parquet(df: pl.DataFrame) -> bytes:
        """Convert DataFrame to optimized parquet format"""
        
        buffer = BytesIO()
        
        # Write with optimized settings for visualization use cases
        df.write_parquet(
            buffer,
            compression="snappy",  # Good balance of speed vs size
            use_pyarrow=False,     # Use Polars native (faster)
            statistics=True,       # Enable column statistics
            row_group_size=50000   # Optimize for query performance
        )
        
        parquet_data = buffer.getvalue()
        logger.info(f"📦 Generated parquet: {len(parquet_data)/1024/1024:.2f}MB")
        
        return parquet_data
    
    @staticmethod
    async def _upload_parquet(output_url: str, parquet_data: bytes) -> None:
        """Upload parquet data to R2 using signed URL"""
        
        async with httpx.AsyncClient(timeout=300) as client:  # 5 min timeout for upload
            response = await client.put(
                output_url,
                content=parquet_data,
                headers={"Content-Type": "application/octet-stream"}
            )
            response.raise_for_status()
            
        logger.info(f"📤 Uploaded parquet to R2: {len(parquet_data)/1024/1024:.2f}MB")
    
    @staticmethod
    def _generate_schema(df: pl.DataFrame) -> Dict[str, Any]:
        """Generate schema information for the dataset"""
        
        fields = []
        for col, dtype in zip(df.columns, df.dtypes):
            # Skip internal/hidden fields from schema
            if col.startswith('_'):
                continue
                
            field_info = {
                "name": col,
                "type": str(dtype),
                "polars_type": str(dtype)
            }
            
            # Add basic stats for numeric columns
            if dtype.is_numeric():
                try:
                    stats = df.select(pl.col(col)).describe()
                    field_info["nullable"] = df.select(pl.col(col).is_null().any()).item()
                except:
                    pass  # Skip stats if calculation fails
            
            fields.append(field_info)
        
        return {
            "fields": fields,
            "format": "parquet",
            "encoding": "utf-8"
        }
    
    # Helper methods for GeoJSON processing
    @staticmethod
    def _classify_geo_feature(geometry_type: str) -> str:
        mapping = {
            "Point": "Location",
            "LineString": "Route/Boundary",
            "MultiLineString": "Route/Boundary", 
            "Polygon": "Area/Region",
            "MultiPolygon": "Area/Region"
        }
        return mapping.get(geometry_type, "Geographic Feature")
    
    @staticmethod
    def _count_coordinates(geometry: dict) -> int:
        if not geometry or "coordinates" not in geometry:
            return 0
        
        def count_recursive(coords):
            if not isinstance(coords, list):
                return 0
            if len(coords) == 2 and all(isinstance(x, (int, float)) for x in coords):
                return 1
            return sum(count_recursive(item) for item in coords)
        
        return count_recursive(geometry["coordinates"])
    
    @staticmethod
    def _has_interior_rings(geometry: dict) -> bool:
        return (geometry.get("type") == "Polygon" and 
                len(geometry.get("coordinates", [])) > 1)