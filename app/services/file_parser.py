import polars as pl
import json
import logging
import httpx
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional
import asyncio
from ..config import settings

logger = logging.getLogger(__name__)

class FileParser:
    """Polars-native file parsing with controlled disk usage"""
    
    # Controlled disk limits
    PREVIEW_MAX_DOWNLOAD = 10 * 1024 * 1024   # 10MB for preview
    FULL_MAX_DOWNLOAD = 100 * 1024 * 1024     # 100MB for full processing
    CHUNK_SIZE = 8 * 1024 * 1024              # 8MB download chunks
    
    @staticmethod
    async def parse_from_url(
        signed_url: str, 
        file_format: str, 
        preview: bool = False, 
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Parse file using Polars native methods with controlled temp storage"""
        
        try:
            if file_format in ["csv", "tsv"]:
                return await FileParser._parse_csv_native(
                    signed_url, file_format, preview, limit
                )
            elif file_format == "json":
                return await FileParser._parse_json_native(
                    signed_url, preview, limit
                )
            elif file_format == "geojson":
                return await FileParser._parse_geojson_controlled(
                    signed_url, preview, limit
                )
            else:
                raise ValueError(f"Unsupported format: {file_format}")
                
        except Exception as e:
            logger.error(f"Error parsing with Polars: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0}
            }
    
    @staticmethod
    async def _parse_csv_native(
        url: str, 
        file_format: str, 
        preview: bool, 
        limit: int
    ) -> Dict[str, Any]:
        """Use Polars scan_csv with streaming engine"""
        
        max_download = FileParser.PREVIEW_MAX_DOWNLOAD if preview else FileParser.FULL_MAX_DOWNLOAD
        temp_path = await FileParser._download_controlled(url, max_download)
        
        try:
            separator = '\t' if file_format == 'tsv' else ','
            
            # Build Polars query with lazy evaluation
            query = pl.scan_csv(
                temp_path,
                separator=separator,
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                ignore_errors=True,
            )
            
            # Apply preview limit using Polars lazy operations
            if preview:
                query = query.head(limit)
            elif limit and limit > 0:
                query = query.head(limit)
            
            # Execute with Polars streaming engine
            logger.info(f"Executing Polars query with streaming engine, preview={preview}, limit={limit}")
            df = query.collect(streaming=True)
            
            return {
                "success": True,
                "data": df.to_dicts(),
                "metadata": {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": df.columns,
                    "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                    "was_truncated": preview or (limit and len(df) == limit),
                    "polars_streaming": True,
                    "parsing_engine": "polars_native",
                    "temp_file_size_mb": round(temp_path.stat().st_size / 1024 / 1024, 2),
                    "file_format": file_format
                }
            }
            
        finally:
            temp_path.unlink(missing_ok=True)
            logger.info(f"Cleaned up temp file: {temp_path}")
    
    @staticmethod
    async def _parse_json_native(
        url: str, 
        preview: bool, 
        limit: int
    ) -> Dict[str, Any]:
        """Parse JSON with Polars native methods"""
        
        max_download = FileParser.PREVIEW_MAX_DOWNLOAD if preview else FileParser.FULL_MAX_DOWNLOAD
        temp_path = await FileParser._download_controlled(url, max_download)
        
        try:
            # Always use Polars native JSON reading
            df = pl.read_json(temp_path)
            
            # Apply limits after reading
            if preview and len(df) > limit:
                df = df.head(limit)
                was_truncated = True
            else:
                was_truncated = False
            
            return {
                "success": True,
                "data": df.to_dicts(),
                "metadata": {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": df.columns,
                    "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                    "was_truncated": was_truncated,
                    "polars_streaming": True,
                    "parsing_engine": "polars_json",
                    "temp_file_size_mb": round(temp_path.stat().st_size / 1024 / 1024, 2)
                }
            }
                
        finally:
            temp_path.unlink(missing_ok=True)
    
    @staticmethod
    async def _parse_geojson_controlled(
        url: str, 
        preview: bool, 
        limit: int
    ) -> Dict[str, Any]:
        """Parse GeoJSON with controlled disk usage"""
        
        max_download = FileParser.PREVIEW_MAX_DOWNLOAD if preview else FileParser.FULL_MAX_DOWNLOAD
        temp_path = await FileParser._download_controlled(url, max_download)
        
        try:
            with open(temp_path, 'r') as f:
                geo_data = json.load(f)
            
            business_data = FileParser._process_geojson_features(geo_data, preview, limit)
            
            if business_data:
                df = pl.DataFrame(business_data)
                return {
                    "success": True,
                    "data": df.to_dicts(),
                    "metadata": {
                        "row_count": len(df),
                        "column_count": len([col for col in df.columns if not col.startswith('_')]),
                        "columns": [col for col in df.columns if not col.startswith('_')],
                        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes) if not col.startswith('_')},
                        "was_truncated": preview and len(business_data) >= limit,
                        "polars_streaming": True,
                        "parsing_engine": "geo_geojson",
                        "temp_file_size_mb": round(temp_path.stat().st_size / 1024 / 1024, 2),
                        "geo_fields": ["_geometry"],
                        "hidden_fields": [col for col in df.columns if col.startswith('_')]
                    }
                }
            
            return {
                "success": True,
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0, "polars_streaming": True}
            }
            
        finally:
            temp_path.unlink(missing_ok=True)
    
    @staticmethod
    async def _download_controlled(url: str, max_bytes: int) -> Path:
        """Download file with strict size limits to temp location"""
        
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        temp_dir = Path(tempfile.gettempdir()) / "polars_streaming"
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / f"polars_{url_hash}.tmp"
        
        bytes_downloaded = 0
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    
                    # Check content-length header
                    content_length = response.headers.get('content-length')
                    if content_length and int(content_length) > max_bytes:
                        raise ValueError(
                            f"File too large: {int(content_length)/1024/1024:.1f}MB "
                            f"exceeds limit of {max_bytes/1024/1024:.1f}MB"
                        )
                    
                    with open(temp_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=FileParser.CHUNK_SIZE):
                            bytes_downloaded += len(chunk)
                            
                            # Enforce strict download limit
                            if bytes_downloaded > max_bytes:
                                remaining = max_bytes - (bytes_downloaded - len(chunk))
                                if remaining > 0:
                                    f.write(chunk[:remaining])
                                break
                            
                            f.write(chunk)
            
            logger.info(f"Downloaded {bytes_downloaded/1024/1024:.2f}MB to {temp_path}")
            return temp_path
            
        except Exception as e:
            temp_path.unlink(missing_ok=True)
            raise e
    
    @staticmethod
    def parse_local_file(datasource_id: str, preview: bool = False, limit: int = 1000) -> Dict[str, Any]:
        """Parse local file using Polars native methods"""
        
        data_dir = Path(settings.DATA_DIR)
        
        # Find file - removed topojson support
        for ext in [".csv", ".json", ".tsv", ".geojson"]:
            file_path = data_dir / f"{datasource_id}{ext}"
            if file_path.exists():
                
                # Check file size
                file_size = file_path.stat().st_size
                max_size = FileParser.FULL_MAX_DOWNLOAD
                
                if file_size > max_size and not preview:
                    return {
                        "success": False,
                        "error": f"Local file too large ({file_size/1024/1024:.1f}MB). Use preview mode.",
                        "data": [],
                        "metadata": {"row_count": 0, "column_count": 0}
                    }
                
                return FileParser._parse_local_file_native(file_path, ext[1:], preview, limit)
        
        return {
            "success": False,
            "error": f"File not found for datasource {datasource_id}",
            "data": [],
            "metadata": {"row_count": 0, "column_count": 0}
        }
    
    @staticmethod
    def _parse_local_file_native(file_path: Path, file_format: str, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse local file with Polars native methods"""
        
        try:
            if file_format in ["csv", "tsv"]:
                separator = '\t' if file_format == 'tsv' else ','
                
                query = pl.scan_csv(
                    file_path,
                    separator=separator,
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True
                )
                
                if preview:
                    query = query.head(limit)
                elif limit:
                    query = query.head(limit)
                
                df = query.collect(streaming=True)
                
            elif file_format == "json":
                # Use Polars native JSON reading
                df = pl.read_json(file_path)
                
                if preview and len(df) > limit:
                    df = df.head(limit)
            
            elif file_format == "geojson":
                with open(file_path, 'r') as f:
                    geo_data = json.load(f)
                
                business_data = FileParser._process_geojson_features(geo_data, preview, limit)
                df = pl.DataFrame(business_data) if business_data else pl.DataFrame()
            
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            return {
                "success": True,
                "data": df.to_dicts(),
                "metadata": {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": df.columns,
                    "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                    "was_truncated": preview and len(df) == limit,
                    "polars_streaming": file_format in ["csv", "tsv"],
                    "parsing_engine": f"polars_local_{file_format}",
                    "local_file": True,
                    "file_size_mb": round(file_path.stat().st_size / 1024 / 1024, 2)
                }
            }
            
        except Exception as e:
            logger.error(f"Error parsing local file {file_path}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0}
            }
    
    @staticmethod
    def extract_nested_data(data: Any, data_path: str) -> Any:
        """Reusable function for nested path traversal"""
        target_data = data
        if data_path:
            for part in data_path.split('.'):
                if target_data and isinstance(target_data, dict) and part in target_data:
                    target_data = target_data[part]
                else:
                    raise ValueError(f"Data path '{data_path}' not found in response")
        return target_data
    
    @staticmethod
    def json_to_polars_via_temp(data: Any, preview: bool = False, limit: int = 1000) -> pl.DataFrame:
        """Convert JSON data to Polars DataFrame via temp file for consistency"""
        
        # Create temp file for JSON data
        temp_dir = Path(tempfile.gettempdir()) / "polars_streaming"
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / f"json_data_{hash(str(data))}.json"
        
        try:
            # Write JSON data to temp file
            with open(temp_path, 'w') as f:
                json.dump(data, f)
            
            # Use Polars native JSON reading
            df = pl.read_json(temp_path)
            
            # Apply limits
            if preview and len(df) > limit:
                df = df.head(limit)
            
            return df
            
        finally:
            temp_path.unlink(missing_ok=True)
    
    # GeoJSON processing helper methods
    @staticmethod
    def _process_geojson_features(geo_data: dict, preview: bool, limit: int) -> list:
        """Process GeoJSON features into business data"""
        if geo_data.get("type") != "FeatureCollection":
            raise ValueError("Invalid GeoJSON: Expected FeatureCollection")
        
        features = geo_data.get("features", [])
        business_data = []
        
        for i, feature in enumerate(features):
            if preview and i >= limit:
                break
                
            properties = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            
            row = {
                "feature_id": i + 1,
                "name": properties.get("name") or properties.get("NAME") or f"Feature {i + 1}",
                "geometry_type": geometry.get("type", "Unknown"),
                "area_type": FileParser._classify_geo_feature(geometry.get("type")),
                **properties,
                "coordinate_count": FileParser._count_coordinates(geometry),
                "has_interior_rings": FileParser._has_interior_rings(geometry),
                "_geometry": geometry,
                "_feature_index": i
            }
            business_data.append(row)
        
        return business_data
    
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
    def _count_coordinates(geometry: Dict) -> int:
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
    def _has_interior_rings(geometry: Dict) -> bool:
        return (geometry.get("type") == "Polygon" and 
                len(geometry.get("coordinates", [])) > 1)