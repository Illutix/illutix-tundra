import polars as pl
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from ..utils.fetch import download_file
from ..config import settings

logger = logging.getLogger(__name__)

class FileParser:
    """Parse files from R2 signed URLs or local storage"""
    
    @staticmethod
    async def parse_from_url(signed_url: str, file_format: str, preview: bool = False, limit: int = 1000) -> Dict[str, Any]:
        """Parse file from signed URL"""
        try:
            # Download file to temp location
            temp_path = await download_file(signed_url)
            
            # Parse based on format
            result = FileParser._parse_file(temp_path, file_format, preview, limit)
            
            # Cleanup temp file
            temp_path.unlink(missing_ok=True)
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing file from URL {signed_url}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0}
            }
    
    @staticmethod
    def parse_local_file(datasource_id: str, preview: bool = False, limit: int = 1000) -> Dict[str, Any]:
        """Parse local file (for development)"""
        data_dir = Path(settings.DATA_DIR)
        
        # Find file by trying different extensions
        for ext in [".csv", ".json", ".tsv", ".geojson", ".topojson"]:
            file_path = data_dir / f"{datasource_id}{ext}"
            if file_path.exists():
                return FileParser._parse_file(file_path, ext[1:], preview, limit)
        
        return {
            "success": False,
            "error": f"File not found for datasource {datasource_id}",
            "data": [],
            "metadata": {"row_count": 0, "column_count": 0}
        }
    
    @staticmethod
    def _parse_file(file_path: Path, file_format: str, preview: bool, limit: int) -> Dict[str, Any]:
        """Internal file parsing logic"""
        try:
            if file_format == "csv":
                return FileParser._parse_csv(file_path, preview, limit)
            elif file_format == "tsv":
                return FileParser._parse_tsv(file_path, preview, limit)
            elif file_format == "json":
                return FileParser._parse_json(file_path, preview, limit)
            elif file_format == "geojson":
                return FileParser._parse_geojson(file_path, preview, limit)
            elif file_format == "topojson":
                return FileParser._parse_topojson(file_path, preview, limit)
            else:
                raise ValueError(f"Unsupported format: {file_format}")
                
        except Exception as e:
            logger.error(f"Error parsing {file_format} file {file_path}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "metadata": {"row_count": 0, "column_count": 0}
            }
    
    @staticmethod
    def _parse_csv(file_path: Path, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse CSV file with Polars"""
        df = pl.scan_csv(str(file_path))
        
        if preview:
            df = df.head(limit)
        
        result_df = df.collect()
        
        return {
            "success": True,
            "data": result_df.to_dicts(),
            "metadata": {
                "row_count": len(result_df),
                "column_count": len(result_df.columns),
                "columns": result_df.columns,
                "schema": {col: str(dtype) for col, dtype in zip(result_df.columns, result_df.dtypes)},
                "was_truncated": preview and len(result_df) == limit
            }
        }
    
    @staticmethod
    def _parse_tsv(file_path: Path, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse TSV file with Polars"""
        df = pl.scan_csv(str(file_path), separator='\t')
        
        if preview:
            df = df.head(limit)
        
        result_df = df.collect()
        
        return {
            "success": True,
            "data": result_df.to_dicts(),
            "metadata": {
                "row_count": len(result_df),
                "column_count": len(result_df.columns),
                "columns": result_df.columns,
                "schema": {col: str(dtype) for col, dtype in zip(result_df.columns, result_df.dtypes)},
                "was_truncated": preview and len(result_df) == limit
            }
        }
    
    @staticmethod
    def _parse_json(file_path: Path, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse JSON file with Polars"""
        df = pl.read_json(str(file_path))
        
        if preview:
            df = df.head(limit)
        
        return {
            "success": True,
            "data": df.to_dicts(),
            "metadata": {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                "was_truncated": preview and len(df) == limit
            }
        }
    
    @staticmethod
    def _parse_geojson(file_path: Path, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse GeoJSON with business transformation"""
        with open(file_path, 'r') as f:
            geo_data = json.load(f)
        
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
                    "was_truncated": preview and len(features) > limit,
                    "geo_fields": ["_geometry"],
                    "hidden_fields": [col for col in df.columns if col.startswith('_')]
                }
            }
        
        return {
            "success": True,
            "data": [],
            "metadata": {"row_count": 0, "column_count": 0}
        }
    
    @staticmethod
    def _parse_topojson(file_path: Path, preview: bool, limit: int) -> Dict[str, Any]:
        """Parse TopoJSON with business transformation"""
        with open(file_path, 'r') as f:
            topo_data = json.load(f)
        
        if topo_data.get("type") != "Topology":
            raise ValueError("Invalid TopoJSON: Expected Topology")
        
        objects = topo_data.get("objects", {})
        arcs = topo_data.get("arcs", [])
        business_data = []
        
        for i, (name, obj) in enumerate(objects.items()):
            if preview and i >= limit:
                break
                
            row = {
                "object_id": i + 1,
                "object_name": name,
                "feature_type": obj.get("type", "Unknown"),
                "feature_count": len(obj.get("geometries", [])) if obj.get("geometries") else 1,
                **(obj.get("properties", {})),
                "_topology_object": obj,
                "_arcs": arcs
            }
            business_data.append(row)
        
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
                    "was_truncated": preview and len(objects) > limit,
                    "hidden_fields": [col for col in df.columns if col.startswith('_')]
                }
            }
        
        return {
            "success": True,
            "data": [],
            "metadata": {"row_count": 0, "column_count": 0}
        }
    
    # Helper methods
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