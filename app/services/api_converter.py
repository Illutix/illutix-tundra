import polars as pl
import httpx
import json
import time
import logging
from io import BytesIO
from typing import Dict, Any, Optional
from ..models.conversionRequest import ConversionMetadata

logger = logging.getLogger(__name__)

class ApiConverter:
    """Production API data converter - API to Parquet"""
    
    # Processing limits
    MAX_RESPONSE_SIZE = 100 * 1024 * 1024  # 100MB max API response
    TIMEOUT_SECONDS = 300  # 5 minutes max for API call
    
    @staticmethod
    async def convert(
        endpoint: str,
        output_url: str,
        credentials_id: Optional[str] = None,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch data from API and convert to parquet"""
        
        start_time = time.time()
        
        try:
            logger.info(f"🔗 Starting API conversion: {endpoint}")
            
            # 1. Fetch data from API
            api_data = await ApiConverter._fetch_api_data(
                endpoint, method, headers, credentials_id
            )
            
            # 2. Extract target data using path
            target_data = ApiConverter._extract_data(api_data, data_path)
            
            # 3. Convert to Polars DataFrame
            df = ApiConverter._create_dataframe(target_data)
            
            # 4. Convert to parquet
            parquet_buffer = ApiConverter._convert_to_parquet(df)
            
            # 5. Upload to R2
            await ApiConverter._upload_parquet(output_url, parquet_buffer)
            
            # 6. Generate metadata
            processing_time = time.time() - start_time
            file_size_mb = len(parquet_buffer) / 1024 / 1024
            
            metadata = ConversionMetadata(
                rows=len(df),
                columns=len(df.columns),
                schema=ApiConverter._generate_schema(df),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="api"
            )
            
            logger.info(f"✅ API conversion successful: {len(df)} rows, {file_size_mb:.2f}MB")
            
            return {
                "success": True,
                "metadata": metadata.dict()
            }
            
        except Exception as e:
            logger.error(f"❌ API conversion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    @staticmethod
    async def _fetch_api_data(
        endpoint: str,
        method: str,
        headers: Optional[Dict[str, str]],
        credentials_id: Optional[str]
    ) -> Any:
        """Fetch data from API endpoint with authentication"""
        
        # Build request headers
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        
        # Add authentication if credentials provided
        if credentials_id:
            auth_headers = await ApiConverter._get_auth_headers(credentials_id)
            request_headers.update(auth_headers)
        
        # Make API request with size and timeout limits
        async with httpx.AsyncClient(timeout=ApiConverter.TIMEOUT_SECONDS) as client:
            response = await client.request(method, endpoint, headers=request_headers)
            response.raise_for_status()
            
            # Check response size
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > ApiConverter.MAX_RESPONSE_SIZE:
                raise ValueError(f"API response too large: {int(content_length)/1024/1024:.1f}MB exceeds 100MB limit")
            
            # Parse JSON response
            data = response.json()
            
            # Additional size check on parsed data
            data_size = len(json.dumps(data).encode('utf-8'))
            if data_size > ApiConverter.MAX_RESPONSE_SIZE:
                raise ValueError(f"API response too large after parsing: {data_size/1024/1024:.1f}MB exceeds 100MB limit")
            
            logger.info(f"📥 Fetched API data: {data_size/1024/1024:.2f}MB")
            return data
    
    @staticmethod
    async def _get_auth_headers(credentials_id: str) -> Dict[str, str]:
        """Get authentication headers from stored credentials"""
        
        # TODO: Implement credential retrieval from your vault/storage
        # This is a placeholder - you'll need to implement based on your credential storage
        
        logger.warning(f"🔐 Credential retrieval not implemented for: {credentials_id}")
        return {}
        
        # Example implementation:
        # credentials = await vault_client.get_credential(credentials_id)
        # 
        # if credentials['type'] == 'bearer':
        #     return {"Authorization": f"Bearer {credentials['value']}"}
        # elif credentials['type'] == 'api-key':
        #     key_data = json.loads(credentials['value'])
        #     header_name = key_data.get('header', 'X-API-Key')
        #     return {header_name: key_data['apiKey']}
        # elif credentials['type'] == 'basic':
        #     import base64
        #     user_data = json.loads(credentials['value'])
        #     encoded = base64.b64encode(f"{user_data['username']}:{user_data['password']}".encode()).decode()
        #     return {"Authorization": f"Basic {encoded}"}
        # 
        # return {}
    
    @staticmethod
    def _extract_data(api_data: Any, data_path: Optional[str]) -> list:
        """Extract target data from API response using path"""
        
        target_data = api_data
        
        # Navigate data path if provided
        if data_path:
            path_parts = data_path.split('.')
            for part in path_parts:
                if target_data and isinstance(target_data, dict) and part in target_data:
                    target_data = target_data[part]
                else:
                    raise ValueError(f"Data path '{data_path}' not found in API response")
        
        # Ensure we have an array
        if not isinstance(target_data, list):
            if isinstance(target_data, dict):
                # Single object - wrap in array
                target_data = [target_data]
            else:
                raise ValueError("API response must contain an array or object")
        
        if len(target_data) == 0:
            logger.warning("⚠️ API returned empty dataset")
        
        logger.info(f"📊 Extracted {len(target_data)} records from API response")
        return target_data
    
    @staticmethod
    def _create_dataframe(data: list) -> pl.DataFrame:
        """Create Polars DataFrame from API data"""
        
        if not data:
            # Return empty DataFrame with minimal structure
            return pl.DataFrame({"_empty": []})
        
        try:
            # Use Polars to create DataFrame from list of dictionaries
            df = pl.DataFrame(data)
            logger.info(f"📋 Created DataFrame: {len(df)} rows × {len(df.columns)} columns")
            return df
            
        except Exception as e:
            raise ValueError(f"Failed to create DataFrame from API data: {str(e)}")
    
    @staticmethod
    def _convert_to_parquet(df: pl.DataFrame) -> bytes:
        """Convert DataFrame to optimized parquet format"""
        
        buffer = BytesIO()
        
        df.write_parquet(
            buffer,
            compression="snappy",
            use_pyarrow=False,
            statistics=True,
            row_group_size=50000
        )
        
        parquet_data = buffer.getvalue()
        logger.info(f"📦 Generated parquet: {len(parquet_data)/1024/1024:.2f}MB")
        
        return parquet_data
    
    @staticmethod
    async def _upload_parquet(output_url: str, parquet_data: bytes) -> None:
        """Upload parquet data to R2 using signed URL"""
        
        async with httpx.AsyncClient(timeout=300) as client:
            response = await client.put(
                output_url,
                content=parquet_data,
                headers={"Content-Type": "application/x-parquet"}
            )
            response.raise_for_status()
            
        logger.info(f"📤 Uploaded parquet to R2: {len(parquet_data)/1024/1024:.2f}MB")
    
    @staticmethod
    def _generate_schema(df: pl.DataFrame) -> Dict[str, Any]:
        """Generate schema information for the dataset"""
        
        fields = []
        for col, dtype in zip(df.columns, df.dtypes):
            # Skip internal fields
            if col.startswith('_') and col != '_empty':
                continue
                
            field_info = {
                "name": col,
                "type": str(dtype),
                "polars_type": str(dtype)
            }
            
            # Add nullability info
            try:
                field_info["nullable"] = df.select(pl.col(col).is_null().any()).item()
            except:
                field_info["nullable"] = True
            
            fields.append(field_info)
        
        return {
            "fields": fields,
            "format": "parquet",
            "encoding": "utf-8",
            "source": "api"
        }