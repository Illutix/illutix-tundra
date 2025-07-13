import httpx
import hashlib
from pathlib import Path
import tempfile
import logging

logger = logging.getLogger(__name__)

async def download_file(url: str) -> Path:
    """Download file from URL to temporary location"""
    try:
        # Create temp file with URL hash as name
        url_hash = hashlib.md5(url.encode()).hexdigest()
        temp_dir = Path(tempfile.gettempdir()) / "polars_server"
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / f"{url_hash}.tmp"
        
        # Download file
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                
                with open(temp_path, "wb") as f:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
        
        logger.info(f"Downloaded file to {temp_path}")
        return temp_path
        
    except Exception as e:
        logger.error(f"Error downloading file from {url}: {str(e)}")
        raise