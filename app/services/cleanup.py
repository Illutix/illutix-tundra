import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CleanupService:
    """Handle temporary file cleanup"""
    
    @staticmethod
    async def cleanup_temp_files(temp_dir: Path, max_age_hours: int = 1):
        """Clean up temporary files older than max_age_hours"""
        try:
            if not temp_dir.exists():
                return
            
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            
            for file_path in temp_dir.iterdir():
                if file_path.is_file():
                    file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_modified < cutoff_time:
                        file_path.unlink()
                        logger.info(f"Cleaned up temp file: {file_path}")
                        
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    @staticmethod
    async def start_cleanup_task(temp_dir: Path, interval_minutes: int = 30):
        """Start background cleanup task"""
        while True:
            await CleanupService.cleanup_temp_files(temp_dir)
            await asyncio.sleep(interval_minutes * 60)