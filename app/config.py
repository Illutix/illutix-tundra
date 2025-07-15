from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    """Production settings for Illutix Tundra conversion service"""
    
    # Server configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    ENV: str = "production"
    
    # CORS configuration for production
    ALLOWED_ORIGINS: List[str] = [
        "https://illutix.com",
        "https://www.illutix.com",
        "https://app.illutix.com"
    ]
    
    # Processing limits for production safety
    MAX_FILE_SIZE_MB: int = 500
    MAX_API_RESPONSE_MB: int = 100
    MAX_SQL_RESPONSE_MB: int = 100
    MAX_PROCESSING_TIME_MINUTES: int = 10
    
    # Memory and performance settings
    MAX_MEMORY_USAGE_GB: int = 2
    MAX_CONCURRENT_CONVERSIONS: int = 5
    
    # Supported formats
    SUPPORTED_FILE_FORMATS: List[str] = ["csv", "tsv", "json", "geojson"]
    
    # Parquet optimization settings
    PARQUET_COMPRESSION: str = "snappy"
    PARQUET_ROW_GROUP_SIZE: int = 50000
    
    # Default query limits for safety
    DEFAULT_SQL_LIMIT: int = 100000
    MAX_SQL_LIMIT: int = 1000000
    
    # Logging configuration
    LOG_LEVEL: str = "INFO"
    
    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"

# Create settings instance
settings = Settings()