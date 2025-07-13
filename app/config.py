from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    """Application settings - no secrets needed for MVP"""
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    ENV: str = "development"
    
    # CORS - can be list or comma-separated string
    ALLOWED_ORIGINS: List[str] = ["http://localhost:3000","https://illutix.com","https://www.illutix.com"]
    
    # Data processing limits
    MAX_PREVIEW_ROWS: int = 10000
    DEFAULT_PREVIEW_ROWS: int = 1000
    
    # File storage (for local development only)
    DATA_DIR: str = "./data"
    
    class Config:
        # No .env file needed for MVP
        case_sensitive = True

settings = Settings()