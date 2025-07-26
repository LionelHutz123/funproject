import os
from pathlib import Path
from typing import List, Optional
from pydantic import BaseSettings

class Settings(BaseSettings):
    # Database settings
    DATABASE_URL: str = "sqlite:///basketball_data.db"
    
    # Scraping settings
    BASE_URL: str = "https://www.basketball-reference.com"
    REQUEST_DELAY: float = 1.0
    MAX_RETRIES: int = 3
    TIMEOUT: int = 30
    
    # File paths
    DATA_DIR: Path = Path("data")
    SCORES_DIR: Path = Path("SCORES_DIR")
    LOGS_DIR: Path = Path("logs")
    
    # Seasons to scrape
    START_SEASON: int = 2009
    END_SEASON: int = 2025
    
    # Browser settings
    HEADLESS: bool = True
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    class Config:
        env_file = ".env"

settings = Settings()

# Create directories if they don't exist
for directory in [settings.DATA_DIR, settings.SCORES_DIR, settings.LOGS_DIR]:
    directory.mkdir(exist_ok=True) 