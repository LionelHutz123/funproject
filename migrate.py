#!/usr/bin/env python3
"""
Migration script to transition from CSV-based system to database-driven system
"""

import os
import sys
from pathlib import Path
from loguru import logger

def check_requirements():
    """Check if all requirements are installed"""
    try:
        import sqlalchemy
        import pandas
        import beautifulsoup4
        import playwright
        import loguru
        import pydantic
        logger.info("All requirements are installed")
        return True
    except ImportError as e:
        logger.error(f"Missing requirement: {e}")
        logger.info("Please run: pip install -r requirements.txt")
        return False

def check_directories():
    """Check if required directories exist"""
    required_dirs = ['SCORES_DIR', 'logs', 'data']
    
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            logger.warning(f"Directory {dir_name} does not exist")
            if dir_name == 'SCORES_DIR':
                logger.error("SCORES_DIR is required for migration")
                return False
            else:
                os.makedirs(dir_name, exist_ok=True)
                logger.info(f"Created directory {dir_name}")
    
    return True

def check_html_files():
    """Check if HTML files exist in SCORES_DIR"""
    scores_dir = Path("SCORES_DIR")
    html_files = list(scores_dir.glob("*.html"))
    
    if not html_files:
        logger.error("No HTML files found in SCORES_DIR")
        return False
    
    logger.info(f"Found {len(html_files)} HTML files to migrate")
    return True

def setup_database():
    """Setup the database tables"""
    try:
        from models import create_tables
        create_tables()
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        return False

def process_existing_files():
    """Process existing HTML files"""
    try:
        from data_processor import DataProcessor
        processor = DataProcessor()
        processor.process_existing_files()
        logger.info("Successfully processed existing HTML files")
        return True
    except Exception as e:
        logger.error(f"Error processing existing files: {e}")
        return False

def verify_migration():
    """Verify that the migration was successful"""
    try:
        from database_manager import DatabaseManager
        db = DatabaseManager()
        stats = db.get_database_stats()
        
        if stats['total_games'] > 0:
            logger.info(f"Migration successful! Database contains {stats['total_games']} games")
            logger.info(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            return True
        else:
            logger.error("Migration failed - no games found in database")
            return False
    except Exception as e:
        logger.error(f"Error verifying migration: {e}")
        return False

def main():
    """Main migration function"""
    logger.info("Starting migration from CSV system to database system")
    
    # Step 1: Check requirements
    logger.info("Step 1: Checking requirements...")
    if not check_requirements():
        return False
    
    # Step 2: Check directories
    logger.info("Step 2: Checking directories...")
    if not check_directories():
        return False
    
    # Step 3: Check HTML files
    logger.info("Step 3: Checking HTML files...")
    if not check_html_files():
        return False
    
    # Step 4: Setup database
    logger.info("Step 4: Setting up database...")
    if not setup_database():
        return False
    
    # Step 5: Process existing files
    logger.info("Step 5: Processing existing HTML files...")
    if not process_existing_files():
        return False
    
    # Step 6: Verify migration
    logger.info("Step 6: Verifying migration...")
    if not verify_migration():
        return False
    
    logger.info("Migration completed successfully!")
    logger.info("You can now use the new system with:")
    logger.info("  python main.py stats          # View database statistics")
    logger.info("  python main.py standings --season 2023  # View standings")
    logger.info("  python main.py player --player 'LeBron James' --season 2023  # View player stats")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 