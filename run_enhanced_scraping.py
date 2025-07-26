#!/usr/bin/env python3
"""
Complete enhanced scraping process for NBA data
"""

import asyncio
import sys
from datetime import datetime
from loguru import logger
from pathlib import Path

def setup_logging():
    """Setup comprehensive logging"""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        "logs/enhanced_scraping_complete.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG"
    )

async def run_complete_scraping():
    """Run the complete enhanced scraping process"""
    start_time = datetime.now()
    logger.info("Starting complete enhanced NBA data scraping process")
    
    try:
        # Step 1: Setup and verify
        logger.info("Step 1: Setting up database and verifying requirements...")
        from models import create_tables
        create_tables()
        
        # Step 2: Scrape recent seasons for testing
        logger.info("Step 2: Scraping recent seasons for prediction testing...")
        from scrape_recent_data import scrape_recent_seasons_for_testing
        await scrape_recent_seasons_for_testing()
        
        # Step 3: Scrape additional data sources
        logger.info("Step 3: Scraping additional data sources...")
        from scrape_recent_data import scrape_additional_data_sources
        await scrape_additional_data_sources()
        
        # Step 4: Update existing data
        logger.info("Step 4: Updating existing data for last 5 seasons...")
        from scrape_recent_data import update_existing_data
        await update_existing_data()
        
        # Step 5: Analyze the data
        logger.info("Step 5: Analyzing scraped data...")
        from prediction_data_analysis import main as analyze_data
        analyze_data()
        
        # Step 6: Generate summary report
        logger.info("Step 6: Generating summary report...")
        await generate_summary_report()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"Complete enhanced scraping process finished successfully!")
        logger.info(f"Total duration: {duration}")
        
        print("\n" + "="*80)
        print("ENHANCED SCRAPING PROCESS COMPLETE")
        print("="*80)
        print(f"Duration: {duration}")
        print("Data is now ready for prediction testing!")
        print("\nNext steps:")
        print("1. Review the logs in logs/enhanced_scraping_complete.log")
        print("2. Check the generated features in features/")
        print("3. Use the testing dataset for prediction validation")
        print("4. Run additional analysis as needed")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error during enhanced scraping process: {e}")
        raise

async def generate_summary_report():
    """Generate a summary report of the scraping process"""
    from database_manager import DatabaseManager
    
    db = DatabaseManager()
    stats = db.get_database_stats()
    
    # Create reports directory
    Path("reports").mkdir(exist_ok=True)
    
    # Generate summary report
    report = {
        "timestamp": datetime.now().isoformat(),
        "database_stats": stats,
        "data_sources": {
            "recent_seasons": list(range(datetime.now().year - 2, datetime.now().year + 1)),
            "additional_sources": ["team_rosters", "player_profiles", "historical_data", "season_leaders", "team_advanced_stats"],
            "files_generated": []
        }
    }
    
    # Check for generated files
    data_dir = Path("data")
    if data_dir.exists():
        report["data_sources"]["files_generated"] = [f.name for f in data_dir.glob("*.json")]
    
    features_dir = Path("features")
    if features_dir.exists():
        report["data_sources"]["files_generated"].extend([f.name for f in features_dir.glob("*.csv")])
    
    # Save report
    import json
    report_file = "reports/scraping_summary.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Summary report saved to {report_file}")

def main():
    """Main function"""
    setup_logging()
    
    print("="*80)
    print("ENHANCED NBA DATA SCRAPING SYSTEM")
    print("="*80)
    print("This will:")
    print("1. Scrape recent seasons for prediction testing")
    print("2. Collect additional data sources")
    print("3. Update existing data for the last 5 seasons")
    print("4. Analyze the data for prediction features")
    print("5. Generate testing datasets")
    print("="*80)
    
    # Confirm with user
    response = input("\nDo you want to proceed with the complete scraping process? (y/N): ")
    if response.lower() != 'y':
        print("Scraping cancelled.")
        return
    
    # Run the complete process
    asyncio.run(run_complete_scraping())

if __name__ == "__main__":
    main() 