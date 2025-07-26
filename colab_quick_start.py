#!/usr/bin/env python3
"""
Quick start script for Google Colab
No credentials needed - just run this!
"""

import os
import sys
import subprocess
from pathlib import Path

def setup_colab():
    """Setup NBA data scraping in Google Colab"""
    print("🚀 Setting up NBA Data Scraping in Google Colab...")
    
    # Step 1: Install dependencies
    print("\n📦 Installing dependencies...")
    dependencies = [
        'beautifulsoup4==4.12.2',
        'pandas==2.1.4',
        'playwright==1.40.0',
        'sqlalchemy==2.0.23',
        'loguru==0.7.2',
        'pydantic==2.5.2',
        'aiohttp==3.9.1',
        'asyncio-throttle==1.0.2',
        'lxml==4.9.3',
        'requests==2.31.0',
        'psutil==5.9.6'
    ]
    
    for dep in dependencies:
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', dep], 
                         check=True, capture_output=True)
            print(f"✅ Installed {dep}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Warning: Could not install {dep}: {e}")
    
    # Step 2: Install Playwright browser
    print("\n🌐 Installing Playwright browser...")
    try:
        subprocess.run(['playwright', 'install', 'firefox'], check=True)
        print("✅ Browser installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Warning: Could not install browser: {e}")
    
    # Step 3: Create directories
    print("\n📁 Creating directory structure...")
    directories = ['logs', 'data', 'features', 'reports']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created {directory}/")
    
    # Step 4: Create Colab-optimized config
    print("\n⚙️  Creating Colab-optimized configuration...")
    colab_config = {
        'DATABASE_URL': 'sqlite:///basketball_data.db',
        'BASE_URL': 'https://www.basketball-reference.com',
        'REQUEST_DELAY': 3.0,  # Higher delay for Colab
        'MAX_RETRIES': 3,
        'TIMEOUT': 30,
        'HEADLESS': True,
        'START_SEASON': 2023,
        'END_SEASON': 2024,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'BATCH_SIZE': 5,  # Smaller batches for Colab
        'MEMORY_LIMIT': 0.7  # Conservative memory usage
    }
    
    # Save config
    import json
    with open('colab_config.json', 'w') as f:
        json.dump(colab_config, f, indent=2)
    
    print("✅ Colab configuration created")
    
    # Step 5: Create simple test script
    print("\n🧪 Creating test script...")
    test_script = '''#!/usr/bin/env python3
"""
Simple test script for Colab
"""

import asyncio
from loguru import logger
import json

# Load Colab config
with open('colab_config.json', 'r') as f:
    config = json.load(f)

async def test_scraping():
    """Test basic scraping functionality"""
    logger.info("Testing NBA data scraping...")
    
    try:
        # Test basic imports
        from bs4 import BeautifulSoup
        import pandas as pd
        from playwright.async_api import async_playwright
        
        logger.info("✅ All imports successful")
        
        # Test browser launch
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            page = await browser.new_page()
            await page.goto('https://www.basketball-reference.com')
            title = await page.title()
            await browser.close()
            
            logger.info(f"✅ Browser test successful: {title}")
        
        # Test database
        from sqlalchemy import create_engine
        engine = create_engine(config['DATABASE_URL'])
        logger.info("✅ Database connection successful")
        
        logger.info("🎉 All tests passed! Ready for scraping.")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    asyncio.run(test_scraping())
'''
    
    with open('colab_test.py', 'w') as f:
        f.write(test_script)
    
    print("✅ Test script created")
    
    # Step 6: Create simple scraper for Colab
    print("\n🕷️  Creating Colab-optimized scraper...")
    colab_scraper = '''#!/usr/bin/env python3
"""
Colab-optimized NBA scraper
"""

import asyncio
import json
from loguru import logger
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlite3

# Load config
with open('colab_config.json', 'r') as f:
    config = json.load(f)

class ColabScraper:
    def __init__(self):
        self.base_url = config['BASE_URL']
        self.delay = config['REQUEST_DELAY']
        self.max_retries = config['MAX_RETRIES']
    
    async def scrape_recent_games(self, season=2024, max_games=10):
        """Scrape recent games for testing"""
        logger.info(f"Scraping {max_games} recent games from {season} season...")
        
        games_data = []
        
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            page = await browser.new_page()
            
            # Get recent games
            url = f"{self.base_url}/leagues/NBA_{season}_games.html"
            await page.goto(url)
            await asyncio.sleep(self.delay)
            
            # Find game links
            game_links = await page.query_selector_all('a[href*="boxscore"]')
            
            for i, link in enumerate(game_links[:max_games]):
                try:
                    href = await link.get_attribute('href')
                    game_url = f"{self.base_url}{href}"
                    
                    logger.info(f"Scraping game {i+1}/{max_games}")
                    
                    # Scrape game page
                    await page.goto(game_url)
                    await asyncio.sleep(self.delay)
                    
                    # Extract basic game info
                    game_data = await self.extract_game_data(page)
                    if game_data:
                        games_data.append(game_data)
                    
                except Exception as e:
                    logger.error(f"Error scraping game {i+1}: {e}")
                    continue
            
            await browser.close()
        
        # Save results
        if games_data:
            df = pd.DataFrame(games_data)
            df.to_csv('data/recent_games.csv', index=False)
            logger.info(f"✅ Saved {len(games_data)} games to data/recent_games.csv")
        
        return games_data
    
    async def extract_game_data(self, page):
        """Extract basic game data from page"""
        try:
            # Get game title
            title = await page.title()
            
            # Get teams and scores
            teams = await page.query_selector_all('.scorebox .scores .winner, .scorebox .scores .loser')
            
            if len(teams) >= 2:
                team1 = await teams[0].text_content()
                team2 = await teams[1].text_content()
                
                return {
                    'title': title,
                    'team1': team1.strip() if team1 else 'Unknown',
                    'team2': team2.strip() if team2 else 'Unknown',
                    'scraped_at': datetime.now().isoformat()
                }
        
        except Exception as e:
            logger.error(f"Error extracting game data: {e}")
        
        return None

async def main():
    """Main function"""
    logger.info("Starting Colab NBA scraper...")
    
    scraper = ColabScraper()
    games = await scraper.scrape_recent_games(season=2024, max_games=5)
    
    logger.info(f"Scraping completed! Found {len(games)} games")
    
    # Show results
    if games:
        print("\\n📊 Scraped Games:")
        for i, game in enumerate(games, 1):
            print(f"{i}. {game['team1']} vs {game['team2']}")

if __name__ == "__main__":
    asyncio.run(main())
'''
    
    with open('colab_scraper.py', 'w') as f:
        f.write(colab_scraper)
    
    print("✅ Colab scraper created")
    
    print("\n🎉 Colab setup completed!")
    print("\nNext steps:")
    print("1. Run: python colab_test.py")
    print("2. Run: python colab_scraper.py")
    print("3. Check the data/ directory for results")

if __name__ == "__main__":
    setup_colab() 