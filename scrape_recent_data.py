#!/usr/bin/env python3
"""
Script to scrape recent NBA data for testing predictions and updating existing data
"""

import asyncio
from datetime import datetime, date
from loguru import logger
import sys
from pathlib import Path
from typing import Dict, List

from enhanced_scraper import EnhancedBasketballScraper
from database_manager import DatabaseManager
from models import create_tables

def setup_logging():
    """Setup logging for the scraping process"""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        "logs/recent_data_scraping.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG"
    )

async def scrape_recent_seasons_for_testing():
    """Scrape recent seasons specifically for testing predictions"""
    logger.info("Starting to scrape recent seasons for testing predictions...")
    
    # Get current year and define recent seasons
    current_year = datetime.now().year
    recent_seasons = list(range(current_year - 2, current_year + 1))  # Last 3 seasons
    
    logger.info(f"Scraping seasons for testing: {recent_seasons}")
    
    async with EnhancedBasketballScraper() as scraper:
        # Scrape game data for recent seasons
        all_game_urls = await scraper.scrape_recent_seasons(recent_seasons)
        
        total_games = 0
        for season, game_urls in all_game_urls.items():
            logger.info(f"Processing {len(game_urls)} games for season {season}")
            
            for url in game_urls:
                game_data = await scraper.scrape_comprehensive_game_data(url)
                if game_data:
                    await save_comprehensive_game_data(game_data)
                    total_games += 1
                
                if total_games % 25 == 0:
                    logger.info(f"Processed {total_games} games so far")
        
        logger.info(f"Completed scraping {total_games} games for testing data")

async def scrape_additional_data_sources():
    """Scrape additional data sources for comprehensive analysis"""
    logger.info("Scraping additional data sources...")
    
    current_year = datetime.now().year
    recent_seasons = list(range(current_year - 5, current_year + 1))
    
    async with EnhancedBasketballScraper() as scraper:
        # Scrape team rosters
        logger.info("Scraping team rosters...")
        for season in recent_seasons:
            rosters = await scraper.scrape_team_rosters(season)
            await save_team_rosters(rosters, season)
        
        # Scrape historical data
        logger.info("Scraping historical data...")
        historical_data = await scraper.scrape_historical_data(recent_seasons)
        await save_historical_data(historical_data)
        
        # Scrape player profiles (sample of top players)
        logger.info("Scraping player profiles...")
        top_players = await get_top_players_for_profiles()
        player_profiles = await scraper.scrape_player_profiles(top_players)
        await save_player_profiles(player_profiles)

async def get_top_players_for_profiles():
    """Get URLs for top players to scrape profiles"""
    # This would typically come from the database
    # For now, return a sample of top player URLs
    base_url = "https://www.basketball-reference.com"
    top_players = [
        f"{base_url}/players/j/jamesle01.html",  # LeBron James
        f"{base_url}/players/c/curryst01.html",  # Stephen Curry
        f"{base_url}/players/d/duranke01.html",  # Kevin Durant
        f"{base_url}/players/a/antetgi01.html",  # Giannis Antetokounmpo
        f"{base_url}/players/j/jokicni01.html",  # Nikola Jokic
    ]
    return top_players

async def update_existing_data():
    """Update existing data for the last 5 seasons"""
    logger.info("Updating existing data for the last 5 seasons...")
    
    current_year = datetime.now().year
    seasons_to_update = list(range(current_year - 5, current_year))
    
    async with EnhancedBasketballScraper() as scraper:
        for season in seasons_to_update:
            logger.info(f"Updating data for season {season}")
            
            # Get game URLs for the season
            game_urls = await scraper.scrape_season_schedule(season)
            
            updated_count = 0
            for url in game_urls:
                game_data = await scraper.scrape_comprehensive_game_data(url)
                if game_data:
                    # Check if we need to update (compare with existing data)
                    if await should_update_game(game_data):
                        await update_game_data(game_data)
                        updated_count += 1
                
                if updated_count % 10 == 0:
                    logger.info(f"Updated {updated_count} games for season {season}")
            
            logger.info(f"Updated {updated_count} games for season {season}")

async def should_update_game(game_data: dict) -> bool:
    """Check if a game should be updated based on existing data"""
    db = DatabaseManager()
    
    # Check if game exists and compare data
    existing_game = db.get_game_details(game_data['game_id'])
    
    if not existing_game:
        return True  # New game, should add
    
    # Compare key metrics to see if update is needed
    # This is a simplified check - in practice you'd compare more fields
    existing_score = existing_game['game']['home_score'] + existing_game['game']['away_score']
    new_score = game_data['home_score'] + game_data['away_score']
    
    return existing_score != new_score

async def update_game_data(game_data: dict):
    """Update existing game data in the database"""
    from models import Game, TeamGameStats, PlayerGameStats, GameOfficial, SessionLocal
    
    db = SessionLocal()
    try:
        # Update game record
        game = db.query(Game).filter(Game.game_id == game_data['game_id']).first()
        if game:
            # Update game fields
            game.home_score = game_data['home_score']
            game.away_score = game_data['away_score']
            game.home_won = game_data['home_won']
            
            # Update team stats
            for team_stat in game_data.get('team_stats', []):
                existing_stat = db.query(TeamGameStats).filter(
                    TeamGameStats.game_id == game_data['game_id'],
                    TeamGameStats.team == team_stat['team']
                ).first()
                
                if existing_stat:
                    # Update existing stat
                    for key, value in team_stat.items():
                        if hasattr(existing_stat, key):
                            setattr(existing_stat, key, value)
                else:
                    # Add new stat
                    new_stat = TeamGameStats(**team_stat)
                    db.add(new_stat)
            
            # Update player stats (similar logic)
            for player_stat in game_data.get('player_stats', []):
                existing_player = db.query(PlayerGameStats).filter(
                    PlayerGameStats.game_id == game_data['game_id'],
                    PlayerGameStats.player_name == player_stat['player_name'],
                    PlayerGameStats.team == player_stat['team']
                ).first()
                
                if existing_player:
                    # Update existing player stat
                    for key, value in player_stat.items():
                        if hasattr(existing_player, key):
                            setattr(existing_player, key, value)
                else:
                    # Add new player stat
                    new_player_stat = PlayerGameStats(**player_stat)
                    db.add(new_player_stat)
            
            db.commit()
            logger.info(f"Updated game {game_data['game_id']}")
        
    except Exception as e:
        logger.error(f"Error updating game {game_data.get('game_id', 'unknown')}: {e}")
        db.rollback()
    finally:
        db.close()

async def save_comprehensive_game_data(game_data: Dict):
    """Save comprehensive game data to database"""
    from models import Game, TeamGameStats, GameOfficial, PlayerGameStats, SessionLocal
    
    db = SessionLocal()
    try:
        # Check if game already exists
        existing_game = db.query(Game).filter(Game.game_id == game_data['game_id']).first()
        if existing_game:
            logger.debug(f"Game {game_data['game_id']} already exists, skipping")
            return
        
        # Create game record
        game = Game(**game_data)
        db.add(game)
        db.commit()
        
        # Add team stats
        for team_stat in game_data.get('team_stats', []):
            team_game_stat = TeamGameStats(**team_stat)
            db.add(team_game_stat)
        
        # Add player stats
        for player_stat in game_data.get('player_stats', []):
            player_game_stat = PlayerGameStats(**player_stat)
            db.add(player_game_stat)
        
        # Add officials
        for official in game_data.get('officials', []):
            game_official = GameOfficial(**official)
            db.add(game_official)
        
        db.commit()
        logger.info(f"Saved comprehensive game {game_data['game_id']} to database")
        
    except Exception as e:
        logger.error(f"Error saving game {game_data.get('game_id', 'unknown')}: {e}")
        db.rollback()
    finally:
        db.close()

async def save_team_rosters(rosters: Dict, season: int):
    """Save team rosters to files"""
    import json
    
    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)
    
    roster_file = f"data/rosters_{season}.json"
    with open(roster_file, 'w') as f:
        json.dump(rosters, f, indent=2)
    
    logger.info(f"Saved rosters for {len(rosters)} teams to {roster_file}")

async def save_historical_data(historical_data: Dict):
    """Save historical data to files"""
    import json
    
    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)
    
    for key, data in historical_data.items():
        filename = f"data/{key}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    logger.info(f"Saved {len(historical_data)} historical data files")

async def save_player_profiles(player_profiles: List[Dict]):
    """Save player profiles to files"""
    import json
    
    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)
    
    profiles_file = "data/player_profiles.json"
    with open(profiles_file, 'w') as f:
        json.dump(player_profiles, f, indent=2)
    
    logger.info(f"Saved {len(player_profiles)} player profiles to {profiles_file}")

def analyze_scraped_data():
    """Analyze the scraped data to verify quality and completeness"""
    logger.info("Analyzing scraped data...")
    
    db = DatabaseManager()
    stats = db.get_database_stats()
    
    print("\n" + "="*60)
    print("SCRAPED DATA ANALYSIS")
    print("="*60)
    print(f"Total Games: {stats['total_games']}")
    print(f"Total Team Stats Records: {stats['total_team_stats']}")
    print(f"Total Player Stats Records: {stats['total_player_stats']}")
    print(f"Seasons Available: {stats['seasons']}")
    print(f"Date Range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
    
    # Analyze recent seasons specifically
    current_year = datetime.now().year
    recent_seasons = list(range(current_year - 2, current_year + 1))
    
    print(f"\nRecent Seasons Analysis:")
    for season in recent_seasons:
        try:
            games = db.get_games_by_season(season)
            print(f"  {season}: {len(games)} games")
        except Exception as e:
            print(f"  {season}: Error - {e}")
    
    print("="*60)

async def main():
    """Main function to orchestrate the scraping process"""
    setup_logging()
    
    # Create database tables
    create_tables()
    
    logger.info("Starting comprehensive NBA data scraping for testing predictions")
    
    try:
        # Step 1: Scrape recent seasons for testing
        logger.info("Step 1: Scraping recent seasons for testing predictions...")
        await scrape_recent_seasons_for_testing()
        
        # Step 2: Scrape additional data sources
        logger.info("Step 2: Scraping additional data sources...")
        await scrape_additional_data_sources()
        
        # Step 3: Update existing data for last 5 seasons
        logger.info("Step 3: Updating existing data for last 5 seasons...")
        await update_existing_data()
        
        # Step 4: Analyze the scraped data
        logger.info("Step 4: Analyzing scraped data...")
        analyze_scraped_data()
        
        logger.info("Comprehensive data scraping and updating completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during scraping process: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 