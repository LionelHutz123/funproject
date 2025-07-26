#!/usr/bin/env python3
"""
Basketball Data Management System
Advanced scraper and database system for NBA data
"""

import argparse
import asyncio
from pathlib import Path
from loguru import logger
import sys

from config import settings
from models import create_tables
from scraper import BasketballScraper
from data_processor import DataProcessor
from database_manager import DatabaseManager

def setup_logging():
    """Setup logging configuration"""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        "logs/main.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG"
    )

def process_existing_files():
    """Process existing HTML files and migrate to database"""
    logger.info("Starting to process existing HTML files...")
    
    processor = DataProcessor()
    processor.process_existing_files()
    
    logger.info("Finished processing existing files")

async def scrape_new_data():
    """Scrape new data from basketball-reference.com"""
    logger.info("Starting to scrape new data...")
    
    async with BasketballScraper() as scraper:
        for season in range(settings.START_SEASON, settings.END_SEASON + 1):
            logger.info(f"Scraping season {season}")
            
            # Get all game URLs for the season
            game_urls = await scraper.scrape_season_schedule(season)
            
            # Scrape each game
            for url in game_urls:
                game_data = await scraper.scrape_game_data(url)
                if game_data:
                    # Save to database
                    await save_game_to_database(game_data)
                
                # Small delay between games
                await asyncio.sleep(0.5)

async def save_game_to_database(game_data):
    """Save scraped game data to database"""
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
        logger.info(f"Saved game {game_data['game_id']} to database")
        
    except Exception as e:
        logger.error(f"Error saving game {game_data.get('game_id', 'unknown')}: {e}")
        db.rollback()
    finally:
        db.close()

def show_database_stats():
    """Show database statistics"""
    logger.info("Getting database statistics...")
    
    db = DatabaseManager()
    stats = db.get_database_stats()
    
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    print(f"Total Games: {stats['total_games']}")
    print(f"Total Team Stats Records: {stats['total_team_stats']}")
    print(f"Total Player Stats Records: {stats['total_player_stats']}")
    print(f"Total Officials Records: {stats['total_officials']}")
    print(f"Seasons Available: {stats['seasons']}")
    print(f"Date Range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
    print("="*50)

def show_team_standings(season: int):
    """Show team standings for a specific season"""
    logger.info(f"Getting team standings for season {season}...")
    
    db = DatabaseManager()
    standings = db.get_team_standings(season)
    
    print(f"\n{season} Season Standings")
    print("="*60)
    print(f"{'Team':<15} {'W':<3} {'L':<3} {'PCT':<6} {'PF':<6} {'PA':<6} {'DIFF':<6}")
    print("-"*60)
    
    for _, row in standings.iterrows():
        print(f"{row['team']:<15} {row['wins']:<3} {row['losses']:<3} {row['win_pct']:<6.3f} {row['points_for']:<6} {row['points_against']:<6} {row['point_diff']:<6}")

def show_player_stats(player_name: str, season: int):
    """Show player statistics for a specific season"""
    logger.info(f"Getting player stats for {player_name} in season {season}...")
    
    db = DatabaseManager()
    stats = db.get_player_season_stats(player_name, season)
    
    if not stats:
        print(f"No data found for {player_name} in season {season}")
        return
    
    print(f"\n{player_name} - {season} Season Statistics")
    print("="*50)
    print(f"Games Played: {stats['games_played']}")
    print("\nAverages:")
    print(f"  Points: {stats['averages']['pts']}")
    print(f"  Rebounds: {stats['averages']['trb']}")
    print(f"  Assists: {stats['averages']['ast']}")
    print(f"  Steals: {stats['averages']['stl']}")
    print(f"  Blocks: {stats['averages']['blk']}")
    print(f"  FG%: {stats['averages']['fg_pct']:.3f}")
    print(f"  3P%: {stats['averages']['fg3_pct']:.3f}")
    print(f"  FT%: {stats['averages']['ft_pct']:.3f}")

def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(description="Basketball Data Management System")
    parser.add_argument("command", choices=[
        "setup", "process", "scrape", "scrape-recent", "scrape-all", "update-data", "stats", "standings", "player"
    ], help="Command to execute")
    
    parser.add_argument("--season", type=int, help="Season for standings or player stats")
    parser.add_argument("--player", type=str, help="Player name for stats")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Create database tables
    create_tables()
    
    if args.command == "setup":
        logger.info("Setting up database tables...")
        create_tables()
        logger.info("Database setup complete!")
        
    elif args.command == "process":
        process_existing_files()
        
    elif args.command == "scrape":
        if args.headless:
            settings.HEADLESS = True
        asyncio.run(scrape_new_data())
        
    elif args.command == "scrape-recent":
        """Scrape recent seasons for testing predictions"""
        if args.headless:
            settings.HEADLESS = True
        from scrape_recent_data import scrape_recent_seasons_for_testing
        asyncio.run(scrape_recent_seasons_for_testing())
        
    elif args.command == "scrape-all":
        """Scrape comprehensive data including additional sources"""
        if args.headless:
            settings.HEADLESS = True
        from scrape_recent_data import main as scrape_comprehensive
        asyncio.run(scrape_comprehensive())
        
    elif args.command == "update-data":
        """Update existing data for the last 5 seasons"""
        if args.headless:
            settings.HEADLESS = True
        from scrape_recent_data import update_existing_data
        asyncio.run(update_existing_data())
        
    elif args.command == "stats":
        show_database_stats()
        
    elif args.command == "standings":
        if not args.season:
            print("Please specify a season with --season")
            return
        show_team_standings(args.season)
        
    elif args.command == "player":
        if not args.player or not args.season:
            print("Please specify both --player and --season")
            return
        show_player_stats(args.player, args.season)

if __name__ == "__main__":
    main() 