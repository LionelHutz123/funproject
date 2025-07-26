#!/usr/bin/env python3
"""
Example analysis script demonstrating the new database-driven system
"""

import pandas as pd
from datetime import date
from loguru import logger
from database_manager import DatabaseManager

def analyze_team_performance():
    """Analyze team performance across seasons"""
    logger.info("Analyzing team performance...")
    
    db = DatabaseManager()
    
    # Get standings for multiple seasons
    seasons = [2020, 2021, 2022, 2023]
    all_standings = {}
    
    for season in seasons:
        try:
            standings = db.get_team_standings(season)
            all_standings[season] = standings
            print(f"\n{season} Season Top 5 Teams:")
            print(standings.head())
        except Exception as e:
            logger.warning(f"Could not get standings for {season}: {e}")
    
    # Analyze specific team performance
    team = "LAL"  # Lakers
    print(f"\n{team} Performance Analysis:")
    
    for season in seasons:
        if season in all_standings:
            team_data = all_standings[season][all_standings[season]['team'] == team]
            if not team_data.empty:
                row = team_data.iloc[0]
                print(f"{season}: {row['wins']}-{row['losses']} ({row['win_pct']:.3f})")

def analyze_player_performance():
    """Analyze player performance trends"""
    logger.info("Analyzing player performance...")
    
    db = DatabaseManager()
    
    # Analyze LeBron James performance
    player_name = "LeBron James"
    seasons = [2020, 2021, 2022, 2023]
    
    print(f"\n{player_name} Performance Analysis:")
    print("Season | Games | PPG  | RPG  | APG  | FG%  | 3P%  | FT%")
    print("-" * 55)
    
    for season in seasons:
        try:
            stats = db.get_player_season_stats(player_name, season)
            if stats:
                print(f"{season}   | {stats['games_played']:5d} | {stats['averages']['pts']:5.1f} | {stats['averages']['trb']:5.1f} | {stats['averages']['ast']:5.1f} | {stats['averages']['fg_pct']:5.3f} | {stats['averages']['fg3_pct']:5.3f} | {stats['averages']['ft_pct']:5.3f}")
        except Exception as e:
            logger.warning(f"Could not get stats for {player_name} in {season}: {e}")

def analyze_recent_games():
    """Analyze recent games and trends"""
    logger.info("Analyzing recent games...")
    
    db = DatabaseManager()
    
    # Get recent games
    recent_games = db.get_games_by_date_range(
        date(2023, 12, 1),
        date(2023, 12, 31)
    )
    
    print(f"\nRecent Games Analysis (December 2023):")
    print(f"Total games: {len(recent_games)}")
    
    if recent_games:
        # Calculate average scores
        home_scores = [game.home_score for game in recent_games]
        away_scores = [game.away_score for game in recent_games]
        
        avg_home_score = sum(home_scores) / len(home_scores)
        avg_away_score = sum(away_scores) / len(away_scores)
        
        print(f"Average home team score: {avg_home_score:.1f}")
        print(f"Average away team score: {avg_away_score:.1f}")
        print(f"Home court advantage: {avg_home_score - avg_away_score:.1f} points")
        
        # Find highest scoring game
        highest_scoring = max(recent_games, key=lambda g: g.home_score + g.away_score)
        print(f"Highest scoring game: {highest_scoring.away_team} {highest_scoring.away_score} @ {highest_scoring.home_team} {highest_scoring.home_score}")

def analyze_team_stats():
    """Analyze team statistics"""
    logger.info("Analyzing team statistics...")
    
    db = DatabaseManager()
    
    # Get team stats for a specific team
    team = "LAL"
    season = 2023
    
    try:
        team_stats = db.get_team_stats(team, season)
        
        if not team_stats.empty:
            print(f"\n{team} {season} Season Statistics:")
            print(f"Games played: {len(team_stats)}")
            print(f"Average points per game: {team_stats['pts'].mean():.1f}")
            print(f"Average field goal percentage: {team_stats['fg_pct'].mean():.3f}")
            print(f"Average three-point percentage: {team_stats['fg3_pct'].mean():.3f}")
            print(f"Average rebounds per game: {team_stats['trb'].mean():.1f}")
            print(f"Average assists per game: {team_stats['ast'].mean():.1f}")
            
            # Find best and worst games
            best_game = team_stats.loc[team_stats['pts'].idxmax()]
            worst_game = team_stats.loc[team_stats['pts'].idxmin()]
            
            print(f"\nBest offensive game: {best_game['pts']} points")
            print(f"Worst offensive game: {worst_game['pts']} points")
            
    except Exception as e:
        logger.error(f"Error analyzing team stats: {e}")

def compare_players():
    """Compare multiple players"""
    logger.info("Comparing players...")
    
    db = DatabaseManager()
    
    players = ["LeBron James", "Stephen Curry", "Kevin Durant"]
    season = 2023
    
    print(f"\nPlayer Comparison - {season} Season:")
    print("Player          | Games | PPG  | RPG  | APG  | FG%  | 3P%  | FT%")
    print("-" * 70)
    
    for player in players:
        try:
            stats = db.get_player_season_stats(player, season)
            if stats:
                print(f"{player:<15} | {stats['games_played']:5d} | {stats['averages']['pts']:5.1f} | {stats['averages']['trb']:5.1f} | {stats['averages']['ast']:5.1f} | {stats['averages']['fg_pct']:5.3f} | {stats['averages']['fg3_pct']:5.3f} | {stats['averages']['ft_pct']:5.3f}")
        except Exception as e:
            logger.warning(f"Could not get stats for {player}: {e}")

def main():
    """Run all analysis examples"""
    logger.info("Starting example analysis...")
    
    # Check if database has data
    db = DatabaseManager()
    stats = db.get_database_stats()
    
    if stats['total_games'] == 0:
        logger.error("No data found in database. Please run migration first:")
        logger.error("python migrate.py")
        return
    
    print("=" * 60)
    print("BASKETBALL DATA ANALYSIS EXAMPLES")
    print("=" * 60)
    
    # Run various analyses
    analyze_team_performance()
    analyze_player_performance()
    analyze_recent_games()
    analyze_team_stats()
    compare_players()
    
    print("\n" + "=" * 60)
    print("Analysis complete!")
    print("=" * 60)

if __name__ == "__main__":
    main() 