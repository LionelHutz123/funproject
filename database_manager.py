from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, joinedload
from typing import List, Dict, Optional, Tuple
import pandas as pd
from datetime import datetime, date
from loguru import logger

from config import settings
from models import Game, TeamGameStats, GameOfficial, PlayerGameStats, SessionLocal

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(settings.DATABASE_URL)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self):
        """Get a database session"""
        return self.SessionLocal()
    
    def get_games_by_season(self, season: int) -> List[Game]:
        """Get all games for a specific season"""
        session = self.get_session()
        try:
            games = session.query(Game).filter(Game.season == season).all()
            return games
        finally:
            session.close()
    
    def get_games_by_date_range(self, start_date: date, end_date: date) -> List[Game]:
        """Get games within a date range"""
        session = self.get_session()
        try:
            games = session.query(Game).filter(
                Game.date >= start_date,
                Game.date <= end_date
            ).all()
            return games
        finally:
            session.close()
    
    def get_games_by_team(self, team: str, season: Optional[int] = None) -> List[Game]:
        """Get all games for a specific team"""
        session = self.get_session()
        try:
            query = session.query(Game).filter(
                (Game.home_team == team) | (Game.away_team == team)
            )
            
            if season:
                query = query.filter(Game.season == season)
            
            return query.all()
        finally:
            session.close()
    
    def get_team_stats(self, team: str, season: Optional[int] = None) -> pd.DataFrame:
        """Get team statistics as a DataFrame"""
        session = self.get_session()
        try:
            query = session.query(TeamGameStats).join(Game).filter(TeamGameStats.team == team)
            
            if season:
                query = query.filter(Game.season == season)
            
            # Convert to DataFrame
            results = query.all()
            data = []
            for stat in results:
                data.append({
                    'game_id': stat.game_id,
                    'date': stat.home_game.date if stat.home_game else stat.away_game.date,
                    'season': stat.home_game.season if stat.home_game else stat.away_game.season,
                    'is_home': stat.is_home,
                    'fg': stat.fg,
                    'fga': stat.fga,
                    'fg_pct': stat.fg_pct,
                    'fg3': stat.fg3,
                    'fg3a': stat.fg3a,
                    'fg3_pct': stat.fg3_pct,
                    'ft': stat.ft,
                    'fta': stat.fta,
                    'ft_pct': stat.ft_pct,
                    'orb': stat.orb,
                    'drb': stat.drb,
                    'trb': stat.trb,
                    'ast': stat.ast,
                    'stl': stat.stl,
                    'blk': stat.blk,
                    'tov': stat.tov,
                    'pf': stat.pf,
                    'pts': stat.pts,
                    'ts_pct': stat.ts_pct,
                    'efg_pct': stat.efg_pct,
                    'off_rtg': stat.off_rtg,
                    'def_rtg': stat.def_rtg,
                })
            
            return pd.DataFrame(data)
        finally:
            session.close()
    
    def get_player_stats(self, player_name: str, season: Optional[int] = None) -> pd.DataFrame:
        """Get player statistics as a DataFrame"""
        session = self.get_session()
        try:
            query = session.query(PlayerGameStats).filter(
                PlayerGameStats.player_name.like(f"%{player_name}%")
            )
            
            if season:
                # Join with Game to filter by season
                query = query.join(Game, PlayerGameStats.game_id == Game.game_id).filter(Game.season == season)
            
            results = query.all()
            data = []
            for stat in results:
                data.append({
                    'game_id': stat.game_id,
                    'team': stat.team,
                    'player_name': stat.player_name,
                    'mp': stat.mp,
                    'fg': stat.fg,
                    'fga': stat.fga,
                    'fg_pct': stat.fg_pct,
                    'fg3': stat.fg3,
                    'fg3a': stat.fg3a,
                    'fg3_pct': stat.fg3_pct,
                    'ft': stat.ft,
                    'fta': stat.fta,
                    'ft_pct': stat.ft_pct,
                    'orb': stat.orb,
                    'drb': stat.drb,
                    'trb': stat.trb,
                    'ast': stat.ast,
                    'stl': stat.stl,
                    'blk': stat.blk,
                    'tov': stat.tov,
                    'pf': stat.pf,
                    'pts': stat.pts,
                    'plus_minus': stat.plus_minus,
                    'ts_pct': stat.ts_pct,
                    'efg_pct': stat.efg_pct,
                    'bpm': stat.bpm,
                })
            
            return pd.DataFrame(data)
        finally:
            session.close()
    
    def get_game_details(self, game_id: str) -> Optional[Dict]:
        """Get complete game details including all stats"""
        session = self.get_session()
        try:
            game = session.query(Game).options(
                joinedload(Game.home_stats),
                joinedload(Game.away_stats),
                joinedload(Game.officials)
            ).filter(Game.game_id == game_id).first()
            
            if not game:
                return None
            
            # Get player stats for this game
            player_stats = session.query(PlayerGameStats).filter(
                PlayerGameStats.game_id == game_id
            ).all()
            
            return {
                'game': {
                    'id': game.id,
                    'game_id': game.game_id,
                    'date': game.date,
                    'season': game.season,
                    'home_team': game.home_team,
                    'away_team': game.away_team,
                    'home_score': game.home_score,
                    'away_score': game.away_score,
                    'home_won': game.home_won,
                },
                'team_stats': [
                    {
                        'team': stat.team,
                        'is_home': stat.is_home,
                        'fg': stat.fg,
                        'fga': stat.fga,
                        'fg_pct': stat.fg_pct,
                        'fg3': stat.fg3,
                        'fg3a': stat.fg3a,
                        'fg3_pct': stat.fg3_pct,
                        'ft': stat.ft,
                        'fta': stat.fta,
                        'ft_pct': stat.ft_pct,
                        'orb': stat.orb,
                        'drb': stat.drb,
                        'trb': stat.trb,
                        'ast': stat.ast,
                        'stl': stat.stl,
                        'blk': stat.blk,
                        'tov': stat.tov,
                        'pf': stat.pf,
                        'pts': stat.pts,
                        'ts_pct': stat.ts_pct,
                        'efg_pct': stat.efg_pct,
                        'off_rtg': stat.off_rtg,
                        'def_rtg': stat.def_rtg,
                    }
                    for stat in game.home_stats + game.away_stats
                ],
                'player_stats': [
                    {
                        'team': stat.team,
                        'player_name': stat.player_name,
                        'mp': stat.mp,
                        'fg': stat.fg,
                        'fga': stat.fga,
                        'fg_pct': stat.fg_pct,
                        'fg3': stat.fg3,
                        'fg3a': stat.fg3a,
                        'fg3_pct': stat.fg3_pct,
                        'ft': stat.ft,
                        'fta': stat.fta,
                        'ft_pct': stat.ft_pct,
                        'orb': stat.orb,
                        'drb': stat.drb,
                        'trb': stat.trb,
                        'ast': stat.ast,
                        'stl': stat.stl,
                        'blk': stat.blk,
                        'tov': stat.tov,
                        'pf': stat.pf,
                        'pts': stat.pts,
                        'plus_minus': stat.plus_minus,
                        'ts_pct': stat.ts_pct,
                        'efg_pct': stat.efg_pct,
                        'bpm': stat.bpm,
                    }
                    for stat in player_stats
                ],
                'officials': [
                    {
                        'name': official.official_name,
                        'url': official.official_url,
                        'position': official.position,
                    }
                    for official in game.officials
                ],
            }
        finally:
            session.close()
    
    def get_team_standings(self, season: int) -> pd.DataFrame:
        """Get team standings for a season"""
        session = self.get_session()
        try:
            # Get all games for the season
            games = session.query(Game).filter(Game.season == season).all()
            
            # Calculate standings
            standings = {}
            for game in games:
                # Home team
                if game.home_team not in standings:
                    standings[game.home_team] = {'wins': 0, 'losses': 0, 'points_for': 0, 'points_against': 0}
                
                if game.home_won:
                    standings[game.home_team]['wins'] += 1
                else:
                    standings[game.home_team]['losses'] += 1
                
                standings[game.home_team]['points_for'] += game.home_score
                standings[game.home_team]['points_against'] += game.away_score
                
                # Away team
                if game.away_team not in standings:
                    standings[game.away_team] = {'wins': 0, 'losses': 0, 'points_for': 0, 'points_against': 0}
                
                if not game.home_won:
                    standings[game.away_team]['wins'] += 1
                else:
                    standings[game.away_team]['losses'] += 1
                
                standings[game.away_team]['points_for'] += game.away_score
                standings[game.away_team]['points_against'] += game.home_score
            
            # Convert to DataFrame and sort by wins
            data = []
            for team, stats in standings.items():
                data.append({
                    'team': team,
                    'wins': stats['wins'],
                    'losses': stats['losses'],
                    'win_pct': stats['wins'] / (stats['wins'] + stats['losses']) if (stats['wins'] + stats['losses']) > 0 else 0,
                    'points_for': stats['points_for'],
                    'points_against': stats['points_against'],
                    'point_diff': stats['points_for'] - stats['points_against'],
                })
            
            df = pd.DataFrame(data)
            return df.sort_values('wins', ascending=False)
        finally:
            session.close()
    
    def get_player_season_stats(self, player_name: str, season: int) -> Dict:
        """Get aggregated player stats for a season"""
        df = self.get_player_stats(player_name, season)
        
        if df.empty:
            return {}
        
        # Calculate season totals and averages
        totals = df.sum(numeric_only=True)
        averages = df.mean(numeric_only=True)
        
        return {
            'player_name': player_name,
            'season': season,
            'games_played': len(df),
            'totals': {
                'fg': int(totals['fg']),
                'fga': int(totals['fga']),
                'fg3': int(totals['fg3']),
                'fg3a': int(totals['fg3a']),
                'ft': int(totals['ft']),
                'fta': int(totals['fta']),
                'orb': int(totals['orb']),
                'drb': int(totals['drb']),
                'trb': int(totals['trb']),
                'ast': int(totals['ast']),
                'stl': int(totals['stl']),
                'blk': int(totals['blk']),
                'tov': int(totals['tov']),
                'pf': int(totals['pf']),
                'pts': int(totals['pts']),
            },
            'averages': {
                'fg': round(averages['fg'], 1),
                'fga': round(averages['fga'], 1),
                'fg_pct': round(averages['fg_pct'], 3),
                'fg3': round(averages['fg3'], 1),
                'fg3a': round(averages['fg3a'], 1),
                'fg3_pct': round(averages['fg3_pct'], 3),
                'ft': round(averages['ft'], 1),
                'fta': round(averages['fta'], 1),
                'ft_pct': round(averages['ft_pct'], 3),
                'orb': round(averages['orb'], 1),
                'drb': round(averages['drb'], 1),
                'trb': round(averages['trb'], 1),
                'ast': round(averages['ast'], 1),
                'stl': round(averages['stl'], 1),
                'blk': round(averages['blk'], 1),
                'tov': round(averages['tov'], 1),
                'pf': round(averages['pf'], 1),
                'pts': round(averages['pts'], 1),
                'plus_minus': round(averages['plus_minus'], 1),
                'ts_pct': round(averages['ts_pct'], 3),
                'efg_pct': round(averages['efg_pct'], 3),
                'bpm': round(averages['bpm'], 1),
            }
        }
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        session = self.get_session()
        try:
            game_count = session.query(Game).count()
            team_stats_count = session.query(TeamGameStats).count()
            player_stats_count = session.query(PlayerGameStats).count()
            officials_count = session.query(GameOfficial).count()
            
            # Get season range
            seasons = session.query(Game.season).distinct().all()
            season_list = [s[0] for s in seasons]
            
            return {
                'total_games': game_count,
                'total_team_stats': team_stats_count,
                'total_player_stats': player_stats_count,
                'total_officials': officials_count,
                'seasons': sorted(season_list),
                'date_range': {
                    'earliest': session.query(Game.date).order_by(Game.date.asc()).first()[0].strftime('%Y-%m-%d'),
                    'latest': session.query(Game.date).order_by(Game.date.desc()).first()[0].strftime('%Y-%m-%d'),
                }
            }
        finally:
            session.close()

# Example usage functions
def example_queries():
    """Example of how to use the DatabaseManager"""
    db = DatabaseManager()
    
    # Get database stats
    stats = db.get_database_stats()
    print(f"Database contains {stats['total_games']} games from {stats['seasons']}")
    
    # Get team standings for 2023 season
    standings = db.get_team_standings(2023)
    print("\n2023 Season Standings:")
    print(standings.head())
    
    # Get player stats for LeBron James
    lebron_stats = db.get_player_season_stats("LeBron James", 2023)
    if lebron_stats:
        print(f"\nLeBron James 2023 Season:")
        print(f"Games: {lebron_stats['games_played']}")
        print(f"PPG: {lebron_stats['averages']['pts']}")
        print(f"APG: {lebron_stats['averages']['ast']}")
        print(f"RPG: {lebron_stats['averages']['trb']}")
    
    # Get recent games
    recent_games = db.get_games_by_date_range(
        date(2023, 12, 1),
        date(2023, 12, 31)
    )
    print(f"\nFound {len(recent_games)} games in December 2023")

if __name__ == "__main__":
    example_queries() 