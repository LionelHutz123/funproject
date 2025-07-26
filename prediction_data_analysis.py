#!/usr/bin/env python3
"""
Comprehensive analysis of scraped data for prediction testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from loguru import logger
import json
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

from database_manager import DatabaseManager

class PredictionDataAnalyzer:
    def __init__(self):
        self.db = DatabaseManager()
        self.current_year = datetime.now().year
        
    def analyze_recent_seasons_data(self):
        """Analyze data from recent seasons for prediction testing"""
        logger.info("Analyzing recent seasons data for prediction testing...")
        
        recent_seasons = list(range(self.current_year - 2, self.current_year + 1))
        
        print("\n" + "="*70)
        print("RECENT SEASONS DATA ANALYSIS FOR PREDICTION TESTING")
        print("="*70)
        
        for season in recent_seasons:
            self._analyze_season_data(season)
    
    def _analyze_season_data(self, season: int):
        """Analyze data for a specific season"""
        print(f"\n--- {season} Season Analysis ---")
        
        try:
            # Get games for the season
            games = self.db.get_games_by_season(season)
            print(f"Total games: {len(games)}")
            
            if not games:
                print("No games found for this season")
                return
            
            # Analyze game distribution
            self._analyze_game_distribution(games, season)
            
            # Analyze team performance
            self._analyze_team_performance(season)
            
            # Analyze player performance
            self._analyze_player_performance(season)
            
            # Analyze scoring trends
            self._analyze_scoring_trends(games, season)
            
        except Exception as e:
            logger.error(f"Error analyzing season {season}: {e}")
    
    def _analyze_game_distribution(self, games: list, season: int):
        """Analyze game distribution throughout the season"""
        print(f"\nGame Distribution Analysis ({season}):")
        
        # Convert to DataFrame for easier analysis
        game_data = []
        for game in games:
            game_data.append({
                'date': game.date,
                'home_team': game.home_team,
                'away_team': game.away_team,
                'home_score': game.home_score,
                'away_score': game.away_score,
                'total_score': game.home_score + game.away_score,
                'point_diff': abs(game.home_score - game.away_score),
                'home_won': game.home_won
            })
        
        df = pd.DataFrame(game_data)
        
        # Monthly distribution
        df['month'] = df['date'].dt.month
        monthly_games = df.groupby('month').size()
        print(f"Games per month: {dict(monthly_games)}")
        
        # Home court advantage
        home_wins = df['home_won'].sum()
        total_games = len(df)
        home_win_rate = home_wins / total_games
        print(f"Home court advantage: {home_win_rate:.3f} ({home_wins}/{total_games})")
        
        # Scoring statistics
        print(f"Average total score: {df['total_score'].mean():.1f}")
        print(f"Average point differential: {df['point_diff'].mean():.1f}")
        print(f"Highest scoring game: {df['total_score'].max()}")
        print(f"Lowest scoring game: {df['total_score'].min()}")
    
    def _analyze_team_performance(self, season: int):
        """Analyze team performance for the season"""
        print(f"\nTeam Performance Analysis ({season}):")
        
        try:
            standings = self.db.get_team_standings(season)
            
            if standings.empty:
                print("No standings data available")
                return
            
            # Top 5 teams
            top_5 = standings.head()
            print("Top 5 Teams:")
            for _, team in top_5.iterrows():
                print(f"  {team['team']}: {team['wins']}-{team['losses']} ({team['win_pct']:.3f})")
            
            # Bottom 5 teams
            bottom_5 = standings.tail()
            print("\nBottom 5 Teams:")
            for _, team in bottom_5.iterrows():
                print(f"  {team['team']}: {team['wins']}-{team['losses']} ({team['win_pct']:.3f})")
            
            # Performance statistics
            print(f"\nPerformance Statistics:")
            print(f"  Best team win %: {standings['win_pct'].max():.3f}")
            print(f"  Worst team win %: {standings['win_pct'].min():.3f}")
            print(f"  Average win %: {standings['win_pct'].mean():.3f}")
            print(f"  Standard deviation: {standings['win_pct'].std():.3f}")
            
        except Exception as e:
            logger.error(f"Error analyzing team performance for {season}: {e}")
    
    def _analyze_player_performance(self, season: int):
        """Analyze player performance for the season"""
        print(f"\nPlayer Performance Analysis ({season}):")
        
        # Get top players by points
        try:
            # This would require additional database queries
            # For now, show sample analysis
            print("Top scoring players analysis would be available with enhanced queries")
            
        except Exception as e:
            logger.error(f"Error analyzing player performance for {season}: {e}")
    
    def _analyze_scoring_trends(self, games: list, season: int):
        """Analyze scoring trends throughout the season"""
        print(f"\nScoring Trends Analysis ({season}):")
        
        # Convert to DataFrame
        game_data = []
        for game in games:
            game_data.append({
                'date': game.date,
                'home_score': game.home_score,
                'away_score': game.away_score,
                'total_score': game.home_score + game.away_score
            })
        
        df = pd.DataFrame(game_data)
        df = df.sort_values('date')
        
        # Rolling average of total scores
        df['rolling_avg'] = df['total_score'].rolling(window=10).mean()
        
        print(f"Season scoring trends:")
        print(f"  Early season avg (first 20 games): {df.head(20)['total_score'].mean():.1f}")
        print(f"  Late season avg (last 20 games): {df.tail(20)['total_score'].mean():.1f}")
        print(f"  Overall trend: {'Increasing' if df['rolling_avg'].iloc[-1] > df['rolling_avg'].iloc[0] else 'Decreasing'}")
    
    def analyze_additional_data_sources(self):
        """Analyze additional data sources scraped"""
        logger.info("Analyzing additional data sources...")
        
        print("\n" + "="*70)
        print("ADDITIONAL DATA SOURCES ANALYSIS")
        print("="*70)
        
        # Analyze team rosters
        self._analyze_team_rosters()
        
        # Analyze historical data
        self._analyze_historical_data()
        
        # Analyze player profiles
        self._analyze_player_profiles()
    
    def _analyze_team_rosters(self):
        """Analyze team roster data"""
        print("\n--- Team Rosters Analysis ---")
        
        data_dir = Path("data")
        roster_files = list(data_dir.glob("rosters_*.json"))
        
        if not roster_files:
            print("No roster files found")
            return
        
        for roster_file in roster_files:
            try:
                with open(roster_file, 'r') as f:
                    rosters = json.load(f)
                
                season = roster_file.stem.split('_')[1]
                print(f"\n{season} Season Rosters:")
                print(f"  Teams with rosters: {len(rosters)}")
                
                total_players = 0
                for team, roster in rosters.items():
                    total_players += len(roster)
                    print(f"    {team}: {len(roster)} players")
                
                print(f"  Total players: {total_players}")
                
            except Exception as e:
                logger.error(f"Error analyzing roster file {roster_file}: {e}")
    
    def _analyze_historical_data(self):
        """Analyze historical data files"""
        print("\n--- Historical Data Analysis ---")
        
        data_dir = Path("data")
        historical_files = list(data_dir.glob("*_*.json"))
        
        if not historical_files:
            print("No historical data files found")
            return
        
        for file_path in historical_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                file_type = file_path.stem
                print(f"\n{file_type}:")
                print(f"  Records: {len(data) if isinstance(data, list) else 'N/A'}")
                
                if file_type.startswith('standings'):
                    self._analyze_standings_data(data, file_type)
                elif file_type.startswith('leaders'):
                    self._analyze_leaders_data(data, file_type)
                elif file_type.startswith('team_stats'):
                    self._analyze_team_stats_data(data, file_type)
                
            except Exception as e:
                logger.error(f"Error analyzing historical file {file_path}: {e}")
    
    def _analyze_standings_data(self, data: list, file_type: str):
        """Analyze standings data"""
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"  Teams: {len(df)}")
        print(f"  Win % range: {df['win_pct'].min():.3f} - {df['win_pct'].max():.3f}")
        print(f"  Average win %: {df['win_pct'].mean():.3f}")
    
    def _analyze_leaders_data(self, data: dict, file_type: str):
        """Analyze leaders data"""
        for category, leaders in data.items():
            if isinstance(leaders, list) and leaders:
                print(f"  {category}: {len(leaders)} leaders")
                if leaders:
                    top_leader = leaders[0]
                    print(f"    Top: {top_leader.get('player', 'Unknown')} ({top_leader.get('value', 0)})")
    
    def _analyze_team_stats_data(self, data: list, file_type: str):
        """Analyze team stats data"""
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"  Teams: {len(df)}")
        print(f"  Offensive rating range: {df['off_rtg'].min():.1f} - {df['off_rtg'].max():.1f}")
        print(f"  Defensive rating range: {df['def_rtg'].min():.1f} - {df['def_rtg'].max():.1f}")
    
    def _analyze_player_profiles(self):
        """Analyze player profile data"""
        print("\n--- Player Profiles Analysis ---")
        
        profile_file = Path("data/player_profiles.json")
        if not profile_file.exists():
            print("No player profiles file found")
            return
        
        try:
            with open(profile_file, 'r') as f:
                profiles = json.load(f)
            
            print(f"Player profiles: {len(profiles)}")
            
            # Analyze player characteristics
            heights = [p.get('height', '') for p in profiles if p.get('height')]
            weights = [p.get('weight', '') for p in profiles if p.get('weight')]
            colleges = [p.get('college', '') for p in profiles if p.get('college')]
            
            print(f"  Players with height data: {len(heights)}")
            print(f"  Players with weight data: {len(weights)}")
            print(f"  Players with college data: {len(colleges)}")
            
            # Most common colleges
            college_counts = pd.Series(colleges).value_counts()
            if not college_counts.empty:
                print(f"  Top colleges: {college_counts.head(3).to_dict()}")
            
        except Exception as e:
            logger.error(f"Error analyzing player profiles: {e}")
    
    def generate_prediction_features(self):
        """Generate features that could be used for predictions"""
        logger.info("Generating prediction features...")
        
        print("\n" + "="*70)
        print("PREDICTION FEATURES GENERATION")
        print("="*70)
        
        # Get recent seasons data
        recent_seasons = list(range(self.current_year - 2, self.current_year + 1))
        
        features = []
        for season in recent_seasons:
            try:
                season_features = self._generate_season_features(season)
                features.extend(season_features)
            except Exception as e:
                logger.error(f"Error generating features for season {season}: {e}")
        
        # Save features for prediction models
        self._save_prediction_features(features)
        
        print(f"Generated {len(features)} prediction features")
    
    def _generate_season_features(self, season: int) -> list:
        """Generate features for a specific season"""
        features = []
        
        try:
            # Get team standings
            standings = self.db.get_team_standings(season)
            
            for _, team in standings.iterrows():
                feature = {
                    'season': season,
                    'team': team['team'],
                    'wins': team['wins'],
                    'losses': team['losses'],
                    'win_pct': team['win_pct'],
                    'points_for': team['points_for'],
                    'points_against': team['points_against'],
                    'point_diff': team['point_diff'],
                    'srs': team.get('srs', 0),
                    'off_rtg': team.get('off_rtg', 0),
                    'def_rtg': team.get('def_rtg', 0),
                    'pace': team.get('pace', 0)
                }
                features.append(feature)
            
        except Exception as e:
            logger.error(f"Error generating features for season {season}: {e}")
        
        return features
    
    def _save_prediction_features(self, features: list):
        """Save prediction features to file"""
        try:
            features_df = pd.DataFrame(features)
            
            # Create features directory
            Path("features").mkdir(exist_ok=True)
            
            # Save to CSV
            features_file = "features/prediction_features.csv"
            features_df.to_csv(features_file, index=False)
            
            # Save summary statistics
            summary_file = "features/features_summary.json"
            summary = {
                'total_features': len(features),
                'seasons': features_df['season'].unique().tolist(),
                'teams': features_df['team'].nunique(),
                'columns': features_df.columns.tolist(),
                'summary_stats': features_df.describe().to_dict()
            }
            
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"Features saved to {features_file}")
            print(f"Summary saved to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving prediction features: {e}")
    
    def create_testing_dataset(self):
        """Create a testing dataset for prediction validation"""
        logger.info("Creating testing dataset for predictions...")
        
        print("\n" + "="*70)
        print("TESTING DATASET CREATION")
        print("="*70)
        
        # Get most recent season data
        current_season = self.current_year
        recent_games = self.db.get_games_by_date_range(
            date(current_season, 1, 1),
            date(current_season, 12, 31)
        )
        
        if not recent_games:
            print("No recent games found for testing dataset")
            return
        
        # Create testing dataset
        testing_data = []
        for game in recent_games:
            game_data = {
                'game_id': game.game_id,
                'date': game.date,
                'home_team': game.home_team,
                'away_team': game.away_team,
                'home_score': game.home_score,
                'away_score': game.away_score,
                'total_score': game.home_score + game.away_score,
                'point_diff': game.home_score - game.away_score,
                'home_won': game.home_won
            }
            testing_data.append(game_data)
        
        # Save testing dataset
        testing_df = pd.DataFrame(testing_data)
        testing_file = "features/testing_dataset.csv"
        testing_df.to_csv(testing_file, index=False)
        
        print(f"Testing dataset created with {len(testing_data)} games")
        print(f"Dataset saved to {testing_file}")
        
        # Show dataset statistics
        print(f"\nTesting Dataset Statistics:")
        print(f"  Date range: {testing_df['date'].min()} to {testing_df['date'].max()}")
        print(f"  Average total score: {testing_df['total_score'].mean():.1f}")
        print(f"  Home win rate: {testing_df['home_won'].mean():.3f}")
        print(f"  Average point differential: {testing_df['point_diff'].abs().mean():.1f}")

def main():
    """Main function to run comprehensive analysis"""
    logger.info("Starting comprehensive prediction data analysis...")
    
    analyzer = PredictionDataAnalyzer()
    
    # Analyze recent seasons data
    analyzer.analyze_recent_seasons_data()
    
    # Analyze additional data sources
    analyzer.analyze_additional_data_sources()
    
    # Generate prediction features
    analyzer.generate_prediction_features()
    
    # Create testing dataset
    analyzer.create_testing_dataset()
    
    print("\n" + "="*70)
    print("COMPREHENSIVE ANALYSIS COMPLETE")
    print("="*70)
    print("The system is now ready for prediction testing with:")
    print("  - Recent seasons data for training")
    print("  - Additional data sources for features")
    print("  - Generated prediction features")
    print("  - Testing dataset for validation")

if __name__ == "__main__":
    main() 