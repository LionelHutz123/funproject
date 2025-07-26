import os
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from typing import List, Dict, Optional
from datetime import datetime
import re
from pathlib import Path

from config import settings
from models import Game, TeamGameStats, GameOfficial, PlayerGameStats, SessionLocal, create_tables

class DataProcessor:
    def __init__(self):
        self.scores_dir = settings.SCORES_DIR
        
    def process_existing_files(self):
        """Process all existing HTML files and migrate to database"""
        logger.info("Starting to process existing HTML files")
        
        html_files = list(self.scores_dir.glob("*.html"))
        logger.info(f"Found {len(html_files)} HTML files to process")
        
        processed_count = 0
        error_count = 0
        
        for html_file in html_files:
            try:
                game_data = self.parse_html_file(html_file)
                if game_data:
                    self.save_game_to_database(game_data)
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count}/{len(html_files)} files")
                        
            except Exception as e:
                logger.error(f"Error processing {html_file}: {e}")
                error_count += 1
        
        logger.info(f"Processing complete. Processed: {processed_count}, Errors: {error_count}")
    
    def parse_html_file(self, file_path: Path) -> Optional[Dict]:
        """Parse a single HTML file and extract game data"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                html = f.read()
            
            soup = BeautifulSoup(html, 'lxml')
            
            # Remove problematic elements
            for selector in ["tr.over_header", "tr.thead"]:
                for element in soup.select(selector):
                    element.decompose()
            
            # Parse game data
            game_data = self._parse_basic_game_info(soup, file_path)
            if not game_data:
                return None
            
            # Parse team stats
            team_stats = self._parse_team_stats(soup, game_data['game_id'])
            game_data['team_stats'] = team_stats
            
            # Parse player stats
            player_stats = self._parse_player_stats(soup, game_data['game_id'])
            game_data['player_stats'] = player_stats
            
            # Parse officials
            officials = self._parse_officials(soup, game_data['game_id'])
            game_data['officials'] = officials
            
            return game_data
            
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
            return None
    
    def _parse_basic_game_info(self, soup: BeautifulSoup, file_path: Path) -> Optional[Dict]:
        """Parse basic game information from HTML"""
        try:
            # Extract game ID from filename
            game_id = file_path.stem
            
            # Parse date from filename
            date_str = game_id[:8]  # YYYYMMDD format
            game_date = datetime.strptime(date_str, "%Y%m%d")
            
            # Get season
            season = game_date.year
            if game_date.month < 7:  # NBA season spans two calendar years
                season -= 1
            
            # Parse line score
            line_score_table = soup.find('table', {'id': 'line_score'})
            if not line_score_table:
                return None
            
            # Extract team names and scores
            rows = line_score_table.find_all('tr')[1:]  # Skip header
            if len(rows) < 2:
                return None
            
            teams = []
            scores = []
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    team = cells[0].get_text(strip=True)
                    total_score = int(cells[-1].get_text(strip=True))
                    teams.append(team)
                    scores.append(total_score)
            
            if len(teams) != 2 or len(scores) != 2:
                return None
            
            # Determine home/away (usually second team is home)
            away_team, home_team = teams
            away_score, home_score = scores
            home_won = home_score > away_score
            
            return {
                'game_id': game_id,
                'date': game_date,
                'season': season,
                'home_team': home_team,
                'away_team': away_team,
                'home_score': home_score,
                'away_score': away_score,
                'home_won': home_won
            }
            
        except Exception as e:
            logger.error(f"Error parsing basic game info: {e}")
            return None
    
    def _parse_team_stats(self, soup: BeautifulSoup, game_id: str) -> List[Dict]:
        """Parse team statistics"""
        team_stats = []
        
        try:
            # Find team stat tables
            for team in ['home', 'away']:
                basic_table = soup.find('table', {'id': f'box-{team}-game-basic'})
                advanced_table = soup.find('table', {'id': f'box-{team}-game-advanced'})
                
                if not basic_table:
                    continue
                
                # Parse basic stats
                basic_stats = self._parse_stats_table(basic_table, 'basic')
                advanced_stats = self._parse_stats_table(advanced_table, 'advanced') if advanced_table else {}
                
                # Combine stats
                team_stat = {
                    'game_id': game_id,
                    'team': team,
                    'is_home': team == 'home',
                    **basic_stats,
                    **advanced_stats
                }
                
                team_stats.append(team_stat)
                
        except Exception as e:
            logger.error(f"Error parsing team stats: {e}")
        
        return team_stats
    
    def _parse_player_stats(self, soup: BeautifulSoup, game_id: str) -> List[Dict]:
        """Parse individual player statistics"""
        player_stats = []
        
        try:
            for team in ['home', 'away']:
                basic_table = soup.find('table', {'id': f'box-{team}-game-basic'})
                advanced_table = soup.find('table', {'id': f'box-{team}-game-advanced'})
                
                if not basic_table:
                    continue
                
                # Parse player rows (skip totals row)
                rows = basic_table.find_all('tr')[1:-1]  # Skip header and totals
                
                for row in rows:
                    player_stat = self._parse_player_row(row, game_id, team)
                    if player_stat:
                        player_stats.append(player_stat)
                        
        except Exception as e:
            logger.error(f"Error parsing player stats: {e}")
        
        return player_stats
    
    def _parse_player_row(self, row, game_id: str, team: str) -> Optional[Dict]:
        """Parse a single player row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 10:  # Need minimum columns
                return None
            
            player_name = cells[0].get_text(strip=True)
            if not player_name or player_name == 'Reserves' or player_name == 'Team Totals':
                return None
            
            # Extract basic stats
            stats = {
                'game_id': game_id,
                'team': team,
                'player_name': player_name,
                'mp': cells[1].get_text(strip=True) if len(cells) > 1 else '0',
                'fg': self._safe_int(cells[2]) if len(cells) > 2 else 0,
                'fga': self._safe_int(cells[3]) if len(cells) > 3 else 0,
                'fg_pct': self._safe_float(cells[4]) if len(cells) > 4 else 0.0,
                'fg3': self._safe_int(cells[5]) if len(cells) > 5 else 0,
                'fg3a': self._safe_int(cells[6]) if len(cells) > 6 else 0,
                'fg3_pct': self._safe_float(cells[7]) if len(cells) > 7 else 0.0,
                'ft': self._safe_int(cells[8]) if len(cells) > 8 else 0,
                'fta': self._safe_int(cells[9]) if len(cells) > 9 else 0,
                'ft_pct': self._safe_float(cells[10]) if len(cells) > 10 else 0.0,
                'orb': self._safe_int(cells[11]) if len(cells) > 11 else 0,
                'drb': self._safe_int(cells[12]) if len(cells) > 12 else 0,
                'trb': self._safe_int(cells[13]) if len(cells) > 13 else 0,
                'ast': self._safe_int(cells[14]) if len(cells) > 14 else 0,
                'stl': self._safe_int(cells[15]) if len(cells) > 15 else 0,
                'blk': self._safe_int(cells[16]) if len(cells) > 16 else 0,
                'tov': self._safe_int(cells[17]) if len(cells) > 17 else 0,
                'pf': self._safe_int(cells[18]) if len(cells) > 18 else 0,
                'pts': self._safe_int(cells[19]) if len(cells) > 19 else 0,
                'plus_minus': self._safe_int(cells[20]) if len(cells) > 20 else 0,
            }
            
            # Add advanced stats if available
            if advanced_table := soup.find('table', {'id': f'box-{team}-game-advanced'}):
                advanced_stats = self._parse_player_advanced_stats(advanced_table, player_name)
                stats.update(advanced_stats)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error parsing player row: {e}")
            return None
    
    def _parse_player_advanced_stats(self, table, player_name: str) -> Dict:
        """Parse advanced stats for a specific player"""
        stats = {}
        
        try:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) > 0 and cells[0].get_text(strip=True) == player_name:
                    # Advanced stats mapping
                    stat_mapping = {
                        1: 'ts_pct', 2: 'efg_pct', 3: 'fg3a_rate', 4: 'fta_rate',
                        5: 'orb_pct', 6: 'drb_pct', 7: 'trb_pct', 8: 'ast_pct',
                        9: 'stl_pct', 10: 'blk_pct', 11: 'tov_pct', 12: 'usg_pct',
                        13: 'off_rtg', 14: 'def_rtg', 15: 'bpm'
                    }
                    
                    for i, cell in enumerate(cells[1:], 1):
                        if i in stat_mapping:
                            stat_name = stat_mapping[i]
                            value = cell.get_text(strip=True)
                            stats[stat_name] = self._safe_float(value)
                    break
                    
        except Exception as e:
            logger.error(f"Error parsing advanced stats for {player_name}: {e}")
        
        return stats
    
    def _parse_stats_table(self, table, stat_type: str) -> Dict:
        """Parse a statistics table (basic or advanced)"""
        stats = {}
        
        try:
            if not table:
                return stats
            
            # Find totals row (usually last row)
            rows = table.find_all('tr')
            if not rows:
                return stats
            
            totals_row = rows[-1]  # Last row is usually totals
            cells = totals_row.find_all(['td', 'th'])
            
            if stat_type == 'basic':
                # Basic stats mapping
                stat_mapping = {
                    1: 'mp', 2: 'fg', 3: 'fga', 4: 'fg_pct', 5: 'fg3', 6: 'fg3a', 
                    7: 'fg3_pct', 8: 'ft', 9: 'fta', 10: 'ft_pct', 11: 'orb', 
                    12: 'drb', 13: 'trb', 14: 'ast', 15: 'stl', 16: 'blk', 
                    17: 'tov', 18: 'pf', 19: 'pts'
                }
            else:  # advanced
                # Advanced stats mapping
                stat_mapping = {
                    1: 'ts_pct', 2: 'efg_pct', 3: 'fg3a_rate', 4: 'fta_rate',
                    5: 'orb_pct', 6: 'drb_pct', 7: 'trb_pct', 8: 'ast_pct',
                    9: 'stl_pct', 10: 'blk_pct', 11: 'tov_pct', 12: 'usg_pct',
                    13: 'off_rtg', 14: 'def_rtg', 15: 'bpm'
                }
            
            for i, cell in enumerate(cells[1:], 1):  # Skip first column (team name)
                if i in stat_mapping:
                    stat_name = stat_mapping[i]
                    value = cell.get_text(strip=True)
                    
                    if stat_type == 'basic':
                        if 'pct' in stat_name:
                            stats[stat_name] = self._safe_float(value)
                        else:
                            stats[stat_name] = self._safe_int(value)
                    else:  # advanced
                        stats[stat_name] = self._safe_float(value)
            
            # Add max stats for basic stats
            if stat_type == 'basic':
                for i, row in enumerate(rows[1:-1]):  # Skip header and totals
                    cells = row.find_all(['td', 'th'])
                    for j, cell in enumerate(cells[1:], 1):
                        if j in stat_mapping:
                            stat_name = stat_mapping[j] + '_max'
                            value = cell.get_text(strip=True)
                            
                            if 'pct' in stat_mapping[j]:
                                current_max = stats.get(stat_name, 0.0)
                                new_value = self._safe_float(value)
                                stats[stat_name] = max(current_max, new_value)
                            else:
                                current_max = stats.get(stat_name, 0)
                                new_value = self._safe_int(value)
                                stats[stat_name] = max(current_max, new_value)
                                
        except Exception as e:
            logger.error(f"Error parsing {stat_type} stats table: {e}")
        
        return stats
    
    def _parse_officials(self, soup: BeautifulSoup, game_id: str) -> List[Dict]:
        """Parse game officials"""
        officials = []
        
        try:
            # Look for officials section
            officials_div = soup.find('div', string=lambda text: 'Officials:' in str(text) if text else False)
            
            if officials_div:
                official_links = officials_div.find_next_siblings('a')
                for i, official in enumerate(official_links[:3], 1):  # Max 3 officials
                    official_data = {
                        'game_id': game_id,
                        'official_name': official.get_text(strip=True),
                        'official_url': official.get('href', ''),
                        'position': i
                    }
                    officials.append(official_data)
                    
        except Exception as e:
            logger.error(f"Error parsing officials: {e}")
        
        return officials
    
    def _safe_int(self, cell) -> int:
        """Safely convert cell text to integer"""
        try:
            text = cell.get_text(strip=True)
            return int(text) if text and text != '' else 0
        except (ValueError, AttributeError):
            return 0
    
    def _safe_float(self, cell) -> float:
        """Safely convert cell text to float"""
        try:
            text = cell.get_text(strip=True)
            return float(text) if text and text != '' else 0.0
        except (ValueError, AttributeError):
            return 0.0
    
    def save_game_to_database(self, game_data: Dict):
        """Save processed game data to database"""
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
            logger.debug(f"Saved game {game_data['game_id']} to database")
            
        except Exception as e:
            logger.error(f"Error saving game {game_data.get('game_id', 'unknown')}: {e}")
            db.rollback()
        finally:
            db.close()

def main():
    """Main function to process existing HTML files"""
    logger.add("logs/data_processor.log", rotation="1 day", retention="30 days")
    
    # Create database tables
    create_tables()
    
    # Process existing files
    processor = DataProcessor()
    processor.process_existing_files()

if __name__ == "__main__":
    main() 