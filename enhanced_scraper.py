import asyncio
import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from loguru import logger
from typing import List, Dict, Optional, Tuple, Set
import time
from datetime import datetime, date
from pathlib import Path
import re
from asyncio_throttle import Throttler
import json

from config import settings
from models import Game, TeamGameStats, GameOfficial, PlayerGameStats, SessionLocal

class EnhancedBasketballScraper:
    def __init__(self):
        self.throttler = Throttler(rate_limit=1, period=1)  # 1 request per second
        self.session = None
        self.browser = None
        self.page = None
        self.scraped_games = set()  # Track already scraped games
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            headers={'User-Agent': settings.USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=settings.TIMEOUT)
        )
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.firefox.launch(headless=settings.HEADLESS)
        self.page = await self.browser.new_page()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def get_html_with_retry(self, url: str, selector: str = "body", max_retries: int = None) -> Optional[str]:
        """Get HTML content with retry logic and rate limiting"""
        max_retries = max_retries or settings.MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                async with self.throttler:
                    await self.page.goto(url, wait_until="networkidle")
                    await asyncio.sleep(settings.REQUEST_DELAY)
                    
                    if selector == "body":
                        html = await self.page.content()
                    else:
                        element = await self.page.wait_for_selector(selector, timeout=10000)
                        html = await element.inner_html()
                    
                    return html
                    
            except PlaywrightTimeout:
                logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to load {url} after {max_retries} attempts")
                    return None
                await asyncio.sleep(settings.REQUEST_DELAY * (attempt + 1))
                
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(settings.REQUEST_DELAY * (attempt + 1))
        
        return None
    
    async def scrape_recent_seasons(self, seasons: List[int]) -> Dict[str, List[str]]:
        """Scrape multiple recent seasons for comprehensive data collection"""
        all_game_urls = {}
        
        for season in seasons:
            logger.info(f"Scraping season {season}")
            game_urls = await self.scrape_season_schedule(season)
            all_game_urls[str(season)] = game_urls
            logger.info(f"Found {len(game_urls)} games for season {season}")
        
        return all_game_urls
    
    async def scrape_season_schedule(self, season: int) -> List[str]:
        """Scrape all game URLs for a given season with enhanced error handling"""
        url = f"{settings.BASE_URL}/leagues/NBA_{season}_games.html"
        logger.info(f"Scraping schedule for season {season}")
        
        html = await self.get_html_with_retry(url, "#content .filter")
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all("a")
        schedule_pages = [f"{settings.BASE_URL}{l['href']}" for l in links if l.get('href')]
        
        game_urls = []
        for schedule_url in schedule_pages:
            html = await self.get_html_with_retry(schedule_url, "#all_schedule")
            if not html:
                continue
                
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all("a")
            box_scores = [
                f"{settings.BASE_URL}{l['href']}" 
                for l in links 
                if l.get('href') and "boxscore" in l['href'] and '.html' in l['href']
            ]
            game_urls.extend(box_scores)
        
        return game_urls
    
    async def scrape_comprehensive_game_data(self, url: str) -> Optional[Dict]:
        """Scrape comprehensive game data including additional sources"""
        game_id = self.parse_game_id(url)
        
        # Check if already scraped
        if game_id in self.scraped_games:
            logger.debug(f"Game {game_id} already scraped, skipping")
            return None
        
        logger.info(f"Scraping comprehensive data for game {game_id}")
        
        html = await self.get_html_with_retry(url, "#content")
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Parse basic game info
            game_data = self._parse_basic_game_info(soup, game_id)
            if not game_data:
                return None
            
            # Parse team stats
            team_stats = self._parse_team_stats(soup, game_id)
            game_data['team_stats'] = team_stats
            
            # Parse player stats
            player_stats = self._parse_player_stats(soup, game_id)
            game_data['player_stats'] = player_stats
            
            # Parse officials
            officials = self._parse_officials(soup, game_id)
            game_data['officials'] = officials
            
            # Parse additional data sources
            additional_data = await self._scrape_additional_data(soup, game_id)
            game_data.update(additional_data)
            
            # Mark as scraped
            self.scraped_games.add(game_id)
            
            return game_data
            
        except Exception as e:
            logger.error(f"Error parsing game {game_id}: {e}")
            return None
    
    async def _scrape_additional_data(self, soup: BeautifulSoup, game_id: str) -> Dict:
        """Scrape additional data sources from the game page"""
        additional_data = {}
        
        try:
            # Parse game notes and context
            game_notes = self._parse_game_notes(soup)
            additional_data['game_notes'] = game_notes
            
            # Parse attendance and venue info
            venue_info = self._parse_venue_info(soup)
            additional_data['venue_info'] = venue_info
            
            # Parse game duration and timing
            timing_info = self._parse_timing_info(soup)
            additional_data['timing_info'] = timing_info
            
            # Parse advanced team metrics
            advanced_metrics = self._parse_advanced_metrics(soup)
            additional_data['advanced_metrics'] = advanced_metrics
            
        except Exception as e:
            logger.error(f"Error scraping additional data for {game_id}: {e}")
        
        return additional_data
    
    def _parse_game_notes(self, soup: BeautifulSoup) -> Dict:
        """Parse game notes and context information"""
        notes = {}
        
        try:
            # Look for game notes section
            notes_div = soup.find('div', string=lambda text: 'Game Notes:' in str(text) if text else False)
            if notes_div:
                notes_text = notes_div.get_text(strip=True)
                notes['game_notes'] = notes_text
            
            # Look for injuries and roster changes
            injury_info = soup.find('div', string=lambda text: 'Injuries:' in str(text) if text else False)
            if injury_info:
                notes['injuries'] = injury_info.get_text(strip=True)
                
        except Exception as e:
            logger.error(f"Error parsing game notes: {e}")
        
        return notes
    
    def _parse_venue_info(self, soup: BeautifulSoup) -> Dict:
        """Parse venue and attendance information"""
        venue_info = {}
        
        try:
            # Look for venue information
            venue_div = soup.find('div', string=lambda text: 'Venue:' in str(text) if text else False)
            if venue_div:
                venue_text = venue_div.get_text(strip=True)
                venue_info['venue'] = venue_text
            
            # Look for attendance
            attendance_div = soup.find('div', string=lambda text: 'Attendance:' in str(text) if text else False)
            if attendance_div:
                attendance_text = attendance_div.get_text(strip=True)
                # Extract attendance number
                attendance_match = re.search(r'(\d{1,3}(?:,\d{3})*)', attendance_text)
                if attendance_match:
                    venue_info['attendance'] = int(attendance_match.group(1).replace(',', ''))
                    
        except Exception as e:
            logger.error(f"Error parsing venue info: {e}")
        
        return venue_info
    
    def _parse_timing_info(self, soup: BeautifulSoup) -> Dict:
        """Parse game timing and duration information"""
        timing_info = {}
        
        try:
            # Look for game duration
            duration_div = soup.find('div', string=lambda text: 'Duration:' in str(text) if text else False)
            if duration_div:
                duration_text = duration_div.get_text(strip=True)
                timing_info['duration'] = duration_text
            
            # Look for start time
            time_div = soup.find('div', string=lambda text: 'Start Time:' in str(text) if text else False)
            if time_div:
                time_text = time_div.get_text(strip=True)
                timing_info['start_time'] = time_text
                
        except Exception as e:
            logger.error(f"Error parsing timing info: {e}")
        
        return timing_info
    
    def _parse_advanced_metrics(self, soup: BeautifulSoup) -> Dict:
        """Parse advanced team and player metrics"""
        advanced_metrics = {}
        
        try:
            # Look for pace and efficiency metrics
            pace_div = soup.find('div', string=lambda text: 'Pace:' in str(text) if text else False)
            if pace_div:
                pace_text = pace_div.get_text(strip=True)
                pace_match = re.search(r'(\d+\.?\d*)', pace_text)
                if pace_match:
                    advanced_metrics['pace'] = float(pace_match.group(1))
            
            # Look for efficiency ratings
            efficiency_div = soup.find('div', string=lambda text: 'Efficiency:' in str(text) if text else False)
            if efficiency_div:
                efficiency_text = efficiency_div.get_text(strip=True)
                advanced_metrics['efficiency'] = efficiency_text
                
        except Exception as e:
            logger.error(f"Error parsing advanced metrics: {e}")
        
        return advanced_metrics
    
    async def scrape_player_profiles(self, player_urls: List[str]) -> List[Dict]:
        """Scrape detailed player profile information"""
        player_profiles = []
        
        for url in player_urls:
            try:
                html = await self.get_html_with_retry(url)
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                profile = self._parse_player_profile(soup, url)
                if profile:
                    player_profiles.append(profile)
                    
            except Exception as e:
                logger.error(f"Error scraping player profile {url}: {e}")
        
        return player_profiles
    
    def _parse_player_profile(self, soup: BeautifulSoup, url: str) -> Optional[Dict]:
        """Parse detailed player profile information"""
        try:
            # Extract player name
            name_elem = soup.find('h1', {'itemprop': 'name'})
            player_name = name_elem.get_text(strip=True) if name_elem else "Unknown"
            
            # Extract basic info
            info_table = soup.find('table', {'id': 'info'})
            if not info_table:
                return None
            
            profile = {
                'name': player_name,
                'url': url,
                'height': None,
                'weight': None,
                'birth_date': None,
                'college': None,
                'draft_info': None,
                'experience': None
            }
            
            rows = info_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    if 'height' in label:
                        profile['height'] = value
                    elif 'weight' in label:
                        profile['weight'] = value
                    elif 'birth' in label:
                        profile['birth_date'] = value
                    elif 'college' in label:
                        profile['college'] = value
                    elif 'draft' in label:
                        profile['draft_info'] = value
                    elif 'experience' in label:
                        profile['experience'] = value
            
            return profile
            
        except Exception as e:
            logger.error(f"Error parsing player profile: {e}")
            return None
    
    async def scrape_team_rosters(self, season: int) -> Dict[str, List[Dict]]:
        """Scrape team rosters for a given season"""
        rosters = {}
        
        # NBA team abbreviations
        teams = [
            'ATL', 'BOS', 'BRK', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW',
            'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
            'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
        ]
        
        for team in teams:
            try:
                url = f"{settings.BASE_URL}/teams/{team}/{season}.html"
                html = await self.get_html_with_retry(url)
                
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    roster = self._parse_team_roster(soup, team, season)
                    if roster:
                        rosters[team] = roster
                        logger.info(f"Scraped roster for {team} ({len(roster)} players)")
                
                await asyncio.sleep(0.5)  # Small delay between teams
                
            except Exception as e:
                logger.error(f"Error scraping roster for {team}: {e}")
        
        return rosters
    
    def _parse_team_roster(self, soup: BeautifulSoup, team: str, season: int) -> List[Dict]:
        """Parse team roster information"""
        roster = []
        
        try:
            # Find roster table
            roster_table = soup.find('table', {'id': 'roster'})
            if not roster_table:
                return roster
            
            rows = roster_table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 8:
                    player = {
                        'team': team,
                        'season': season,
                        'number': cells[0].get_text(strip=True),
                        'name': cells[1].get_text(strip=True),
                        'position': cells[2].get_text(strip=True),
                        'height': cells[3].get_text(strip=True),
                        'weight': cells[4].get_text(strip=True),
                        'birth_date': cells[5].get_text(strip=True),
                        'college': cells[6].get_text(strip=True),
                        'experience': cells[7].get_text(strip=True)
                    }
                    roster.append(player)
            
        except Exception as e:
            logger.error(f"Error parsing roster for {team}: {e}")
        
        return roster
    
    async def scrape_historical_data(self, seasons: List[int]) -> Dict:
        """Scrape historical data for analysis and predictions"""
        historical_data = {}
        
        for season in seasons:
            logger.info(f"Scraping historical data for season {season}")
            
            # Scrape season standings
            standings = await self.scrape_season_standings(season)
            historical_data[f'standings_{season}'] = standings
            
            # Scrape season leaders
            leaders = await self.scrape_season_leaders(season)
            historical_data[f'leaders_{season}'] = leaders
            
            # Scrape team advanced stats
            team_stats = await self.scrape_team_advanced_stats(season)
            historical_data[f'team_stats_{season}'] = team_stats
        
        return historical_data
    
    async def scrape_season_standings(self, season: int) -> List[Dict]:
        """Scrape season standings"""
        url = f"{settings.BASE_URL}/leagues/NBA_{season}_standings.html"
        html = await self.get_html_with_retry(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        standings = []
        
        try:
            standings_table = soup.find('table', {'id': 'expanded_standings'})
            if standings_table:
                rows = standings_table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 15:
                        standing = {
                            'team': cells[1].get_text(strip=True),
                            'wins': int(cells[2].get_text(strip=True)),
                            'losses': int(cells[3].get_text(strip=True)),
                            'win_pct': float(cells[4].get_text(strip=True)),
                            'games_back': cells[5].get_text(strip=True),
                            'points_for': float(cells[6].get_text(strip=True)),
                            'points_against': float(cells[7].get_text(strip=True)),
                            'srs': float(cells[8].get_text(strip=True)),
                            'sos': float(cells[9].get_text(strip=True)),
                            'off_rtg': float(cells[10].get_text(strip=True)),
                            'def_rtg': float(cells[11].get_text(strip=True)),
                            'pace': float(cells[12].get_text(strip=True)),
                            'fta_per_fga': float(cells[13].get_text(strip=True)),
                            'fg3a_per_fga': float(cells[14].get_text(strip=True))
                        }
                        standings.append(standing)
            
        except Exception as e:
            logger.error(f"Error parsing standings for season {season}: {e}")
        
        return standings
    
    async def scrape_season_leaders(self, season: int) -> Dict:
        """Scrape season statistical leaders"""
        leaders = {}
        
        # Categories to scrape
        categories = ['pts_per_g', 'trb_per_g', 'ast_per_g', 'stl_per_g', 'blk_per_g']
        
        for category in categories:
            try:
                url = f"{settings.BASE_URL}/leagues/NBA_{season}_{category}.html"
                html = await self.get_html_with_retry(url)
                
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    category_leaders = self._parse_leaders_table(soup, category)
                    leaders[category] = category_leaders
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error scraping {category} leaders for season {season}: {e}")
        
        return leaders
    
    def _parse_leaders_table(self, soup: BeautifulSoup, category: str) -> List[Dict]:
        """Parse leaders table"""
        leaders = []
        
        try:
            table = soup.find('table', {'id': 'stats'})
            if table:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 8:
                        leader = {
                            'rank': int(cells[0].get_text(strip=True)),
                            'player': cells[1].get_text(strip=True),
                            'team': cells[2].get_text(strip=True),
                            'value': float(cells[3].get_text(strip=True)),
                            'games': int(cells[4].get_text(strip=True)),
                            'minutes': cells[5].get_text(strip=True)
                        }
                        leaders.append(leader)
            
        except Exception as e:
            logger.error(f"Error parsing leaders table for {category}: {e}")
        
        return leaders
    
    async def scrape_team_advanced_stats(self, season: int) -> List[Dict]:
        """Scrape team advanced statistics"""
        url = f"{settings.BASE_URL}/leagues/NBA_{season}.html"
        html = await self.get_html_with_retry(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        team_stats = []
        
        try:
            # Find team stats table
            stats_table = soup.find('table', {'id': 'advanced-team'})
            if stats_table:
                rows = stats_table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 20:
                        team_stat = {
                            'team': cells[1].get_text(strip=True),
                            'off_rtg': float(cells[2].get_text(strip=True)),
                            'def_rtg': float(cells[3].get_text(strip=True)),
                            'net_rtg': float(cells[4].get_text(strip=True)),
                            'pace': float(cells[5].get_text(strip=True)),
                            'fg_pct': float(cells[6].get_text(strip=True)),
                            'fg3_pct': float(cells[7].get_text(strip=True)),
                            'ft_pct': float(cells[8].get_text(strip=True)),
                            'orb_pct': float(cells[9].get_text(strip=True)),
                            'drb_pct': float(cells[10].get_text(strip=True)),
                            'trb_pct': float(cells[11].get_text(strip=True)),
                            'ast_pct': float(cells[12].get_text(strip=True)),
                            'stl_pct': float(cells[13].get_text(strip=True)),
                            'blk_pct': float(cells[14].get_text(strip=True)),
                            'tov_pct': float(cells[15].get_text(strip=True)),
                            'efg_pct': float(cells[16].get_text(strip=True)),
                            'ts_pct': float(cells[17].get_text(strip=True)),
                            'off_efficiency': float(cells[18].get_text(strip=True)),
                            'def_efficiency': float(cells[19].get_text(strip=True))
                        }
                        team_stats.append(team_stat)
            
        except Exception as e:
            logger.error(f"Error parsing team advanced stats for season {season}: {e}")
        
        return team_stats
    
    # Inherit existing parsing methods from the original scraper
    def parse_game_id(self, url: str) -> str:
        """Extract game ID from URL"""
        match = re.search(r'/(\d{9}[A-Z]{3})\.html', url)
        return match.group(1) if match else url.split('/')[-1].replace('.html', '')
    
    def _parse_basic_game_info(self, soup: BeautifulSoup, game_id: str) -> Optional[Dict]:
        """Parse basic game information"""
        try:
            # Get line score
            line_score_table = soup.find('table', {'id': 'line_score'})
            if not line_score_table:
                return None
            
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
            
            # Determine home/away
            away_team, home_team = teams
            away_score, home_score = scores
            home_won = home_score > away_score
            
            # Get date from URL or page
            date_match = re.search(r'(\d{4})(\d{2})(\d{2})', game_id)
            if date_match:
                year, month, day = date_match.groups()
                game_date = datetime(int(year), int(month), int(day))
            else:
                game_date = datetime.now()
            
            # Get season
            season = game_date.year
            if game_date.month < 7:  # NBA season spans two calendar years
                season -= 1
            
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
            for team in ['home', 'away']:
                basic_table = soup.find('table', {'id': f'box-{team}-game-basic'})
                advanced_table = soup.find('table', {'id': f'box-{team}-game-advanced'})
                
                if not basic_table:
                    continue
                
                basic_stats = self._parse_stats_table(basic_table, 'basic')
                advanced_stats = self._parse_stats_table(advanced_table, 'advanced') if advanced_table else {}
                
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
            if len(cells) < 10:
                return None
            
            player_name = cells[0].get_text(strip=True)
            if not player_name or player_name in ['Reserves', 'Team Totals']:
                return None
            
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
            
            return stats
            
        except Exception as e:
            logger.error(f"Error parsing player row: {e}")
            return None
    
    def _parse_stats_table(self, table, stat_type: str) -> Dict:
        """Parse a statistics table"""
        stats = {}
        
        try:
            if not table:
                return stats
            
            rows = table.find_all('tr')
            if not rows:
                return stats
            
            totals_row = rows[-1]
            cells = totals_row.find_all(['td', 'th'])
            
            if stat_type == 'basic':
                stat_mapping = {
                    1: 'mp', 2: 'fg', 3: 'fga', 4: 'fg_pct', 5: 'fg3', 6: 'fg3a', 
                    7: 'fg3_pct', 8: 'ft', 9: 'fta', 10: 'ft_pct', 11: 'orb', 
                    12: 'drb', 13: 'trb', 14: 'ast', 15: 'stl', 16: 'blk', 
                    17: 'tov', 18: 'pf', 19: 'pts'
                }
            else:
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
                    
                    if stat_type == 'basic':
                        if 'pct' in stat_name:
                            stats[stat_name] = self._safe_float(value)
                        else:
                            stats[stat_name] = self._safe_int(value)
                    else:
                        stats[stat_name] = self._safe_float(value)
            
            # Add max stats for basic stats
            if stat_type == 'basic':
                for row in rows[1:-1]:
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
            officials_div = soup.find('div', string=lambda text: 'Officials:' in str(text) if text else False)
            
            if officials_div:
                official_links = officials_div.find_next_siblings('a')
                for i, official in enumerate(official_links[:3], 1):
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

async def main():
    """Main function to scrape comprehensive data"""
    logger.add("logs/enhanced_scraper.log", rotation="1 day", retention="30 days")
    
    # Define seasons to scrape (current and recent seasons)
    current_year = datetime.now().year
    seasons_to_scrape = list(range(current_year - 5, current_year + 1))
    
    logger.info(f"Starting comprehensive data scraping for seasons: {seasons_to_scrape}")
    
    async with EnhancedBasketballScraper() as scraper:
        # Scrape recent seasons for testing data
        logger.info("Scraping recent seasons for testing data...")
        all_game_urls = await scraper.scrape_recent_seasons(seasons_to_scrape)
        
        # Scrape all games
        total_games = 0
        for season, game_urls in all_game_urls.items():
            logger.info(f"Processing {len(game_urls)} games for season {season}")
            
            for url in game_urls:
                game_data = await scraper.scrape_comprehensive_game_data(url)
                if game_data:
                    await save_comprehensive_game_data(game_data)
                    total_games += 1
                
                if total_games % 50 == 0:
                    logger.info(f"Processed {total_games} games so far")
        
        # Scrape additional data sources
        logger.info("Scraping additional data sources...")
        
        # Scrape team rosters for current season
        current_season = current_year
        rosters = await scraper.scrape_team_rosters(current_season)
        await save_team_rosters(rosters, current_season)
        
        # Scrape historical data
        historical_data = await scraper.scrape_historical_data(seasons_to_scrape)
        await save_historical_data(historical_data)
        
        logger.info(f"Comprehensive scraping complete! Processed {total_games} games")

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
    """Save team rosters to database"""
    # This would require additional database models for rosters
    # For now, save to JSON file
    import json
    
    roster_file = f"data/rosters_{season}.json"
    with open(roster_file, 'w') as f:
        json.dump(rosters, f, indent=2)
    
    logger.info(f"Saved rosters for {len(rosters)} teams to {roster_file}")

async def save_historical_data(historical_data: Dict):
    """Save historical data to database/files"""
    import json
    
    # Save to JSON files for now
    for key, data in historical_data.items():
        filename = f"data/{key}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    logger.info(f"Saved {len(historical_data)} historical data files")

if __name__ == "__main__":
    asyncio.run(main()) 