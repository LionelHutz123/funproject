from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
from typing import Optional, List
from config import settings

Base = declarative_base()

class Game(Base):
    __tablename__ = "games"
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), unique=True, nullable=False)
    date = Column(DateTime, nullable=False)
    season = Column(Integer, nullable=False)
    home_team = Column(String(10), nullable=False)
    away_team = Column(String(10), nullable=False)
    home_score = Column(Integer, nullable=False)
    away_score = Column(Integer, nullable=False)
    home_won = Column(Boolean, nullable=False)
    
    # Relationships
    home_stats = relationship("TeamGameStats", foreign_keys="TeamGameStats.home_game_id", back_populates="home_game")
    away_stats = relationship("TeamGameStats", foreign_keys="TeamGameStats.away_game_id", back_populates="away_game")
    officials = relationship("GameOfficial", back_populates="game")
    
    def __repr__(self):
        return f"<Game(id={self.id}, {self.away_team}@{self.home_team}, {self.date})>"

class TeamGameStats(Base):
    __tablename__ = "team_game_stats"
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), ForeignKey("games.game_id"), nullable=False)
    team = Column(String(10), nullable=False)
    is_home = Column(Boolean, nullable=False)
    
    # Basic stats
    fg = Column(Integer, default=0)  # Field Goals Made
    fga = Column(Integer, default=0)  # Field Goal Attempts
    fg_pct = Column(Float, default=0.0)  # Field Goal Percentage
    fg3 = Column(Integer, default=0)  # 3-Point Field Goals Made
    fg3a = Column(Integer, default=0)  # 3-Point Field Goal Attempts
    fg3_pct = Column(Float, default=0.0)  # 3-Point Field Goal Percentage
    ft = Column(Integer, default=0)  # Free Throws Made
    fta = Column(Integer, default=0)  # Free Throw Attempts
    ft_pct = Column(Float, default=0.0)  # Free Throw Percentage
    orb = Column(Integer, default=0)  # Offensive Rebounds
    drb = Column(Integer, default=0)  # Defensive Rebounds
    trb = Column(Integer, default=0)  # Total Rebounds
    ast = Column(Integer, default=0)  # Assists
    stl = Column(Integer, default=0)  # Steals
    blk = Column(Integer, default=0)  # Blocks
    tov = Column(Integer, default=0)  # Turnovers
    pf = Column(Integer, default=0)  # Personal Fouls
    pts = Column(Integer, default=0)  # Points
    
    # Advanced stats
    ts_pct = Column(Float, default=0.0)  # True Shooting Percentage
    efg_pct = Column(Float, default=0.0)  # Effective Field Goal Percentage
    fg3a_rate = Column(Float, default=0.0)  # 3-Point Attempt Rate
    fta_rate = Column(Float, default=0.0)  # Free Throw Attempt Rate
    orb_pct = Column(Float, default=0.0)  # Offensive Rebound Percentage
    drb_pct = Column(Float, default=0.0)  # Defensive Rebound Percentage
    trb_pct = Column(Float, default=0.0)  # Total Rebound Percentage
    ast_pct = Column(Float, default=0.0)  # Assist Percentage
    stl_pct = Column(Float, default=0.0)  # Steal Percentage
    blk_pct = Column(Float, default=0.0)  # Block Percentage
    tov_pct = Column(Float, default=0.0)  # Turnover Percentage
    usg_pct = Column(Float, default=0.0)  # Usage Percentage
    off_rtg = Column(Float, default=0.0)  # Offensive Rating
    def_rtg = Column(Float, default=0.0)  # Defensive Rating
    
    # Max stats (for individual players)
    fg_max = Column(Integer, default=0)
    fga_max = Column(Integer, default=0)
    fg3_max = Column(Integer, default=0)
    fg3a_max = Column(Integer, default=0)
    ft_max = Column(Integer, default=0)
    fta_max = Column(Integer, default=0)
    orb_max = Column(Integer, default=0)
    drb_max = Column(Integer, default=0)
    trb_max = Column(Integer, default=0)
    ast_max = Column(Integer, default=0)
    stl_max = Column(Integer, default=0)
    blk_max = Column(Integer, default=0)
    tov_max = Column(Integer, default=0)
    pf_max = Column(Integer, default=0)
    pts_max = Column(Integer, default=0)
    
    # Foreign keys for relationships
    home_game_id = Column(Integer, ForeignKey("games.id"))
    away_game_id = Column(Integer, ForeignKey("games.id"))
    
    # Relationships
    home_game = relationship("Game", foreign_keys=[home_game_id], back_populates="home_stats")
    away_game = relationship("Game", foreign_keys=[away_game_id], back_populates="away_stats")
    
    __table_args__ = (
        UniqueConstraint('game_id', 'team', name='unique_game_team'),
    )
    
    def __repr__(self):
        return f"<TeamGameStats(team={self.team}, game_id={self.game_id})>"

class GameOfficial(Base):
    __tablename__ = "game_officials"
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), ForeignKey("games.game_id"), nullable=False)
    official_name = Column(String(100), nullable=False)
    official_url = Column(String(500))
    position = Column(Integer, nullable=False)  # 1, 2, 3 for ref positions
    
    # Relationship
    game = relationship("Game", back_populates="officials")
    
    def __repr__(self):
        return f"<GameOfficial(name={self.official_name}, game_id={self.game_id})>"

class PlayerGameStats(Base):
    __tablename__ = "player_game_stats"
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String(50), nullable=False)
    team = Column(String(10), nullable=False)
    player_name = Column(String(100), nullable=False)
    player_url = Column(String(500))
    
    # Basic stats
    mp = Column(String(10), default="0")  # Minutes Played
    fg = Column(Integer, default=0)
    fga = Column(Integer, default=0)
    fg_pct = Column(Float, default=0.0)
    fg3 = Column(Integer, default=0)
    fg3a = Column(Integer, default=0)
    fg3_pct = Column(Float, default=0.0)
    ft = Column(Integer, default=0)
    fta = Column(Integer, default=0)
    ft_pct = Column(Float, default=0.0)
    orb = Column(Integer, default=0)
    drb = Column(Integer, default=0)
    trb = Column(Integer, default=0)
    ast = Column(Integer, default=0)
    stl = Column(Integer, default=0)
    blk = Column(Integer, default=0)
    tov = Column(Integer, default=0)
    pf = Column(Integer, default=0)
    pts = Column(Integer, default=0)
    plus_minus = Column(Integer, default=0)
    
    # Advanced stats
    ts_pct = Column(Float, default=0.0)
    efg_pct = Column(Float, default=0.0)
    fg3a_rate = Column(Float, default=0.0)
    fta_rate = Column(Float, default=0.0)
    orb_pct = Column(Float, default=0.0)
    drb_pct = Column(Float, default=0.0)
    trb_pct = Column(Float, default=0.0)
    ast_pct = Column(Float, default=0.0)
    stl_pct = Column(Float, default=0.0)
    blk_pct = Column(Float, default=0.0)
    tov_pct = Column(Float, default=0.0)
    usg_pct = Column(Float, default=0.0)
    off_rtg = Column(Float, default=0.0)
    def_rtg = Column(Float, default=0.0)
    bpm = Column(Float, default=0.0)  # Box Plus/Minus
    
    __table_args__ = (
        UniqueConstraint('game_id', 'team', 'player_name', name='unique_player_game'),
    )
    
    def __repr__(self):
        return f"<PlayerGameStats(player={self.player_name}, team={self.team}, game_id={self.game_id})>"

# Database setup
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    """Create all tables in the database"""
    Base.metadata.create_all(bind=engine)

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 