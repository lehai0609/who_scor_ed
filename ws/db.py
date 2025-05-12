#!/usr/bin/env python3
"""
Database module for the WhoScored scraper.

Handles SQLite database connection, schema definition (using SQLAlchemy ORM),
and provides helper functions for common database operations like upserting
DataFrames and checking for existing records.

Reflects schema changes from the updated workflow:
- Removed PlayerEvent and AggStat tables.
- Updated fields in the MinuteData table to have home/away specifics.
"""

import pandas as pd
from datetime import datetime
import os

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

# --- Configuration ---
DEFAULT_DB_PATH = "data/ws.db"

# --- SQLAlchemy Setup ---
Base = declarative_base()

# --- Model Definitions ---

class Competition(Base):
    """
    Represents a football competition (league/tournament).
    """
    __tablename__ = "competitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    whoscored_id = Column(Integer, unique=True, nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    country = Column(String, nullable=True)
    season = Column(String, nullable=False)
    stage = Column(String, nullable=True)
    
    scraped_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    fixtures = relationship("Fixture", back_populates="competition")

    def __repr__(self):
        return f"<Competition(id={self.id}, name='{self.name}', season='{self.season}')>"

class Fixture(Base):
    """
    Represents a single football match (fixture).
    """
    __tablename__ = "fixtures"

    id = Column(Integer, primary_key=True) # WhoScored Match ID
    competition_id = Column(Integer, ForeignKey("competitions.id"), nullable=True)
    
    datetime_utc = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=True)
    round_name = Column(String, nullable=True)
    
    home_team_id = Column(Integer, nullable=False, index=True)
    home_team_name = Column(String, nullable=False)
    away_team_id = Column(Integer, nullable=False, index=True)
    away_team_name = Column(String, nullable=False)
    
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    
    referee_name = Column(String, nullable=True)
    venue_name = Column(String, nullable=True)
    
    scraped_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    competition = relationship("Competition", back_populates="fixtures")
    minutes_data = relationship("MinuteData", back_populates="fixture", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Fixture(id={self.id}, home='{self.home_team_name}', away='{self.away_team_name}', date='{self.datetime_utc.strftime('%Y-%m-%d')}')>"

class MinuteData(Base):
    """
    Stores minute-by-minute aggregated data for a match.
    Updated with new home/away specific fields from the workflow.
    """
    __tablename__ = "minutes_data"

    match_id = Column(Integer, ForeignKey("fixtures.id"), primary_key=True)
    minute = Column(Integer, primary_key=True)
    
    added_time = Column(Integer, nullable=True)
    
    possession_home = Column(Float, nullable=True)
    possession_away = Column(Float, nullable=True)
    
    rating_home = Column(Float, nullable=True)
    rating_away = Column(Float, nullable=True)
    
    # New fields from the updated workflow (home/away specific):
    total_shots_home = Column(Integer, nullable=True)
    total_shots_away = Column(Integer, nullable=True)
    
    pass_success_home = Column(Float, nullable=True) # Assuming Float (percentage)
    pass_success_away = Column(Float, nullable=True) # Assuming Float (percentage)
    
    dribbles_home = Column(Integer, nullable=True)
    dribbles_away = Column(Integer, nullable=True)
    
    aerial_won_home = Column(Integer, nullable=True)
    aerial_won_away = Column(Integer, nullable=True)
    
    tackles_home = Column(Integer, nullable=True)
    tackles_away = Column(Integer, nullable=True)
    
    corners_home = Column(Integer, nullable=True)
    corners_away = Column(Integer, nullable=True)
    
    scraped_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    fixture = relationship("Fixture", back_populates="minutes_data")

    def __repr__(self):
        return f"<MinuteData(match_id={self.match_id}, minute={self.minute})>"

# --- Helper Functions ---

def get_engine(db_path: str = DEFAULT_DB_PATH, create_tables: bool = True):
    """
    Creates and returns a SQLAlchemy engine for the SQLite database.
    Optionally creates all defined tables if they don't exist.
    """
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Created database directory: {db_dir}")

    engine = create_engine(f"sqlite:///{db_path}")
    if create_tables:
        try:
            Base.metadata.create_all(engine)
            print(f"Tables created successfully (if they didn't exist) in {db_path}")
        except SQLAlchemyError as e:
            print(f"Error creating tables: {e}")
            raise
    return engine

def upsert_df(engine, df: pd.DataFrame, table: Base, pk_cols: list[str]):
    """
    Upserts a Pandas DataFrame into the specified SQLAlchemy table.
    Uses SQLite's 'ON CONFLICT DO UPDATE' for efficiency.
    """
    if df.empty:
        print(f"DataFrame for table '{table.name}' is empty. Nothing to upsert.")
        return

    missing_pk_cols = [col for col in pk_cols if col not in df.columns]
    if missing_pk_cols:
        raise ValueError(f"Primary key columns {missing_pk_cols} not found in DataFrame for table '{table.name}'.")

    records_to_insert = df.to_dict(orient='records')
    table_columns = [c.name for c in table.columns]
    
    filtered_records = []
    for record in records_to_insert:
        filtered_record = {key: value for key, value in record.items() if key in table_columns}
        for pk_col in pk_cols:
            if pk_col not in filtered_record and pk_col in record:
                 filtered_record[pk_col] = record[pk_col]
        filtered_records.append(filtered_record)
    
    if not filtered_records:
        print(f"No valid records to upsert for table '{table.name}' after filtering columns.")
        return

    update_cols = {
        col.name: col for col in table.columns if col.name not in pk_cols
    }
    
    stmt = sqlite_insert(table.__table__).values(filtered_records)
    
    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=pk_cols,
        set_={
            col_name: getattr(stmt.excluded, col_name) for col_name in update_cols
        }
    )

    try:
        with engine.connect() as connection:
            connection.execute(on_conflict_stmt)
            connection.commit()
        print(f"Successfully upserted {len(filtered_records)} records into '{table.name}'.")
    except SQLAlchemyError as e:
        print(f"Error upserting data into '{table.name}': {e}")
        raise

def fixture_exists(engine, match_id: int) -> bool:
    """
    Checks if a fixture with the given match_id exists in the database.
    """
    Session = sessionmaker(bind=engine)
    session = None
    try:
        session = Session()
        exists_query = session.query(Fixture.id).filter_by(id=match_id).scalar() is not None
        return exists_query
    except SQLAlchemyError as e:
        print(f"Error checking if fixture {match_id} exists: {e}")
        return False
    finally:
        if session:
            session.close()

# --- Example Usage (for testing this module directly) ---
if __name__ == "__main__":
    print("Running DB module tests (revised schema V2)...")
    
    test_db_path = "data/test_ws_revised_v2.db" # Using a new test DB file
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        print(f"Removed old test database: {test_db_path}")

    engine = get_engine(db_path=test_db_path, create_tables=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    db_session = SessionLocal()

    # 1. Test Competition Table
    print("\n--- Testing Competition Table ---")
    retrieved_comp_id = None
    try:
        new_comp = Competition(whoscored_id=252, name="Premier League", country="England", season="2024/2025")
        db_session.add(new_comp)
        db_session.commit()
        db_session.refresh(new_comp)
        retrieved_comp_id = new_comp.id 
        print(f"Added competition: {new_comp}")

        retrieved_comp = db_session.query(Competition).filter_by(id=retrieved_comp_id).first()
        print(f"Retrieved competition: {retrieved_comp}")
        assert retrieved_comp is not None
        assert retrieved_comp.country == "England"
    except Exception as e:
        print(f"Error during Competition test: {e}")
        db_session.rollback()

    # 2. Test Fixture Table & fixture_exists
    print("\n--- Testing Fixture Table & fixture_exists ---")
    fixture_id_to_test = 1800001
    try:
        if not fixture_exists(engine, 12345):
            print("Fixture 12345 does not exist (correct).")
        
        if retrieved_comp_id is None:
            comp_for_fixture = db_session.query(Competition).filter_by(name="Premier League").first()
            if not comp_for_fixture:
                 comp_for_fixture = Competition(whoscored_id=2520, name="Premier League Fallback", country="England", season="2024/2025")
                 db_session.add(comp_for_fixture)
                 db_session.commit()
                 db_session.refresh(comp_for_fixture)
            retrieved_comp_id = comp_for_fixture.id

        fixtures_data = {
            'id': [fixture_id_to_test, 1800002],
            'competition_id': [retrieved_comp_id, retrieved_comp_id],
            'datetime_utc': [datetime(2024, 8, 17, 14, 0, 0), datetime(2024, 8, 17, 16, 30, 0)],
            'status': ['Scheduled', 'Scheduled'],
            'home_team_id': [10, 20],'home_team_name': ['Team A', 'Team C'],
            'away_team_id': [15, 25],'away_team_name': ['Team B', 'Team D'],
        }
        fixtures_df = pd.DataFrame(fixtures_data)
        upsert_df(engine, fixtures_df, Fixture, pk_cols=['id'])

        retrieved_fixture = db_session.query(Fixture).filter_by(id=fixture_id_to_test).first()
        print(f"Retrieved fixture after upsert: {retrieved_fixture}")
        assert retrieved_fixture is not None and retrieved_fixture.home_team_name == "Team A"

        if fixture_exists(engine, fixture_id_to_test):
            print(f"Fixture {fixture_id_to_test} exists (correct).")

        updated_fixtures_data = {
            'id': [fixture_id_to_test], 'competition_id': [retrieved_comp_id],
            'datetime_utc': [datetime(2024, 8, 17, 14, 0, 0)], 'status': ['FullTime'],
            'home_team_id': [10],'home_team_name': ['Team A Updated'],
            'away_team_id': [15],'away_team_name': ['Team B'],
            'home_score': [2], 'away_score': [1]
        }
        updated_fixtures_df = pd.DataFrame(updated_fixtures_data)
        upsert_df(engine, updated_fixtures_df, Fixture, pk_cols=['id'])
        
        db_session.expire(retrieved_fixture)
        updated_retrieved_fixture = db_session.query(Fixture).filter_by(id=fixture_id_to_test).first()
        print(f"Retrieved fixture after second upsert (update): {updated_retrieved_fixture}")
        assert updated_retrieved_fixture.status == "FullTime" and updated_retrieved_fixture.home_team_name == "Team A Updated"

    except Exception as e:
        print(f"Error during Fixture test: {e}")
        db_session.rollback()

    # 3. Test MinuteData Table (with new home/away fields)
    print("\n--- Testing MinuteData Table (home/away fields) ---")
    try:
        minute_data_list = [
            {
                'match_id': fixture_id_to_test, 'minute': 1, 
                'possession_home': 0.55, 'possession_away': 0.45,
                'rating_home': 6.5, 'rating_away': 6.4,
                'total_shots_home': 1, 'total_shots_away': 0,
                'pass_success_home': 0.80, 'pass_success_away': 0.75,
                'dribbles_home': 0, 'dribbles_away': 1,
                'aerial_won_home': 1, 'aerial_won_away': 0,
                'tackles_home': 2, 'tackles_away': 1,
                'corners_home': 0, 'corners_away': 1
            },
            {
                'match_id': fixture_id_to_test, 'minute': 2, 
                'possession_home': 0.50, 'possession_away': 0.50,
                'rating_home': 6.5, 'rating_away': 6.5,
                'total_shots_home': 0, 'total_shots_away': 1,
                'pass_success_home': 0.85, 'pass_success_away': 0.82,
                'dribbles_home': 1, 'dribbles_away': 0,
                'aerial_won_home': 0, 'aerial_won_away': 1,
                'tackles_home': 1, 'tackles_away': 2,
                'corners_home': 1, 'corners_away': 0
            }
        ]
        minute_data_df = pd.DataFrame(minute_data_list)
        upsert_df(engine, minute_data_df, MinuteData, pk_cols=['match_id', 'minute'])

        retrieved_minute_data = db_session.query(MinuteData).filter_by(match_id=fixture_id_to_test, minute=1).first()
        print(f"Retrieved minute_data for match {fixture_id_to_test}, minute 1: {retrieved_minute_data}")
        assert retrieved_minute_data is not None
        assert retrieved_minute_data.total_shots_home == 1
        assert retrieved_minute_data.total_shots_away == 0
        assert retrieved_minute_data.pass_success_home == 0.80
        assert retrieved_minute_data.corners_away == 1

    except Exception as e:
        print(f"Error during MinuteData test: {e}")
        db_session.rollback()
    finally:
        if db_session:
            db_session.close()

    print("\nDB module tests (revised schema V2) finished.")
    # if os.path.exists(test_db_path):
    #     os.remove(test_db_path)
    #     print(f"Removed test database: {test_db_path}")

