#!/usr/bin/env python3
"""
Module for parsing raw match data obtained from WhoScored.com into structured DataFrames.

This module takes the match data dictionary (fetched by ws/match.py) and the corresponding
match ID, then transforms the data into Pandas DataFrames suitable for insertion into the
SQLite database, aligning with the schema defined in ws/db.py.
"""

import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import json
import os
import traceback # Import traceback for better error reporting

# NOTE: match_id is now a required argument for both parsing functions

def parse_fixture_data(match_id: int, match_data_dict: Dict[str, Any], competition_id: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Parses the general fixture information from the match data dictionary.

    Args:
        match_id: The WhoScored match ID for this fixture.
        match_data_dict: The dictionary containing match data (keys like 'home', 'away', 'startDate').
        competition_id: Optional competition ID to associate with this fixture.

    Returns:
        A single-row Pandas DataFrame containing fixture details, or None if essential data is missing.
    """
    if not match_data_dict:
        print(f"Error: Input match_data_dict is empty for match ID {match_id}. Cannot parse fixture.")
        return None

    try:
        # Convert 'startDate' (e.g., "2024-03-03T15:30:00") to UTC datetime
        start_date_str = match_data_dict.get("startDate")
        datetime_utc = None
        if start_date_str:
            try:
                # Add 'Z' if timestamp doesn't have timezone info, assuming UTC
                if 'Z' not in start_date_str and '+' not in start_date_str and '-' not in start_date_str[10:]: # Check if timezone info exists
                     if '.' in start_date_str: # Handle milliseconds if present
                         dt_obj = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S.%f")
                     else:
                         dt_obj = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
                     datetime_utc = dt_obj.replace(tzinfo=timezone.utc)
                else:
                    # Let fromisoformat handle timezone if present
                    dt_obj = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                    datetime_utc = dt_obj.astimezone(timezone.utc)

            except ValueError as e:
                print(f"Warning: Could not parse startDate '{start_date_str}' for match {match_id}. Error: {e}")
                datetime_utc = datetime.now(timezone.utc) # Placeholder

        home_team_data = match_data_dict.get("home", {})
        away_team_data = match_data_dict.get("away", {})

        data = {
            "id": match_id, # Use the passed-in match_id
            "competition_id": competition_id,
            "datetime_utc": datetime_utc,
            # Check 'statusDescription' or 'detailedStatus', 'statusCode' might be useful too
            "status": match_data_dict.get("statusDescription") or match_data_dict.get("detailedStatus") or match_data_dict.get("status"),
            # 'stage' might not exist directly, check if needed elsewhere
            "round_name": match_data_dict.get("stage", {}).get("stageName"),

            "home_team_id": home_team_data.get("teamId"),
            "home_team_name": home_team_data.get("name"),
            "away_team_id": away_team_data.get("teamId"),
            "away_team_name": away_team_data.get("name"),

            "home_score": None,
            "away_score": None,

            "referee_name": match_data_dict.get("referee", {}).get("name") or match_data_dict.get("referee", {}).get("officialName"), # Try 'name' first
            "venue_name": match_data_dict.get("venueName") or match_data_dict.get("venue", {}).get("name"), # Try 'venueName' first

            "scraped_at": datetime.now(timezone.utc)
        }

        # Score extraction logic (remains similar, checks ftScore first)
        ft_score_info = match_data_dict.get("ftScore") # Full Time score might be separate
        score_info = match_data_dict.get("score")

        if isinstance(ft_score_info, str) and '-' in ft_score_info: # e.g., "2-1"
             scores = ft_score_info.split('-')
             if len(scores) == 2:
                 data["home_score"] = scores[0].strip()
                 data["away_score"] = scores[1].strip()
        elif isinstance(score_info, str) and '-' in score_info: # Sometimes score is just "1-0"
             scores = score_info.split('-')
             if len(scores) == 2:
                 data["home_score"] = scores[0].strip()
                 data["away_score"] = scores[1].strip()
        # Add check if score is directly numbers
        elif isinstance(score_info, dict) and score_info.get("home") is not None:
             data["home_score"] = score_info.get("home")
             data["away_score"] = score_info.get("away")
        elif match_data_dict.get("homeScore") is not None: # Fallback to direct keys if needed
             data["home_score"] = match_data_dict.get("homeScore")
             data["away_score"] = match_data_dict.get("awayScore")


        df = pd.DataFrame([data])

        # Ensure correct dtypes
        int_cols = ['id', 'competition_id', 'home_team_id', 'away_team_id', 'home_score', 'away_score']
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        return df

    except Exception as e:
        print(f"Error parsing fixture data for match ID {match_id}: {e}")
        traceback.print_exc()
        return None

def parse_minute_data(match_id: int, match_data_dict: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Parses minute-by-minute statistics from the match data dictionary.

    Args:
        match_id: The WhoScored match ID for this fixture.
        match_data_dict: The dictionary containing match data (keys like 'home', 'away').

    Returns:
        A Pandas DataFrame where each row represents a minute of the match
        with corresponding statistics, or None if essential data is missing.
    """
    if not match_data_dict:
        print(f"Error: Input match_data_dict is empty for match ID {match_id}. Cannot parse minute data.")
        return None

    home_stats_data = match_data_dict.get("home", {}).get("stats", {})
    away_stats_data = match_data_dict.get("away", {}).get("stats", {})

    if not home_stats_data and not away_stats_data:
        print(f"Warning: No 'stats' data found for home or away team for match {match_id}. Returning empty DataFrame for minute data.")
        return pd.DataFrame()

    all_minutes_data: List[Dict[str, Any]] = []

    stat_keys_map = {
        "possession": "possession",
        "rating": "ratings",
        "total_shots": "shotsTotal",
        "pass_success": "passSuccess",
        "dribbles": "dribblesWon",
        "aerial_won": "aerialsWon",
        "tackles": "tackleSuccessful",
        "corners": "cornersTotal"
    }

    observed_minutes = set()
    for stat_json_key in stat_keys_map.values():
        # Check if the stat key exists and is a dictionary before accessing keys
        home_stat_val = home_stats_data.get(stat_json_key)
        if isinstance(home_stat_val, dict):
            observed_minutes.update(home_stat_val.keys())

        away_stat_val = away_stats_data.get(stat_json_key)
        if isinstance(away_stat_val, dict):
            observed_minutes.update(away_stat_val.keys())

    if not observed_minutes:
        print(f"Warning: No minute-keyed statistics found for match {match_id}. Returning empty DataFrame for minute data.")
        return pd.DataFrame()

    # Filter out non-digit keys like "fullGame" before sorting
    numeric_minutes = sorted([int(m) for m in observed_minutes if m.isdigit()])

    current_scraped_at = datetime.now(timezone.utc)

    for minute_val in numeric_minutes:
        minute_str = str(minute_val)
        minute_entry: Dict[str, Any] = {
            "match_id": match_id, # Use the passed-in match_id
            "minute": minute_val,
            "added_time": None, # Still not directly available per minute
            "scraped_at": current_scraped_at
        }

        for schema_suffix, json_key in stat_keys_map.items():
            home_stat_dict = home_stats_data.get(json_key, {})
            minute_entry[f"{schema_suffix}_home"] = home_stat_dict.get(minute_str) if isinstance(home_stat_dict, dict) else None

            away_stat_dict = away_stats_data.get(json_key, {})
            minute_entry[f"{schema_suffix}_away"] = away_stat_dict.get(minute_str) if isinstance(away_stat_dict, dict) else None

        all_minutes_data.append(minute_entry)

    if not all_minutes_data:
        print(f"No minute data processed for match {match_id}. Returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(all_minutes_data)

    int_cols = ['match_id', 'minute', 'added_time',
                'total_shots_home', 'total_shots_away',
                'dribbles_home', 'dribbles_away',
                'aerial_won_home', 'aerial_won_away',
                'tackles_home', 'tackles_away',
                'corners_home', 'corners_away']

    float_cols = ['possession_home', 'possession_away',
                  'rating_home', 'rating_away',
                  'pass_success_home', 'pass_success_away']

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')

    return df

# --- Example Usage (for testing this module directly) ---
if __name__ == "__main__":
    print("Running ws/parse.py directly for testing...")

    # The match ID is known when testing this specific file
    test_match_id_from_user = 1821372 # Use the integer ID

    base_path = "."
    if os.path.basename(os.getcwd()) == "ws":
        base_path = ".."

    # Use the string version for the filename
    sample_json_path = os.path.join(base_path, "data", "test_match_output", f"match_{str(test_match_id_from_user)}_fetched_data.json")

    loaded_json_data = None
    if not os.path.exists(sample_json_path):
        print(f"ERROR: Sample JSON file not found at {sample_json_path}")
        print("Please ensure you have run ws/match.py for match ID 1821372 and the output is in the expected location.")
    else:
        try:
            with open(sample_json_path, 'r', encoding='utf-8') as f:
                loaded_json_data = json.load(f)
            print(f"\n--- Loaded sample JSON data from: {sample_json_path} ---")
        except json.JSONDecodeError as e:
             print(f"ERROR: Failed to decode JSON from {sample_json_path}: {e}")
        except Exception as e:
             print(f"ERROR: Failed to load JSON file {sample_json_path}: {e}")

    # --- Use the loaded data directly if available, otherwise use dummy ---
    actual_match_data_to_parse = None
    if loaded_json_data and isinstance(loaded_json_data, dict):
         # Assume the loaded file IS the data dictionary based on user feedback
         print("Using loaded JSON file content directly for parsing.")
         actual_match_data_to_parse = loaded_json_data
    else:
         # Fallback to dummy data only if file loading failed or wasn't a dict
         actual_match_data_to_parse = {
             # Dummy ID is not used here as test_match_id_from_user is passed explicitly
             "startDate": "2024-01-01T12:00:00",
             "home": {"teamId": 1, "name": "Home Team FC", "stats": {"ratings": {"1": 6.5, "2": 6.6}}},
             "away": {"teamId": 2, "name": "Away Team Utd", "stats": {"ratings": {"1": 6.4, "2": 6.5}}},
             "venueName": "Dummy Stadium",
             "referee": {"officialName": "Ref A. Roni"},
             "statusDescription": "FullTime",
             "ftScore": "2-1"
         }
         print("\n--- Using DUMMY match data for parsing test ---")
         # Use a dummy ID consistent with the dummy data if needed elsewhere
         test_match_id_for_dummy = 999999
         test_match_id_to_use = test_match_id_for_dummy if actual_match_data_to_parse["startDate"] == "2024-01-01T12:00:00" else test_match_id_from_user
    
    # Ensure we have a valid match ID to pass
    test_match_id_to_use = test_match_id_from_user if actual_match_data_to_parse != loaded_json_data else test_match_id_from_user

    # --- Proceed with parsing ---
    if actual_match_data_to_parse:
        # Test parse_fixture_data
        print("\n--- Testing parse_fixture_data ---")
        test_competition_id = 101 # Example competition ID
        fixture_df = parse_fixture_data(
            match_id=test_match_id_to_use, # Pass the ID explicitly
            match_data_dict=actual_match_data_to_parse,
            competition_id=test_competition_id
        )
        if fixture_df is not None:
            print("Fixture DataFrame:")
            print(fixture_df.head().to_string())
            print("\nFixture DataFrame Info:")
            fixture_df.info()
            output_dir_test = os.path.join(base_path, "data", "parsed_test_output")
            os.makedirs(output_dir_test, exist_ok=True)
            try:
                # Use the actual match ID in the filename if possible
                output_match_id_str = str(test_match_id_to_use)
                fixture_df.to_csv(os.path.join(output_dir_test, f"parsed_fixture_{output_match_id_str}.csv"), index=False)
                print(f"Saved parsed fixture data to {output_dir_test}")
            except Exception as e:
                 print(f"Error saving fixture DataFrame: {e}")
        else:
            print("Failed to parse fixture data or result was None.")

        # Test parse_minute_data
        print("\n--- Testing parse_minute_data ---")
        minute_df = parse_minute_data(
            match_id=test_match_id_to_use, # Pass the ID explicitly
            match_data_dict=actual_match_data_to_parse
        )
        if minute_df is not None and not minute_df.empty:
            print("\nMinute DataFrame (first 5 rows):")
            print(minute_df.head().to_string())
            print("\nMinute DataFrame (last 5 rows):")
            print(minute_df.tail().to_string())
            print(f"\nTotal minutes parsed: {len(minute_df)}")
            print("\nMinute DataFrame Info:")
            minute_df.info()
            output_dir_test = os.path.join(base_path, "data", "parsed_test_output")
            os.makedirs(output_dir_test, exist_ok=True)
            try:
                 output_match_id_str = str(test_match_id_to_use)
                 minute_df.to_csv(os.path.join(output_dir_test, f"parsed_minutes_{output_match_id_str}.csv"), index=False)
                 print(f"Saved parsed minute data to {output_dir_test}")
            except Exception as e:
                 print(f"Error saving minute DataFrame: {e}")
        elif minute_df is not None and minute_df.empty:
            print("Minute data parsing resulted in an empty DataFrame (e.g., no minute stats found).")
        else:
            print("Failed to parse minute data or result was None.")
    else:
        print("Could not load or determine the actual match data to parse.")


    print("\nws/parse.py testing finished.")
