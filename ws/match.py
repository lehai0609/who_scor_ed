#!/usr/bin/env python3
"""
Module for fetching detailed match data from WhoScored.com.

This module is responsible for acquiring the raw data for a single match,
primarily by extracting the 'matchCentreData' JavaScript object from the
match's live page.
"""

import time
import re
import json
import os
from typing import Dict, Any

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Default timeout for WebDriverWait
DEFAULT_TIMEOUT = 20
# Time to sleep after page load to ensure JavaScript execution
DEFAULT_SCRIPT_WAIT_TIME = 8

def _js_object_to_json_string(js_text: str) -> str:
    """
    Convert a JavaScript object string to a JSON-compatible string.
    This is a helper function for parsing JavaScript objects embedded in HTML.

    Args:
        js_text: A string containing a JavaScript object.

    Returns:
        A string that is more likely to be parsable by json.loads().
    """
    # Quote unquoted keys: { keyName: ... } -> { "keyName": ... }
    # Handles keys with alphanumeric characters, underscores, and dollar signs
    js_text = re.sub(r'([{,]\s*)([a-zA-Z_$][\w$]*)\s*:', r'\1"\2":', js_text)
    
    # Replace single quotes with double quotes for string values,
    # being careful not to replace escaped single quotes or single quotes within double-quoted strings.
    # This regex is complex; it aims to replace 'string' with "string"
    # It looks for ' not preceded by \ (escape) and not followed by ' within a certain distance (heuristic for simple cases)
    # A more robust solution might involve a proper JS parser or more sophisticated regex.
    # For now, this handles many common cases.
    js_text = re.sub(r"(?<!\\)'", '"', js_text)

    # Handle boolean values (true, false) - already valid in JSON
    # js_text = re.sub(r'\btrue\b', 'true', js_text) # No change needed
    # js_text = re.sub(r'\bfalse\b', 'false', js_text) # No change needed
    
    # Handle null values - already valid in JSON
    # js_text = re.sub(r'\bnull\b', 'null', js_text) # No change needed
    
    # Remove trailing commas if any, e.g. [1, 2, ] -> [1, 2] or { "a":1, } -> { "a":1 }
    js_text = re.sub(r',\s*([}\]])', r'\1', js_text)
    
    return js_text

def setup_match_driver(headless: bool = True, user_agent: str = None) -> webdriver.Chrome:
    """
    Set up a Chrome WebDriver instance specifically for fetching match data.
    Similar to fixture scraper's driver but can be tailored if needed.

    Args:
        headless: Whether to run Chrome in headless mode.
        user_agent: Optional custom user agent string.

    Returns:
        A Selenium Chrome WebDriver instance.
    """
    options = ChromeOptions()
    if headless:
        # Using "headless=new" is recommended for modern Chrome versions
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu") # Often recommended for headless
    options.add_argument("--window-size=1920,1080") # Define window size
    
    ua = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f"--user-agent={ua}")
    
    # Disable automation flags to make detection harder
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled") # Another common flag

    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except ValueError as e:
        print(f"WebDriverManager failed: {e}. Attempting to use default ChromeDriver path.")
        driver = webdriver.Chrome(options=options) # Fallback
    except Exception as e:
        print(f"Error setting up Chrome driver: {e}")
        raise

    # Remove navigator.webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def fetch_match_centre_data(match_id: str, headless: bool = True, debug: bool = False, output_dir: str = "data/raw/match_debug") -> Dict[str, Any]:
    """
    Fetches the 'matchCentreData' JavaScript object from a WhoScored match page.

    Args:
        match_id: The WhoScored ID for the match.
        headless: Whether to run the browser in headless mode.
        debug: If True, saves raw JS and parsed JSON for debugging.
        output_dir: Directory to save debug files.

    Returns:
        A dictionary containing the parsed 'matchCentreData'.

    Raises:
        RuntimeError: If the data cannot be found or parsed.
    """
    url = f"https://www.whoscored.com/matches/{match_id}/Live"
    driver = None
    
    print(f"Fetching match data for ID: {match_id} from URL: {url}")

    try:
        driver = setup_match_driver(headless=headless)
        driver.get(url)
        
        # Wait for a known element that indicates page load, e.g., layout wrapper or specific match stats container
        wait = WebDriverWait(driver, DEFAULT_TIMEOUT)
        wait.until(EC.presence_of_element_located((By.ID, "layout-wrapper"))) # As in proto.py
        
        # Additional explicit wait for JavaScript to execute and populate data
        print(f"Page loaded. Waiting {DEFAULT_SCRIPT_WAIT_TIME}s for scripts to populate data...")
        time.sleep(DEFAULT_SCRIPT_WAIT_TIME)
        
        html_content = driver.page_source
        
        # Parse HTML to find the script tag containing 'matchCentreData'
        soup = BeautifulSoup(html_content, "lxml")
        
        # The target script is usually inside #layout-wrapper and contains 'matchCentreData'
        # or 'require.config.params["args"]' as seen in proto.py
        scripts = soup.select("script")
        target_script_content = None
        
        # Look for the script containing 'require.config.params["args"]'
        # This pattern was identified in the user's proto.py
        pattern_args = r'require\.config\.params\["args"\]\s*=\s*({.*?});'
        
        for script in scripts:
            if script.string:
                match = re.search(pattern_args, script.string, re.DOTALL | re.IGNORECASE)
                if match:
                    target_script_content = match.group(1)
                    print("Found 'require.config.params[\"args\"]' script block.")
                    break
        
        if not target_script_content:
            # Fallback: Look for 'matchCentreData' directly if the above pattern fails
            pattern_direct = r'var\s+matchCentreData\s*=\s*({.*?});'
            for script in scripts:
                if script.string:
                    match = re.search(pattern_direct, script.string, re.DOTALL | re.IGNORECASE)
                    if match:
                        target_script_content = match.group(1)
                        print("Found 'matchCentreData' directly in a script block.")
                        break
            
        if not target_script_content:
            if debug:
                os.makedirs(output_dir, exist_ok=True)
                debug_html_path = os.path.join(output_dir, f"match_{match_id}_page_source.html")
                with open(debug_html_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
                print(f"Saved full page HTML for debugging: {debug_html_path}")
            raise RuntimeError(f"Could not locate 'matchCentreData' or 'require.config.params[\"args\"]' script block for match {match_id}.")

        # Convert the extracted JavaScript object string to a JSON-like string
        json_like_string = _js_object_to_json_string(target_script_content)
        
        if debug:
            os.makedirs(output_dir, exist_ok=True)
            raw_js_path = os.path.join(output_dir, f"match_{match_id}_raw_script_extract.js")
            json_like_path = os.path.join(output_dir, f"match_{match_id}_json_like_extract.txt")
            
            with open(raw_js_path, "w", encoding="utf-8") as f:
                f.write(target_script_content)
            print(f"Saved raw extracted JS to: {raw_js_path}")
            
            with open(json_like_path, "w", encoding="utf-8") as f:
                f.write(json_like_string)
            print(f"Saved JSON-like string to: {json_like_path}")

        try:
            # Parse the JSON-like string
            parsed_data = json.loads(json_like_string)
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON-like string for match {match_id}: {e}")
            print(f"Problematic string (first 500 chars): {json_like_string[:500]}")
            raise RuntimeError(f"JSON parsing failed for match {match_id}. Enable debug for details.") from e

        # The actual match data is expected to be under a 'matchCentreData' key within the parsed_data
        # if we extracted from 'require.config.params["args"]'
        if "matchCentreData" in parsed_data:
            match_data = parsed_data["matchCentreData"]
            print(f"Successfully extracted 'matchCentreData' for match {match_id}.")
            return match_data
        elif "matchId" in parsed_data: # If we directly found matchCentreData
            print(f"Successfully extracted direct 'matchCentreData' structure for match {match_id}.")
            return parsed_data # The entire object is the matchCentreData
        else:
            if debug:
                 final_json_path = os.path.join(output_dir, f"match_{match_id}_parsed_full_args.json")
                 with open(final_json_path, "w", encoding="utf-8") as f:
                    json.dump(parsed_data, f, indent=2)
                 print(f"Saved fully parsed args object to: {final_json_path}")
            raise RuntimeError(f"'matchCentreData' key not found in parsed script data for match {match_id}. Parsed keys: {list(parsed_data.keys())}")

    except Exception as e:
        print(f"An error occurred while fetching data for match {match_id}: {e}")
        import traceback
        traceback.print_exc()
        # Consider re-raising or returning a specific error object
        raise
    finally:
        if driver:
            driver.quit()
            print(f"WebDriver closed for match {match_id}.")

if __name__ == "__main__":
    print("Running ws/match.py directly for testing...")
    
    # Example Match ID (use a recent, known valid ID for testing)
    # Ensure this match ID is for a game that has "Live" data available or has finished.
    # You might need to find a suitable ID from WhoScored.com.
    # Let's use the one from your proto.py as an example.
    test_match_id = "1821372" # Example: Man City vs Man Utd, 03 Mar 2024 (check if this ID is still valid/has data)
    
    # Create a dummy competition ID for testing purposes if needed by other parts of a larger test
    # For now, this script focuses only on fetching data.

    # Test with debugging enabled and headless=False to see the browser
    try:
        print(f"\n--- Test 1: Fetching data for match ID {test_match_id} (headless=False, debug=True) ---")
        # Make sure the output_dir for debugging exists or can be created
        debug_output_directory = "data/test_match_output" 
        if not os.path.exists(debug_output_directory):
            os.makedirs(debug_output_directory)
            
        match_data = fetch_match_centre_data(
            test_match_id, 
            headless=False, 
            debug=True, 
            output_dir=debug_output_directory
        )
        
        if match_data:
            print(f"\nSuccessfully fetched data for match {test_match_id}.")
            print(f"Keys in match_data: {list(match_data.keys())}")
            
            # Optionally, save the full fetched data to a file for inspection
            output_file_path = os.path.join(debug_output_directory, f"match_{test_match_id}_fetched_data.json")
            with open(output_file_path, "w", encoding="utf-8") as f:
                json.dump(match_data, f, indent=2)
            print(f"Full fetched data saved to: {output_file_path}")

            # Example: Print some specific parts if known
            if "home" in match_data and "name" in match_data["home"]:
                print(f"Home Team: {match_data['home']['name']}")
            if "away" in match_data and "name" in match_data["away"]:
                print(f"Away Team: {match_data['away']['name']}")
            if "score" in match_data:
                 print(f"Score: {match_data.get('ftScore') or match_data.get('score', 'N/A')}")


        else:
            print(f"Failed to fetch data for match {test_match_id} or data was empty.")

    except RuntimeError as e:
        print(f"RuntimeError during test: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during test: {e}")
        import traceback
        traceback.print_exc()

    # Example of how you might integrate with db.py (conceptual)
    # from ws.db import get_engine, Fixture # Assuming db.py is in the same parent directory or PYTHONPATH is set
    # engine = get_engine("data/ws_test_match.db")
    # This part would typically be in your main CLI/orchestration script.
    print("\nNote: Database integration (saving this data) would be handled by ws.parse.py and the main CLI script.")
