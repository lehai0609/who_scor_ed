import time
import re
import json
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

def js_object_to_json(js_text: str) -> str:
    """Convert JavaScript object to JSON string"""
    # Quote unquoted keys: keyName: → "keyName":
    js_text = re.sub(r'([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:', r'\1"\2":', js_text)
    # Replace single quotes with double quotes, but handle escaped quotes
    js_text = re.sub(r"(?<!\\)'", '"', js_text)
    # Handle boolean values
    js_text = re.sub(r'\btrue\b', 'true', js_text)
    js_text = re.sub(r'\bfalse\b', 'false', js_text)
    # Handle null values
    js_text = re.sub(r'\bnull\b', 'null', js_text)
    return js_text

def fetch_match_centre_data(match_id: str) -> dict:
    """Fetch matchCentreData from whoscored.com using Selenium"""
    url = f"https://www.whoscored.com/matches/{match_id}/live"
    
    # Setup headless Chrome
    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # For Selenium 4.6+, this automatically manages the driver
    driver = webdriver.Chrome(options=chrome_opts)
    
    try:
        driver.get(url)
        
        # Wait for the page to load and JavaScript to execute
        wait = WebDriverWait(driver, 20)
        # Wait for the layout-wrapper to be present
        wait.until(EC.presence_of_element_located((By.ID, "layout-wrapper")))
        
        # Additional sleep to ensure all JavaScript has executed
        time.sleep(8)
        
        html = driver.page_source
        
        # Parse the HTML
        soup = BeautifulSoup(html, "lxml")
        script = soup.select_one("div#layout-wrapper > script")
        
        if not script or not script.string:
            raise RuntimeError("layout-wrapper script tag not found")
        
        # Find the require.config.params["args"] object
        pattern = r'require\.config\.params\["args"\]\s*=\s*({.*?});'
        match = re.search(pattern, script.string, re.DOTALL)
        
        if not match:
            raise RuntimeError("Could not locate require.config.params['args']")
        
        raw_js = match.group(1)
        
        # Convert JavaScript object to JSON
        json_text = js_object_to_json(raw_js)
        
        # Save the raw JavaScript and JSON for debugging
        with open(f"debug_raw_js_{match_id}.txt", "w", encoding="utf-8") as f:
            f.write(raw_js)
        
        with open(f"debug_json_{match_id}.txt", "w", encoding="utf-8") as f:
            f.write(json_text)
        
        print(f"\n=== DEBUG: Saved raw JS to debug_raw_js_{match_id}.txt ===")
        print(f"=== DEBUG: Saved JSON to debug_json_{match_id}.txt ===\n")
        
        try:
            args = json.loads(json_text)
            print("\n=== DEBUG: Extracted args keys ===")
            print("Keys in args:", list(args.keys()) if isinstance(args, dict) else "args is not a dict")
            if "matchCentreData" in args:
                print("matchCentreData found!")
            else:
                print("matchCentreData: NOT FOUND")
                print("Available keys:", list(args.keys()) if isinstance(args, dict) else "N/A")
            print("=================================\n")
            
            return args["matchCentreData"]
        except json.JSONDecodeError as e:
            # If JSON parsing fails, try a more robust approach
            print(f"JSON parsing failed: {e}")
            print("Raw JS:", raw_js[:200] + "...")
            raise
    
    finally:
        driver.quit()

def process_match_data(match_centre_data: dict):
    """Process match centre data into DataFrames"""
    # Let's debug what we're getting
    print("\n=== DEBUG: Match Centre Data Keys ===")
    print("Top level keys:", list(match_centre_data.keys()) if match_centre_data else "No data")
    
    # Check if teamPerformance exists
    if "teamPerformance" in match_centre_data:
        print("teamPerformance keys:", list(match_centre_data["teamPerformance"].keys()))
        print("possessionGraph:", match_centre_data["teamPerformance"].get("possessionGraph", "NOT FOUND"))
    else:
        print("teamPerformance: NOT FOUND")
    
    # Check if playerRatingGraph exists
    if "playerRatingGraph" in match_centre_data:
        print("playerRatingGraph (first 3 items):", match_centre_data["playerRatingGraph"][:3] if match_centre_data["playerRatingGraph"] else "EMPTY")
    else:
        print("playerRatingGraph: NOT FOUND")
    
    print("================================\n")
    
    # Process possession data
    possession_data = match_centre_data.get("teamPerformance", {}).get("possessionGraph", [])
    if possession_data:
        pos_df = pd.DataFrame(possession_data, columns=["minute", "pct_home", "pct_away"])
    else:
        pos_df = pd.DataFrame()
    
    # Process player rating data
    rating_data = match_centre_data.get("playerRatingGraph", [])
    if rating_data:
        rate_df = pd.DataFrame(rating_data, columns=["minute", "rating"])
    else:
        rate_df = pd.DataFrame()
    
    return pos_df, rate_df

if __name__ == "__main__":
    # Replace with your match ID
    MATCH_ID = "1825717"
    
    try:
        # Fetch match centre data
        match_centre_data = fetch_match_centre_data(MATCH_ID)
        
        # Process the data
        pos_df, rate_df = process_match_data(match_centre_data)
        
        # Display results
        print(f"Successfully extracted data for match {MATCH_ID}")
        print("\nPossession data (first 5 rows):")
        print(pos_df.head())
        print(f"\nTotal possession entries: {len(pos_df)}")
        
        print("\nRating data (first 5 rows):")
        print(rate_df.head())
        print(f"\nTotal rating entries: {len(rate_df)}")
        
        # Optionally save to files
        # pos_df.to_csv(f"possession_{MATCH_ID}.csv", index=False)
        # rate_df.to_csv(f"ratings_{MATCH_ID}.csv", index=False)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()