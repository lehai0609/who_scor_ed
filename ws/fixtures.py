#!/usr/bin/env python3
"""
Module for scraping fixture IDs from WhoScored.com.

This module refactors and extends the logic from fetch_epl_fixtures.py
to provide a reusable function for discovering match IDs for various leagues.
It handles navigation from a league overview page to the fixtures section,
and then iterates through previous and next months to collect fixture IDs.
"""

import re
import time
import json
import os
from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    NoSuchElementException,
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

# Default timeout for WebDriverWait
DEFAULT_TIMEOUT = 20
# Default number of retry attempts for certain operations
DEFAULT_RETRY_ATTEMPTS = 3

def setup_driver(headless: bool = True, user_agent: Optional[str] = None) -> webdriver.Chrome:
    """
    Set up Chrome driver with anti-detection measures.
    
    Args:
        headless: Whether to run Chrome in headless mode.
        user_agent: Optional custom user agent string.

    Returns:
        A Selenium Chrome WebDriver instance.
    """
    options = ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Standard user agent to avoid detection
    ua = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f"--user-agent={ua}")
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Using webdriver_manager to handle driver binaries
    # For undetected-chromedriver, the setup would be different:
    # import undetected_chromedriver as uc
    # driver = uc.Chrome(options=options)
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except ValueError as e:
        # Fallback if ChromeDriverManager().install() fails in some environments
        print(f"WebDriverManager failed: {e}. Attempting to use default ChromeDriver path.")
        driver = webdriver.Chrome(options=options)

    # Remove webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def handle_popups(driver: webdriver.Chrome, timeout: int = 5) -> None:
    """
    Handle common popups like cookie consents.

    Args:
        driver: The Selenium WebDriver instance.
        timeout: Time to wait for popup elements.
    """
    popup_selectors = [
        "#onetrust-accept-btn-handler",          # Cookie consent (common ID)
        "button[aria-label='Accept cookies']",   # Another cookie consent variant
        ".qc-cmp2-summary-buttons button[mode='primary']", # Yet another consent
        ".cookie-consent-accept",
        ".modal-close-button",
        "[data-dismiss='modal']"
    ]
    
    for selector in popup_selectors:
        try:
            # Use a shorter timeout for popups as they might not always be present
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            if element.is_displayed():
                print(f"Attempting to close popup with selector: {selector}")
                # Try JS click if normal click is intercepted
                try:
                    element.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", element)
                print(f"Closed popup with selector: {selector}")
                time.sleep(1) # Wait a bit for popup to disappear
        except (NoSuchElementException, TimeoutException):
            # Element not found or not clickable within timeout, which is fine
            continue
        except Exception as e:
            print(f"Error handling popup with selector {selector}: {e}")


def debug_page_state(driver: webdriver.Chrome, stage_name: str, output_dir: str = "debug_screenshots") -> None:
    """
    Debug function to check current page state and save a screenshot.

    Args:
        driver: The Selenium WebDriver instance.
        stage_name: A name for the current debugging stage (e.g., "after_nav_to_fixtures").
        output_dir: Directory to save screenshots.
    """
    print(f"\n=== Debug Info - Stage: {stage_name} ===")
    print(f"Current URL: {driver.current_url}")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    screenshot_name = os.path.join(output_dir, f"debug_{stage_name}_{timestamp}.png")
    try:
        driver.save_screenshot(screenshot_name)
        print(f"Screenshot saved as: {screenshot_name}")
    except Exception as e:
        print(f"Failed to save screenshot: {e}")
    
    # Check for common navigation buttons
    calendar_nav_selectors = [
        ("dayChangeBtn-prev", "ID"),
        ("dayChangeBtn-next", "ID"),
        (".Calendar-module_dayChangeBtn__prev", "CSS_CLASS (example)"), # Placeholder, actual class might vary
        (".Calendar-module_dayChangeBtn__next", "CSS_CLASS (example)"), # Placeholder
        ("a[href*='/matches/']", "CSS (fixture links)")
    ]
    for selector, selector_type_desc in calendar_nav_selectors:
        try:
            if "ID" in selector_type_desc:
                elements = driver.find_elements(By.ID, selector)
            elif "CSS_CLASS" in selector_type_desc:
                 elements = driver.find_elements(By.CLASS_NAME, selector.replace(".","")) # remove leading dot for class name search
            else: # Assume CSS selector
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"Found {len(elements)} elements with {selector_type_desc} '{selector}'")
            if elements and elements[0].is_displayed():
                print(f"  - First element '{selector}' is visible.")
        except Exception:
            print(f"  - Could not check selector '{selector}'.")
    print("=" * 40)


def _extract_fixture_ids_from_page(driver: webdriver.Chrome) -> Set[int]:
    """Helper function to extract fixture IDs from the current page."""
    ids: Set[int] = set()
    try:
        fixture_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/matches/']")
        for link in fixture_links:
            href = link.get_attribute("href")
            if href:
                match = re.search(r"/matches/(\d+)", href)
                if match:
                    ids.add(int(match.group(1)))
    except Exception as e:
        print(f"Error extracting fixture IDs from page: {e}")
    return ids

def _click_element_robustly(driver: webdriver.Chrome, selectors: List[Tuple[str, str]], description: str, timeout: int) -> bool:
    """
    Tries to find and click an element using a list of selectors.

    Args:
        driver: Selenium WebDriver.
        selectors: A list of (selector_value, selector_type) tuples. 
                   selector_type can be "CSS", "ID", "LINK_TEXT", "XPATH".
        description: A description of the element for logging.
        timeout: WebDriverWait timeout.

    Returns:
        True if click was successful, False otherwise.
    """
    button_clicked = False
    for selector_value, selector_type in selectors:
        try:
            wait = WebDriverWait(driver, timeout)
            if selector_type.upper() == "CSS":
                element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector_value)))
            elif selector_type.upper() == "ID":
                element = wait.until(EC.element_to_be_clickable((By.ID, selector_value)))
            elif selector_type.upper() == "LINK_TEXT":
                 element = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, selector_value)))
            elif selector_type.upper() == "XPATH":
                 element = wait.until(EC.element_to_be_clickable((By.XPATH, selector_value)))
            else:
                print(f"Unsupported selector type: {selector_type}")
                continue

            # Scroll to element if necessary
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
            time.sleep(0.5) # Brief pause after scroll

            element.click()
            print(f"Successfully clicked {description} using {selector_type} '{selector_value}'")
            button_clicked = True
            time.sleep(2) # Wait for page to potentially update
            return True # Click successful
            
        except ElementClickInterceptedException:
            print(f"ElementClickInterceptedException for {description} with {selector_type} '{selector_value}'. Trying JS click.")
            try:
                driver.execute_script("arguments[0].click();", element)
                print(f"Successfully JS-clicked {description} using {selector_type} '{selector_value}'")
                button_clicked = True
                time.sleep(2) # Wait for page to potentially update
                return True # Click successful
            except Exception as js_e:
                print(f"JS click also failed for {description} with {selector_type} '{selector_value}': {js_e}")
        except (NoSuchElementException, TimeoutException):
            print(f"{description} not found or clickable with {selector_type} '{selector_value}'")
        except Exception as e:
            print(f"Error clicking {description} with {selector_type} '{selector_value}': {e}")
    
    if not button_clicked:
        print(f"Could not find or click {description} using any provided selectors.")
    return button_clicked


def get_league_fixture_ids(
    league_overview_url: str,
    league_slug: str,
    num_additional_past_months: int = 3,
    num_additional_future_months: int = 1,
    save_file: bool = True,
    output_dir: str = "data/raw",
    headless: bool = True,
    timeout: int = DEFAULT_TIMEOUT,
    enable_debugging: bool = False
) -> Dict:
    """
    Retrieves fixture IDs for a given league from WhoScored.com.

    Args:
        league_overview_url: The URL of the league's main page on WhoScored.
        league_slug: A short name for the league (e.g., "premier-league"), used for filenames.
        num_additional_past_months: Number of past months to scrape (beyond the current).
        num_additional_future_months: Number of future months to scrape (beyond the current).
        save_file: If True, saves the results to a JSON file.
        output_dir: Directory to save the JSON file and debug screenshots.
        headless: Whether to run the browser in headless mode.
        timeout: Timeout in seconds for waiting for elements.
        enable_debugging: If True, saves screenshots at various stages.

    Returns:
        A dictionary containing:
            'fixtures': A sorted list of unique fixture IDs.
            'errors': A list of error messages encountered.
            'league_slug': The provided league slug.
            'total_unique_fixtures': Count of unique fixture IDs.
            'scrape_timestamp': Timestamp of when the scrape finished.
    """
    driver = None
    results = {
        'fixtures': [],
        'errors': [],
        'league_slug': league_slug,
        'total_unique_fixtures': 0,
        'scrape_timestamp': datetime.now().isoformat()
    }
    
    try:
        driver = setup_driver(headless=headless)
        print(f"Navigating to league overview page: {league_overview_url}")
        driver.get(league_overview_url)
        time.sleep(3) # Allow initial load
        handle_popups(driver)
        if enable_debugging:
            debug_page_state(driver, "initial_load", output_dir)

        # Navigate to the "Fixtures" tab/section
        # Common selectors for "Fixtures" link/tab
        fixtures_tab_selectors = [
            ("a[href*='Fixtures']", "CSS"), # Generic
            ("//a[normalize-space()='Fixtures']", "XPATH"), # Text based
            ("ul.ws-sub-navigation a[href*='/Fixtures']", "CSS") # More specific
        ]
        
        print("Attempting to navigate to Fixtures page...")
        if not _click_element_robustly(driver, fixtures_tab_selectors, "Fixtures Tab", timeout):
            results['errors'].append("Failed to navigate to the Fixtures page from overview.")
            if enable_debugging:
                debug_page_state(driver, "fixtures_nav_failed", output_dir)
            raise RuntimeError("Could not navigate to Fixtures page.")
        
        # Current URL should now be the main fixtures page with the calendar
        fixtures_page_url = driver.current_url 
        print(f"Successfully navigated to Fixtures page: {fixtures_page_url}")
        time.sleep(2) # Allow fixtures page to load
        handle_popups(driver) # Handle popups again if they appear on new page
        
        if enable_debugging:
            debug_page_state(driver, "fixtures_page_loaded", output_dir)

        # Wait for at least one fixture link to be present as a sanity check
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/matches/']"))
        )
        print("Initial fixture links found on the page.")

        all_fixture_ids: Set[int] = set()

        # --- Scrape Current Month's View ---
        print("Scraping current month's view...")
        current_ids = _extract_fixture_ids_from_page(driver)
        all_fixture_ids.update(current_ids)
        print(f"Found {len(current_ids)} fixtures in current view. Total unique: {len(all_fixture_ids)}")

        # --- Scrape Past Months ---
        prev_button_selectors = [
            ("dayChangeBtn-prev", "ID"),
            ("button[id*='dayChangeBtn-prev']", "CSS"), # More specific ID match
            ("button[aria-label*='previous month i']", "CSS"), # Common aria-label
            ("button[data-testid='calendar-previous-month']", "CSS"), # Test IDs sometimes exist
            # Add more specific CSS classes if known, e.g. from debug_page_state
            # Example: (".Calendar-module_dayChangeBtn__sEvC8.Calendar-module_prev__XXXX", "CSS")
        ]
        if num_additional_past_months > 0:
            print(f"\nScraping {num_additional_past_months} additional past month(s)...")
            for i in range(num_additional_past_months):
                print(f"Attempting to navigate to previous month {i+1}/{num_additional_past_months}...")
                if not _click_element_robustly(driver, prev_button_selectors, "Previous Month Button", timeout):
                    msg = f"Failed to click 'Previous Month' button on attempt {i+1}."
                    results['errors'].append(msg)
                    print(msg)
                    if enable_debugging:
                        debug_page_state(driver, f"prev_month_click_failed_{i+1}", output_dir)
                    break # Stop trying past months if button fails
                
                time.sleep(2) # Wait for month to update
                handle_popups(driver)
                if enable_debugging:
                    debug_page_state(driver, f"past_month_{i+1}", output_dir)

                new_ids = _extract_fixture_ids_from_page(driver)
                newly_added = len(new_ids - all_fixture_ids)
                all_fixture_ids.update(new_ids)
                print(f"Found {newly_added} new fixtures. Total unique: {len(all_fixture_ids)}")

        # --- Scrape Future Months ---
        # To reliably scrape future months, first navigate back to the fixtures page (which usually defaults to current month)
        if num_additional_future_months > 0:
            print(f"\nResetting to current month's view for future scraping...")
            driver.get(fixtures_page_url) # Re-navigate to reset calendar
            time.sleep(3)
            handle_popups(driver)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/matches/']"))
            )
            if enable_debugging:
                debug_page_state(driver, "reset_for_future_months", output_dir)
            
            print(f"Scraping {num_additional_future_months} additional future month(s)...")
            next_button_selectors = [
                ("dayChangeBtn-next", "ID"),
                ("button[id*='dayChangeBtn-next']", "CSS"),
                ("button[aria-label*='next month i']", "CSS"),
                ("button[data-testid='calendar-next-month']", "CSS"),
            ]
            for i in range(num_additional_future_months):
                print(f"Attempting to navigate to next month {i+1}/{num_additional_future_months}...")
                if not _click_element_robustly(driver, next_button_selectors, "Next Month Button", timeout):
                    msg = f"Failed to click 'Next Month' button on attempt {i+1}."
                    results['errors'].append(msg)
                    print(msg)
                    if enable_debugging:
                        debug_page_state(driver, f"next_month_click_failed_{i+1}", output_dir)
                    break # Stop trying future months if button fails

                time.sleep(2) # Wait for month to update
                handle_popups(driver)
                if enable_debugging:
                    debug_page_state(driver, f"future_month_{i+1}", output_dir)

                new_ids = _extract_fixture_ids_from_page(driver)
                newly_added = len(new_ids - all_fixture_ids)
                all_fixture_ids.update(new_ids)
                print(f"Found {newly_added} new fixtures. Total unique: {len(all_fixture_ids)}")

        results['fixtures'] = sorted(list(all_fixture_ids))
        results['total_unique_fixtures'] = len(all_fixture_ids)

    except WebDriverException as e:
        error_msg = f"WebDriverException occurred: {str(e)}"
        print(error_msg)
        results['errors'].append(error_msg)
        if enable_debugging and driver: # Check if driver exists before taking screenshot
             debug_page_state(driver, "webdriver_exception", output_dir)
    except RuntimeError as e: # Catch custom runtime errors like failure to navigate
        error_msg = f"RuntimeError: {str(e)}"
        print(error_msg)
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"An unexpected error occurred: {type(e).__name__} - {str(e)}"
        print(error_msg)
        results['errors'].append(error_msg)
        # Include traceback for unexpected errors if debugging
        import traceback
        results['errors'].append(traceback.format_exc())
        if enable_debugging and driver:
             debug_page_state(driver, "unexpected_exception", output_dir)
    finally:
        if driver:
            driver.quit()
            print("WebDriver closed.")
    
    results['scrape_timestamp'] = datetime.now().isoformat() # Update timestamp to actual end time

    if save_file:
        _save_results_to_file(results, league_slug, output_dir)
        
    return results

def _save_results_to_file(results: Dict, league_slug: str, output_dir: str = "data/raw") -> None:
    """Saves the scraped fixture data to a JSON file and a simple TXT file."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # Filename based on workflow: {league}_{yyyymmdd}.fixtures.json
    date_str = datetime.now().strftime("%Y%m%d")
    json_filename = os.path.join(output_dir, f"{league_slug}_{date_str}.fixtures.json")
    txt_filename = os.path.join(output_dir, f"{league_slug}_{date_str}_ids.txt")

    try:
        with open(json_filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {json_filename}")

        with open(txt_filename, 'w') as f:
            for fid in results.get('fixtures', []):
                f.write(f"{fid}\n")
        print(f"Fixture IDs saved to {txt_filename}")

    except IOError as e:
        error_msg = f"Error saving results to file: {e}"
        print(error_msg)
        # Add this error to results if it's part of the main dict,
        # but here it's a post-processing step.
        # For now, just print. If this function is called from get_league_fixture_ids
        # before returning, results['errors'] could be appended.


if __name__ == "__main__":
    print("Starting WhoScored Fixture Scraper (Test Run)...")
    
    # Example Usage: Premier League
    # The league overview URL might change structure or domain (e.g. 1xbet.whoscored.com vs www.whoscored.com)
    # Ensure the URL points to a page from which you can navigate to "Fixtures"
    test_league_url = "https://www.whoscored.com/Regions/252/Tournaments/2/England-Premier-League"
    test_league_slug = "england-premier-league"
    
    # Scrape current month, 1 additional past month, and 0 additional future months
    # Enable debugging for screenshots
    # Run with headless=False for easier visual debugging during development
    fixture_data = get_league_fixture_ids(
        league_overview_url=test_league_url,
        league_slug=test_league_slug,
        num_additional_past_months=1, # Current + 1 past
        num_additional_future_months=0, # No future months beyond current
        save_file=True,
        output_dir="data/raw_fixtures", # Custom output directory for this test
        headless=False, # Set to False to see the browser actions
        enable_debugging=True 
    )
    
    print("\n=== SCRAPE SUMMARY ===")
    print(f"League: {fixture_data.get('league_slug')}")
    print(f"Total unique fixtures found: {fixture_data.get('total_unique_fixtures')}")
    print(f"Number of errors: {len(fixture_data.get('errors', []))}")
    
    if fixture_data.get('errors'):
        print("\nErrors encountered:")
        for error in fixture_data['errors']:
            print(f"  - {error}")
            
    if fixture_data.get('fixtures'):
        print(f"\nFirst 10 fixture IDs (sample):")
        for fid in fixture_data['fixtures'][:10]:
            print(f"  {fid}")
    else:
        print("\nNo fixture IDs found.")

    # Example for a different league (e.g., La Liga)
    # test_league_url_laliga = "https://www.whoscored.com/Regions/211/Tournaments/4/Spain-La-Liga"
    # test_league_slug_laliga = "spain-la-liga"
    # fixture_data_laliga = get_league_fixture_ids(
    #     league_overview_url=test_league_url_laliga,
    #     league_slug=test_league_slug_laliga,
    #     num_additional_past_months=0, 
    #     num_additional_future_months=0, # Just current month
    #     save_file=True,
    #     output_dir="data/raw_fixtures",
    #     headless=True, 
    #     enable_debugging=False
    # )
    # print("\nLa Liga Summary:")
    # print(f"Total fixtures: {fixture_data_laliga.get('total_unique_fixtures')}")
