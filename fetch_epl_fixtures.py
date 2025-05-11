#!/usr/bin/env python3
"""
Revised Premier League fixture ID scraper with improved error handling,
debugging output, and better waiting mechanisms.
"""

import re
import time
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


def setup_driver():
    """Set up Chrome driver with anti-detection measures"""
    options = Options()
    # Try running without headless first for debugging
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Add user agent to avoid detection
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    # Remove webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver


def handle_popups(driver):
    """Handle any popups that might appear"""
    # Common popup selectors to check
    popup_selectors = [
        "#onetrust-accept-btn-handler",  # Cookie consent
        ".privacy-policy-banner .accept-button",
        ".modal-close-button",
        "[data-dismiss='modal']",
        ".cookie-consent-accept"
    ]
    
    for selector in popup_selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            if element.is_displayed():
                element.click()
                print(f"Closed popup with selector: {selector}")
                time.sleep(1)
        except NoSuchElementException:
            continue


def debug_page_state(driver, iteration):
    """Debug function to check current page state"""
    print(f"\n=== Debug Info - Iteration {iteration} ===")
    print(f"Current URL: {driver.current_url}")
    
    # Check for various possible selectors
    selectors_to_check = [
        ("dayChangeBtn-prev", "ID"),
        (".Calendar-module_dayChangeBtn__sEvC8", "CSS"),
        ("button[id*='dayChange']", "CSS"),
        ("button[id*='prev']", "CSS"),
        ("a[href*='/matches/']", "CSS")
    ]
    
    for selector, selector_type in selectors_to_check:
        try:
            if selector_type == "ID":
                elements = driver.find_elements(By.ID, selector)
            else:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
            print(f"Found {len(elements)} elements with {selector_type} '{selector}'")
            if elements:
                first_element = elements[0]
                print(f"  - First element visible: {first_element.is_displayed()}")
                print(f"  - First element enabled: {first_element.is_enabled()}")
        except Exception as e:
            print(f"  - Error checking '{selector}': {e}")
    
    # Take a screenshot for debugging
    screenshot_name = f"debug_iteration_{iteration}.png"
    driver.save_screenshot(screenshot_name)
    print(f"Screenshot saved as: {screenshot_name}")
    print("=" * 40)


def get_fixture_ids(months=6, timeout=20, retry_attempts=3):
    """Get EPL fixture IDs with improved error handling"""
    driver = setup_driver()
    results = {
        'fixtures': [],
        'errors': [],
        'debug_info': [],
        'total_unique_fixtures': 0
    }
    
    try:
        base_url = (
            "https://www.whoscored.com/regions/252/"
            "tournaments/2/seasons/10316/stages/23400/fixtures/"
            "england-premier-league-2024-2025"
        )
        
        print(f"Opening URL: {base_url}")
        driver.get(base_url)
        
        # Wait for page to load and handle any popups
        time.sleep(5)
        handle_popups(driver)
        
        # Debug initial page state
        debug_page_state(driver, 0)
        
        # Wait for some content to load first
        for _ in range(retry_attempts):
            try:
                wait = WebDriverWait(driver, timeout)
                # Wait for any fixture links to appear
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/matches/']")))
                print("Initial fixtures loaded successfully")
                break
            except TimeoutException:
                print(f"Timeout waiting for fixtures. Retrying...")
                time.sleep(3)
        
        fixture_ids = set()
        
        # Get fixtures from current month first
        print("Getting fixtures from current month...")
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/matches/']"):
            href = a.get_attribute("href")
            m = re.search(r"/matches/(\d+)", href)
            if m:
                fixture_ids.add(int(m.group(1)))
        
        print(f"Found {len(fixture_ids)} fixtures in current month")
        
        # Try to navigate to previous months
        for i in range(1, months):
            print(f"\nAttempting to navigate to previous month {i}...")
            
            # Debug before clicking
            debug_page_state(driver, i)
            
            # Try different selectors for the previous month button
            prev_button_selectors = [
                ("dayChangeBtn-prev", "ID"),
                (".Calendar-module_dayChangeBtn__sEvC8", "CSS"),
                ("button#dayChangeBtn-prev", "CSS"),
                ("button[aria-label*='previous']", "CSS"),
                ("button[onclick*='prev']", "CSS")
            ]
            
            button_clicked = False
            for selector, selector_type in prev_button_selectors:
                try:
                    if selector_type == "ID":
                        prev_btn = driver.find_element(By.ID, selector)
                    else:
                        prev_btn = driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if prev_btn.is_displayed() and prev_btn.is_enabled():
                        # Scroll to button
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", prev_btn)
                        time.sleep(1)
                        
                        # Try normal click first
                        try:
                            prev_btn.click()
                            print(f"Successfully clicked button with {selector_type} '{selector}'")
                            button_clicked = True
                            break
                        except ElementClickInterceptedException:
                            # Try JavaScript click
                            driver.execute_script("arguments[0].click();", prev_btn)
                            print(f"Successfully JS-clicked button with {selector_type} '{selector}'")
                            button_clicked = True
                            break
                        except Exception as e:
                            print(f"Error clicking button with {selector_type} '{selector}': {e}")
                            continue
                            
                except NoSuchElementException:
                    continue
                except Exception as e:
                    print(f"Error with selector {selector_type} '{selector}': {e}")
                    continue
            
            if not button_clicked:
                error_msg = f"Could not find or click previous month button at iteration {i}"
                print(error_msg)
                results['errors'].append(error_msg)
                break
            
            # Wait for page content to refresh
            time.sleep(3)
            
            # Look for new fixtures
            old_count = len(fixture_ids)
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/matches/']"):
                href = a.get_attribute("href")
                m = re.search(r"/matches/(\d+)", href)
                if m:
                    fixture_ids.add(int(m.group(1)))
            
            new_count = len(fixture_ids)
            print(f"Found {new_count - old_count} new fixtures in month {i}")
        
        # Store results
        results['fixtures'] = sorted(list(fixture_ids))
        results['total_unique_fixtures'] = len(fixture_ids)
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(error_msg)
        results['errors'].append(error_msg)
        
    finally:
        if driver:
            driver.quit()
    
    return results


def save_results(results, filename="epl_fixtures_results.json"):
    """Save results to JSON file"""
    results['timestamp'] = datetime.now().isoformat()
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Also save just the fixture IDs to a simple text file
    with open("epl_fixture_ids.txt", 'w') as f:
        for fid in results['fixtures']:
            f.write(f"{fid}\n")
    
    print(f"\nResults saved to {filename}")
    print(f"Fixture IDs saved to epl_fixture_ids.txt")


if __name__ == "__main__":
    print("Starting EPL fixture scraper...")
    
    # Run the scraper
    results = get_fixture_ids(months=6, timeout=20, retry_attempts=3)
    
    # Print summary
    print(f"\n=== SUMMARY ===")
    print(f"Total unique fixtures found: {results['total_unique_fixtures']}")
    print(f"Number of errors: {len(results['errors'])}")
    
    if results['errors']:
        print("\nErrors encountered:")
        for error in results['errors']:
            print(f"  - {error}")
    
    # Save results
    save_results(results)
    
    # Print first 10 fixture IDs as sample
    if results['fixtures']:
        print(f"\nFirst 10 fixture IDs:")
        for fid in results['fixtures'][:10]:
            print(f"  {fid}")