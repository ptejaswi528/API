import requests
import json
import time
import os
from datetime import datetime
from typing import List, Dict, Any

# --- Configuration ---
API_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
POLLUTANTS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"
CITIES = {
    "Delhi": {"latitude": 28.7041, "longitude": 77.1025},
    "Mumbai": {"latitude": 19.0760, "longitude": 72.8777},
    "Bengaluru": {"latitude": 12.9716, "longitude": 77.5946},
    "Hyderabad": {"latitude": 17.3850, "longitude": 78.4867},
    "Kolkata": {"latitude": 22.5726, "longitude": 88.3639},
}
MAX_RETRIES = 3
RAW_DATA_DIR = "data/raw"

# --- Helper Function for API Call with Retries ---

def fetch_city_data(city_name: str, coords: Dict[str, float]) -> str | None:
    """
    Fetches air quality data for a specific city with retry logic.
    Returns the saved file path on success, or None on failure.
    """
    
    # 1. Prepare Request Parameters
    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "hourly": POLLUTANTS,
        "timeformat": "iso8601", # Ensures consistent time format
        "forecast_days": 1,       # Fetching current data up to 1 day is usually sufficient
    }
    
    # 2. Implement Retry Logic
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"-> Attempt {attempt} for {city_name}...")
        try:
            # 3. API Request
            response = requests.get(API_BASE_URL, params=params, timeout=15)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            
            raw_data = response.json()
            
            # 4. Success: Save the raw JSON data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{city_name.lower().replace(' ', '_')}_raw_{timestamp}.json"
            filepath = os.path.join(RAW_DATA_DIR, filename)
            
            with open(filepath, 'w') as f:
                json.dump(raw_data, f, indent=4)
            
            print(f"   [SUCCESS] Data saved to {filepath}")
            return filepath
            
        # 5. Graceful Failure Handling and Logging
        except requests.exceptions.HTTPError as e:
            # Server/Client error (e.g., 404, 500)
            print(f"   [ERROR] HTTP Error for {city_name} on attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt) # Exponential backoff: 2s, 4s
            else:
                print(f"   [FAILURE] Max retries reached for {city_name}. Skipping.")
                return None
        except requests.exceptions.ConnectionError as e:
            # Network issue (e.g., DNS failure, connection refused)
            print(f"   [ERROR] Connection Error for {city_name} on attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                print(f"   [FAILURE] Max retries reached for {city_name}. Skipping.")
                return None
        except requests.exceptions.Timeout as e:
            # Request timed out
            print(f"   [ERROR] Timeout Error for {city_name} on attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                print(f"   [FAILURE] Max retries reached for {city_name}. Skipping.")
                return None
        except Exception as e:
            # Any other unexpected error (e.g., JSON decode error, file write error)
            print(f"   [ERROR] An unexpected error occurred for {city_name}: {e}")
            return None # Fail immediately for unexpected errors

    return None # Should only be reached if all retries failed

# --- Main Extraction Function ---

def run_extraction() -> List[str]:
    """
    Main function to orchestrate the data extraction process for all cities.
    Returns a list of all successfully saved file paths.
    """
    
    # 0. Ensure the raw data directory exists
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    print(f"Starting Air Quality Data Extraction...")
    
    saved_files = []
    
    for city, coords in CITIES.items():
        filepath = fetch_city_data(city, coords)
        if filepath:
            saved_files.append(filepath)
            
    print("\n--- Extraction Summary ---")
    print(f"Total cities attempted: {len(CITIES)}")
    print(f"Total successful extractions: {len(saved_files)}")
    print("--------------------------")
    
    return saved_files

# --- Execution Block ---
if __name__ == "__main__":
    extracted_files = run_extraction()
    # print("\nList of successfully extracted files:")
    # for f in extracted_files:
    #     print(f)