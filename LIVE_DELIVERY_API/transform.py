import pandas as pd
import os
import glob
import json

# --- Configuration ---
RAW_DATA_DIR = "data/raw"
STAGED_DATA_DIR = "data/staged"
OUTPUT_FILE = os.path.join(STAGED_DATA_DIR, "air_quality_transformed.csv")

# --- Helper Functions ---

def get_aqi_category(pm2_5):
    """
    Determines AQI Category based on PM2.5 levels.
    """
    if pd.isna(pm2_5): return "Unknown"
    if pm2_5 <= 50: return "Good"
    elif pm2_5 <= 100: return "Moderate"
    elif pm2_5 <= 200: return "Unhealthy"
    elif pm2_5 <= 300: return "Very Unhealthy"
    else: return "Hazardous"

def get_risk_label(severity):
    """
    Determines Risk Label based on Severity Score.
    """
    if pd.isna(severity): return "Unknown"
    if severity > 400: return "High Risk"
    elif severity > 200: return "Moderate Risk"
    else: return "Low Risk"

def process_city_file(filepath):
    """
    Reads a raw JSON file and converts it into a Pandas DataFrame.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # 1. Flatten Hourly Data
        # Open-Meteo returns data in a structure like: {"hourly": {"time": [...], "pm10": [...]}}
        hourly_data = data.get("hourly", {})
        
        if not hourly_data:
            print(f"[WARNING] No hourly data found in {filepath}")
            return None

        df = pd.DataFrame(hourly_data)
        
        # 2. Extract City Name from Filename
        # Filename format: delhi_raw_2023...json -> extract "delhi"
        filename = os.path.basename(filepath)
        city_name = filename.split('_raw_')[0].replace('_', ' ').title()
        df['city'] = city_name
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to process {filepath}: {e}")
        return None

# --- Main Transformation Function ---

def run_transformation():
    print("Starting Data Transformation...")
    
    # 0. Ensure staged directory exists
    os.makedirs(STAGED_DATA_DIR, exist_ok=True)
    
    # 1. Load and Merge All Raw Data
    json_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.json"))
    
    if not json_files:
        print("[ERROR] No JSON files found in data/raw/. Please run extract.py first.")
        return

    data_frames = []
    for filepath in json_files:
        df = process_city_file(filepath)
        if df is not None:
            data_frames.append(df)
    
    if not data_frames:
        print("[ERROR] No valid data extracted.")
        return

    # Concatenate all city data into one DataFrame
    full_df = pd.concat(data_frames, ignore_index=True)
    
    # 2. Basic Transformations
    # Convert time to datetime objects
    full_df['time'] = pd.to_datetime(full_df['time'])
    
    # Ensure all pollutant columns are numeric (coercing errors to NaN)
    pollutants = ['pm10', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'ozone', 'sulphur_dioxide', 'uv_index']
    for col in pollutants:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

    # Remove rows where ALL pollutant readings are missing
    full_df.dropna(subset=pollutants, how='all', inplace=True)

    # 3. Feature Engineering
    
    # A. Hour of Day
    full_df['hour'] = full_df['time'].dt.hour
    
    # B. AQI Category (Apply function row-wise)
    full_df['aqi_category'] = full_df['pm2_5'].apply(get_aqi_category)
    
    # C. Pollution Severity Score (Vectorized calculation)
    # severity = (pm2_5 * 5) + (pm10 * 3) + (no2 * 4) + (so2 * 4) + (co * 2) + (o3 * 3)
    full_df['severity_score'] = (
        (full_df['pm2_5'] * 5) +
        (full_df['pm10'] * 3) +
        (full_df['nitrogen_dioxide'] * 4) +
        (full_df['sulphur_dioxide'] * 4) +
        (full_df['carbon_monoxide'] * 2) +
        (full_df['ozone'] * 3)
    )
    
    # D. Risk Classification
    full_df['risk_label'] = full_df['severity_score'].apply(get_risk_label)

    # 4. Reorder Columns for Clean Output
    final_columns = [
        'city', 'time', 'hour', 
        'pm10', 'pm2_5', 'nitrogen_dioxide', 'sulphur_dioxide', 'ozone', 'carbon_monoxide', 'uv_index',
        'aqi_category', 'severity_score', 'risk_label'
    ]
    # Filter only columns that exist (in case API changes) and are in our list
    final_df = full_df[[c for c in final_columns if c in full_df.columns]]

    # 5. Save to CSV
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n--- Transformation Summary ---")
    print(f"Total records processed: {len(final_df)}")
    print(f"Features generated: {list(final_df.columns)}")
    print(f"[SUCCESS] Transformed data saved to: {OUTPUT_FILE}")
    print("----------------------------")

if __name__ == "__main__":
    run_transformation()