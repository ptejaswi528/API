import os
import pandas as pd
import numpy as np
import time
from supabase import create_client, Client
from dotenv import load_dotenv

# --- Configuration ---
# Create a .env file with your Supabase credentials or replace strings below
load_dotenv() 
SUPABASE_URL = os.getenv("SUPABASE_URL", "your_supabase_url_here")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "your_supabase_service_role_key_here")

INPUT_FILE = "data/staged/air_quality_transformed.csv"
TABLE_NAME = "air_quality_data"
BATCH_SIZE = 200
MAX_RETRIES = 2

# --- Initialize Supabase Client ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Helper Functions ---

def clean_data_for_json(df):
    """
    Prepares DataFrame for JSON serialization by Supabase.
    1. Converts NaN to None (which becomes SQL NULL).
    2. Converts Timestamps to ISO string format.
    3. Renames columns to match DB schema if necessary.
    """
    # 1. Rename columns to match Schema (risk_label -> risk_flag)
    if 'risk_label' in df.columns:
        df = df.rename(columns={'risk_label': 'risk_flag'})

    # 2. Convert datetime objects to string (ISO format)
    # Supabase expects ISO 8601 strings for TIMESTAMP columns
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%dT%H:%M:%S')

    # 3. Handle NaN/Infinity: Replace with None
    # 'where' replaces values where the condition is False
    df = df.replace({np.nan: None})
    
    return df

def insert_batch(batch_data, batch_index):
    """
    Inserts a single batch of data with retry logic.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            # Convert list of dicts -> Supabase insert
            response = supabase.table(TABLE_NAME).insert(batch_data).execute()
            
            # Check for data in response to confirm success
            if response.data:
                print(f"   [Batch {batch_index}] Success: {len(response.data)} rows inserted.")
                return True
            else:
                raise Exception("API returned empty data.")

        except Exception as e:
            print(f"   [Batch {batch_index}] Error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2) # Wait before retry
            else:
                print(f"   [Batch {batch_index}] FAILED after {MAX_RETRIES} retries.")
                return False

# --- Main Load Function ---

def run_loading():
    print(f"Starting Data Load to Supabase table: {TABLE_NAME}...")
    
    # 1. Read Transformed Data
    if not os.path.exists(INPUT_FILE):
        print(f"[ERROR] Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # 2. Clean Data (NaN handling, Type conversion)
    df_clean = clean_data_for_json(df)
    
    # Convert DataFrame to list of dictionaries (standard JSON format)
    records = df_clean.to_dict(orient='records')
    total_records = len(records)
    print(f"Loaded {total_records} records from CSV. Starting batch upload...")

    # 3. Batch Processing
    success_count = 0
    fail_count = 0
    
    for i in range(0, total_records, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        batch_index = (i // BATCH_SIZE) + 1
        
        if insert_batch(batch, batch_index):
            success_count += len(batch)
        else:
            fail_count += len(batch)
            
    # 4. Final Summary
    print("\n--- Load Summary ---")
    print(f"Total Rows Processed: {total_records}")
    print(f"Successfully Inserted: {success_count}")
    print(f"Failed Rows:          {fail_count}")
    print("--------------------")

if __name__ == "__main__":
    run_loading()