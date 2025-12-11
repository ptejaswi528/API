import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client, Client
from dotenv import load_dotenv
import warnings

# --- Configuration ---
warnings.filterwarnings("ignore") # Clean output
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PROCESSED_DATA_DIR = "data/processed"

def fetch_data():
    print("Fetching data from Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.table("air_quality_data").select("*").execute()
    
    if not response.data:
        print("[ERROR] No data found.")
        return None
        
    df = pd.DataFrame(response.data)
    df['time'] = pd.to_datetime(df['time'])
    for col in ['pm10', 'pm2_5', 'nitrogen_dioxide', 'ozone', 'severity_score', 'hour']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def run_analysis():
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    df = fetch_data()
    if df is None: return
    print(f"Data Loaded: {len(df)} records.")

    # 🟩 A. KPI Metrics
    print("\n--- A. KPI Metrics ---")
    avg_pm25 = df.groupby('city')['pm2_5'].mean()
    worst_city = avg_pm25.idxmax()
    worst_val = avg_pm25.max()
    
    max_sev_idx = df['severity_score'].idxmax()
    sev_city = df.loc[max_sev_idx, 'city']
    sev_val = df.loc[max_sev_idx, 'severity_score']
    
    risk_pct = df['risk_flag'].value_counts(normalize=True) * 100
    worst_hour = df.groupby('hour')['pm2_5'].mean().idxmax()
    
    print(f"1. Highest Avg PM2.5: {worst_city} ({worst_val:.2f})")
    print(f"2. Highest Severity:  {sev_city} ({sev_val:.2f})")
    print(f"3. Risk Breakdown:\n{risk_pct.to_string()}")
    print(f"4. Worst AQI Hour:    {worst_hour}:00")

    # 🟩 B. City Pollution Trend Report
    print("\n--- B. City Pollution Trend Report (Preview) ---")
    # Requirement: For each city: time → pm2_5, pm10, ozone
    trends_df = df[['city', 'time', 'pm2_5', 'pm10', 'ozone']].copy()
    
    # PRINTING IT TO OUTPUT AS REQUESTED
    print(trends_df.head(10).to_string(index=False)) 
    print(f"... and {len(trends_df)-10} more rows.")

    # 🟩 C. Export Outputs
    print("\n--- C. Exporting CSVs ---")
    
    summary_data = {"Metric": ["Highest Avg PM2.5", "Highest Severity", "Worst Hour"], "Value": [worst_city, sev_city, worst_hour]}
    pd.DataFrame(summary_data).to_csv(f"{PROCESSED_DATA_DIR}/summary_metrics.csv", index=False)
    print("-> summary_metrics.csv saved.")

    df.groupby(['city', 'risk_flag']).size().reset_index(name='count').to_csv(f"{PROCESSED_DATA_DIR}/city_risk_distribution.csv", index=False)
    print("-> city_risk_distribution.csv saved.")

    trends_df.to_csv(f"{PROCESSED_DATA_DIR}/pollution_trends.csv", index=False)
    print("-> pollution_trends.csv saved.")

    # 🟩 D. Visualizations
    print("\n--- D. Generating Plots ---")
    sns.set_theme(style="whitegrid")
    
    plt.figure(figsize=(10,6))
    sns.histplot(df['pm2_5'], bins=30, kde=True, color="skyblue")
    plt.title("PM2.5 Histogram")
    plt.savefig(f"{PROCESSED_DATA_DIR}/pm25_histogram.png")
    plt.show()

    plt.figure(figsize=(10,6))
    sns.countplot(data=df, x='city', hue='risk_flag', palette="viridis")
    plt.title("Risk Flags per City")
    plt.savefig(f"{PROCESSED_DATA_DIR}/risk_flags_city.png")
    plt.show()

    plt.figure(figsize=(12,6))
    sns.lineplot(data=df, x='hour', y='pm2_5', hue='city', marker="o")
    plt.title("Hourly PM2.5 Trends")
    plt.savefig(f"{PROCESSED_DATA_DIR}/hourly_pm25_trends.png")
    plt.show()

    plt.figure(figsize=(10,6))
    sns.scatterplot(data=df, x='pm2_5', y='severity_score', hue='risk_flag', palette="deep")
    plt.title("Severity vs PM2.5")
    plt.savefig(f"{PROCESSED_DATA_DIR}/severity_vs_pm25.png")
    plt.show()
    
    print("\n[SUCCESS] Analysis Complete.")

if __name__ == "__main__":
    run_analysis()