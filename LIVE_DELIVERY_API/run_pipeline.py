import time
import sys

# Import functions from your other scripts
# (Make sure extract.py, transform.py, load.py, etl_analysis.py are in this folder)
try:
    from extract import run_extraction
    from transform import run_transformation
    from load import run_loading
    from etl_analysis import run_analysis
except ImportError as e:
    print(f"❌ [CRITICAL ERROR] Could not import ETL modules: {e}")
    print("   Ensure extract.py, transform.py, load.py, and etl_analysis.py are in this folder.")
    sys.exit(1)

def main():
    print("===================================================")
    print("🏭  ATMOS-TRACK: AUTOMATED ETL PIPELINE STARTED    ")
    print("===================================================\n")

    start_time = time.time()

    # --- STEP 1: EXTRACT ---
    print("1️⃣  STEP 1: EXTRACT (Open-Meteo API)")
    try:
        saved_files = run_extraction()
        if not saved_files:
            raise Exception("Extraction failed (No files saved).")
        print("   ✅ Extraction Complete.\n")
        time.sleep(1) # Short pause for readability
    except Exception as e:
        print(f"   ❌ Extraction Failed: {e}")
        sys.exit(1) # Stop pipeline

    # --- STEP 2: TRANSFORM ---
    print("2️⃣  STEP 2: TRANSFORM (JSON -> CSV)")
    try:
        run_transformation()
        print("   ✅ Transformation Complete.\n")
        time.sleep(1)
    except Exception as e:
        print(f"   ❌ Transformation Failed: {e}")
        sys.exit(1)

    # --- STEP 3: LOAD ---
    print("3️⃣  STEP 3: LOAD (Supabase DB)")
    try:
        run_loading()
        print("   ✅ Loading Complete.\n")
        time.sleep(1)
    except Exception as e:
        print(f"   ❌ Loading Failed: {e}")
        sys.exit(1)

    # --- STEP 4: ANALYZE ---
    print("4️⃣  STEP 4: ANALYSIS & REPORTING")
    try:
        run_analysis()
        print("   ✅ Analysis Complete.\n")
    except Exception as e:
        print(f"   ❌ Analysis Failed: {e}")
        sys.exit(1)

    # --- FINISH ---
    elapsed = time.time() - start_time
    print("===================================================")
    print(f"🎉  PIPELINE FINISHED SUCCESSFULLY in {elapsed:.2f} seconds")
    print("===================================================")

if __name__ == "__main__":
    main()