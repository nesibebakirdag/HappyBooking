import pandas as pd
import os

# Data Paths
DATA_DIR = "data"
SOURCE_FILE = os.path.join(DATA_DIR, "booking_dirty.csv")
BATCH_FILE = os.path.join(DATA_DIR, "hotel_raw_batch.csv")
STREAM_FILE = os.path.join(DATA_DIR, "hotel_raw_stream.csv")

def process_data():
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    print(f"Reading local data from {SOURCE_FILE}...")
    try:
        df = pd.read_csv(SOURCE_FILE)
    except Exception as e:
        print(f"Standard read failed, trying with different options: {e}")
        # Fallback options could be added here if needed
        raise e
    
    n_rows = len(df)
    batch_size = int(n_rows * 0.7)
    
    df_batch = df.iloc[:batch_size]
    df_stream = df.iloc[batch_size:]
    
    print(f"Total rows: {n_rows}")
    print(f"Batch partition (70%): {len(df_batch)} rows")
    print(f"Stream partition (30%): {len(df_stream)} rows")
    
    # Save split files
    df_batch.to_csv(BATCH_FILE, index=False)
    df_stream.to_csv(STREAM_FILE, index=False)
    
    print(f"Generated: {BATCH_FILE}")
    print(f"Generated: {STREAM_FILE}")

if __name__ == "__main__":
    process_data()
