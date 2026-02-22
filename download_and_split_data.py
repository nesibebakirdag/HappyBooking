import pandas as pd
import os
import requests

# Data URL (Original dataset used in Kaggle)
DATA_URL = "https://raw.githubusercontent.com/aaqibqadeer/Hotel-booking-demand/master/hotel_bookings.csv"
DATA_DIR = "data"
RAW_FILE = os.path.join(DATA_DIR, "hotel_raw.csv")
BATCH_FILE = os.path.join(DATA_DIR, "hotel_raw_batch.csv")
STREAM_FILE = os.path.join(DATA_DIR, "hotel_raw_stream.csv")

os.makedirs(DATA_DIR, exist_ok=True)

def download_data():
    print(f"Downloading data from {DATA_URL}...")
    response = requests.get(DATA_URL)
    if response.status_code == 200:
        with open(RAW_FILE, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {RAW_FILE}")
    else:
        raise Exception(f"Failed to download data. Status code: {response.status_code}")

def split_data():
    print("Reading raw data...")
    df = pd.read_csv(RAW_FILE)
    
    # Sort by date to simulate timeline properly?
    # The dataset has 'arrival_date_year', 'arrival_date_month', 'arrival_date_day_of_month'
    # For now, we will just split by index as requested (70/30)
    
    n_rows = len(df)
    batch_size = int(n_rows * 0.7)
    
    df_batch = df.iloc[:batch_size]
    df_stream = df.iloc[batch_size:]
    
    print(f"Split data: Total {n_rows} rows.")
    print(f"Batch: {len(df_batch)} rows (First 70%)")
    print(f"Stream: {len(df_stream)} rows (Last 30%)")
    
    df_batch.to_csv(BATCH_FILE, index=False)
    df_stream.to_csv(STREAM_FILE, index=False)
    
    print(f"Saved {BATCH_FILE}")
    print(f"Saved {STREAM_FILE}")

if __name__ == "__main__":
    download_data()
    split_data()
