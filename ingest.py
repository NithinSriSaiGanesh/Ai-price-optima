import pandas as pd
import os
from datetime import datetime

RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"
DAILY_PATH = "data/daily_ingest"

# Ensure output folders exist
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(DAILY_PATH, exist_ok=True)

# Create today's ingestion folder
today = datetime.now().strftime("%Y-%m-%d")
today_path = os.path.join(DAILY_PATH, today)
os.makedirs(today_path, exist_ok=True)

print("Starting data ingestion pipeline...")

try:
    print("Loading raw dataset...")
    file_path = os.path.join(RAW_PATH, "dynamic_pricing_final_50000.csv")
    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    print("Cleaning data...")
    df.drop_duplicates(inplace=True)
    df.ffill(inplace=True)

    # Save cleaned data
    processed_file = os.path.join(PROCESSED_PATH, "dynamic_pricing_cleaned.csv")
    df.to_csv(processed_file, index=False)

    daily_file = os.path.join(today_path, "dynamic_pricing_cleaned.csv")
    df.to_csv(daily_file, index=False)

    print("Files saved successfully")
    print("Ingestion completed successfully")

except Exception as e:
    print("Error during ingestion:", e)