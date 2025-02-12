import pandas as pd
from config import supabase
import time
from tqdm import tqdm
import json
from datetime import datetime


def upload_chapters():
    print("Reading chapters.csv...")
    df = pd.read_csv("exports/chapters.csv")

    # Convert string representations to proper types
    df["images"] = df["images"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    )
    df["pages"] = df["pages"].astype("Int64")  # Nullable integer

    # Convert NaN to None for optional fields
    df = df.where(pd.notnull(df), None)

    total_records = len(df)
    batch_size = 100
    retries = 3

    print(f"Found {total_records} chapters to upload")

    with tqdm(total=total_records) as pbar:
        for start in range(0, total_records, batch_size):
            end = min(start + batch_size, total_records)
            batch = df.iloc[start:end].to_dict("records")

            # Ensure proper JSON serialization
            for record in batch:
                if isinstance(record["images"], str):
                    record["images"] = json.loads(record["images"])

            for attempt in range(retries):
                try:
                    supabase.table("chapters").upsert(batch).execute()
                    break
                except Exception as e:
                    if attempt == retries - 1:
                        print(f"\nError uploading batch {start}-{end}: {str(e)}")
                        print(f"Problem record: {batch[0]}")  # Debug first record
                    else:
                        time.sleep(1)

            pbar.update(len(batch))

    print("\nUpload completed!")


if __name__ == "__main__":
    upload_chapters()
