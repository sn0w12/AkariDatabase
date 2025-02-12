import pandas as pd
import os
from datetime import datetime


def deduplicate_data():
    print("Starting deduplication process...")

    # Read CSV files
    manga_path = "exports/manga.csv"
    chapters_path = "exports/chapters.csv"

    if not os.path.exists(manga_path) or not os.path.exists(chapters_path):
        print("CSV files not found!")
        return

    # Read data
    print("Reading CSV files...")
    manga_df = pd.read_csv(manga_path)
    chapters_df = pd.read_csv(chapters_path)

    print(f"Original manga count: {len(manga_df)}")
    print(f"Original chapters count: {len(chapters_df)}")

    # Deduplicate manga by id
    manga_df = manga_df.drop_duplicates(subset=["id"], keep="last")

    # Deduplicate chapters by parent_id and id combination
    chapters_df = chapters_df.drop_duplicates(subset=["parent_id", "id"], keep="last")

    print(f"Deduplicated manga count: {len(manga_df)}")
    print(f"Deduplicated chapters count: {len(chapters_df)}")

    # Backup original files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("exports/backup", exist_ok=True)

    if os.path.exists(manga_path):
        os.rename(manga_path, f"exports/backup/manga_{timestamp}.csv")
    if os.path.exists(chapters_path):
        os.rename(chapters_path, f"exports/backup/chapters_{timestamp}.csv")

    # Save deduplicated data
    print("Saving deduplicated data...")
    manga_df.to_csv(manga_path, index=False)
    chapters_df.to_csv(chapters_path, index=False)

    print("\nDeduplication completed!")
    print(f"Backup files saved in exports/backup/")
    print(f"Manga removed: {len(manga_df) - len(manga_df)}")
    print(f"Chapters removed: {len(chapters_df) - len(chapters_df)}")


if __name__ == "__main__":
    deduplicate_data()
