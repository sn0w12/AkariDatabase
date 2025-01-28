import asyncio
import pandas as pd
from sync import (
    fetch_latest_manga_list,
    fetch_manga_and_chapters,
    export_to_csv,
    MAX_CONCURRENT_REQUESTS,
)
import aiohttp
from datetime import datetime
import os
from collections import defaultdict
from typing import List, Dict, Set, Tuple

# Add cache dictionary
manga_cache = {}

SYNC_INTERVAL = 300  # 5 minutes
CONSECUTIVE_MATCHES_THRESHOLD = 3  # Stop after N manga with all chapters present
CSV_MANGA_PATH = "exports/manga.csv"
CSV_CHAPTERS_PATH = "exports/chapters.csv"
BATCH_SIZE = 10  # Process multiple manga concurrently


async def load_existing_data():
    if not os.path.exists(CSV_MANGA_PATH) or not os.path.exists(CSV_CHAPTERS_PATH):
        return pd.DataFrame(), pd.DataFrame()

    manga_df = pd.read_csv(CSV_MANGA_PATH)
    chapters_df = pd.read_csv(CSV_CHAPTERS_PATH)
    return manga_df, chapters_df


async def fetch_manga_and_chapters_cached(manga_id: str, semaphore: asyncio.Semaphore):
    cache_key = str(manga_id)
    if cache_key not in manga_cache:
        manga_cache[cache_key] = await fetch_manga_and_chapters(manga_id, semaphore)
    return manga_cache[cache_key]


async def process_manga_batch(
    manga_batch: List[dict],
    semaphore: asyncio.Semaphore,
    existing_chapters_df: pd.DataFrame,
    existing_chapter_pairs: Set[Tuple[str, str]],
) -> List[bool]:
    tasks = [
        update_manga_and_chapters(manga, semaphore, existing_chapter_pairs)
        for manga in manga_batch
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def update_manga_and_chapters(
    manga: dict,
    semaphore: asyncio.Semaphore,
    existing_chapter_pairs: Set[Tuple[str, str]],
) -> bool:
    try:
        manga_data, chapters = await fetch_manga_and_chapters_cached(
            manga["id"], semaphore
        )

        new_chapters = [
            {**ch, "parent_id": ch["parentId"]}
            for ch in chapters
            if (str(ch.get("parentId", "")), str(ch.get("id", "")))
            not in existing_chapter_pairs
        ]

        if new_chapters:
            print(f"Adding {len(new_chapters)} new chapters for manga {manga['id']}")
            export_to_csv(manga_data, new_chapters, False)

        return True
    except Exception as e:
        print(f"Error processing manga {manga['id']}: {str(e)}")
        return False


async def get_manga_chapter_counts(chapters_df):
    """Create lookup of manga ID -> chapter count"""
    if chapters_df.empty:
        return defaultdict(int)
    return chapters_df.groupby("parent_id").size().to_dict()


async def update_data():
    # Add cache clear at the start of each update
    manga_cache.clear()
    manga_df, chapters_df = await load_existing_data()
    existing_manga_ids = set(manga_df["id"]) if not manga_df.empty else set()
    existing_chapter_counts = await get_manga_chapter_counts(chapters_df)
    existing_chapter_pairs = (
        {(str(row["parent_id"]), str(row["id"])) for _, row in chapters_df.iterrows()}
        if not chapters_df.empty
        else set()
    )

    consecutive_matches = 0
    manga_batch = []

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            try:
                manga_list = await fetch_latest_manga_list(session, page)
                if not manga_list or not manga_list.get("mangaList"):
                    break

                for manga in manga_list["mangaList"]:
                    manga_id = manga["id"]

                    if manga_id in existing_manga_ids:
                        manga_data, _ = await fetch_manga_and_chapters_cached(
                            manga_id, semaphore
                        )
                        api_chapter_count = len(manga_data["chapters"])
                        csv_chapter_count = existing_chapter_counts.get(manga_id, 0)

                        if api_chapter_count == csv_chapter_count:
                            consecutive_matches += 1
                            if consecutive_matches >= CONSECUTIVE_MATCHES_THRESHOLD:
                                if manga_batch:  # Process remaining batch
                                    await process_manga_batch(
                                        manga_batch,
                                        semaphore,
                                        chapters_df,
                                        existing_chapter_pairs,
                                    )
                                return
                            continue

                    consecutive_matches = 0
                    manga_batch.append(manga)

                    if len(manga_batch) >= BATCH_SIZE:
                        await process_manga_batch(
                            manga_batch, semaphore, chapters_df, existing_chapter_pairs
                        )
                        manga_batch = []

                page += 1

            except Exception as e:
                print(f"Error during update: {str(e)}")
                break

        if manga_batch:  # Process any remaining manga
            await process_manga_batch(
                manga_batch, semaphore, chapters_df, existing_chapter_pairs
            )


async def continuous_update():
    while True:
        print(f"\n=== Update started at {datetime.now()} ===")
        try:
            await update_data()
        except Exception as e:
            print(f"Update failed: {str(e)}")
        print(f"=== Update completed at {datetime.now()} ===")
        print(f"Waiting {SYNC_INTERVAL} seconds before next update...")
        await asyncio.sleep(SYNC_INTERVAL)


if __name__ == "__main__":
    asyncio.run(continuous_update())
