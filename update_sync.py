import asyncio
import sys

if sys.platform.startswith("win"):
    import asyncio.windows_events

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from uuid import uuid4
from sync import (
    fetch_latest_manga_list,
    fetch_manga_and_chapters,
    export_to_csv,
    MAX_CONCURRENT_REQUESTS,
)
import aiohttp
from datetime import datetime
from typing import Any, List, Dict, Set
import csv
import os

# Add cache dictionary
manga_cache = {}

SYNC_INTERVAL = 300  # 5 minutes
CONSECUTIVE_MATCHES_THRESHOLD = 3  # Stop after N manga with all chapters present
BATCH_SIZE = 10  # Process multiple manga concurrently

# Add constants to match sync.py
COOKIE_VALUE = '{"user_version":"2.2","user_name":"Sn0w","user_image":"https:\/\/user.manganelo.com\/avt.png","user_data":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.lNXdfJGbnJyeiojIlBXe09lcnJCLiwWYj9Gbp9lclNXdfJGb2MzNxEjI6ICZfJGbnJCLigDNyV2c19lclNXdyEzdw42ciojI19lYsdmIsIyMc2VyX2FwaSI6IiJ9.KnKwLVkLwBR30v9OaHAHtXmaIcSqcrwekA5g-gnpW3Y"}'
HEADERS = {"Cookie": f"user_acc={COOKIE_VALUE}"}
CHAPTER_BATCH_SIZE = 15
REQUEST_DELAY = 0


def extract_temp_version(domain: str) -> str:
    if "tempv" in domain:
        return domain.split("tempv")[1].split(".")[0]
    return ""


def parse_image_url(url: str) -> Dict[str, Any]:
    parts = url.split("/")
    domain = parts[2]
    temp_ver = extract_temp_version(domain)
    subdomain = domain.split(".")[0]
    tab = parts[4].replace("tab_", "")
    path = "/".join(parts[5:-2])
    chapter = parts[-2].replace("chapter_", "")
    filename = parts[-1]
    img_parts = filename.split("-")
    num = img_parts[0]
    if len(img_parts) == 3:
        version = img_parts[1]
        quality, ext = img_parts[2].split(".")
    else:
        version = ""
        quality, ext = img_parts[1].split(".")
    return {
        "s": subdomain,
        "t": tab,
        "tv": temp_ver,
        "p": path,
        "c": chapter,
        "v": version,
        "q": quality,
        "e": ext,
        "n": num,
    }


def optimize_image_urls(images: List[str]) -> Dict[str, Any]:
    if not images:
        return {}
    first_url = images[0]
    base_data = parse_image_url(first_url)
    ext_groups = {}
    for url in images:
        img_data = parse_image_url(url)
        num = int(img_data["n"])
        ext = img_data["e"]
        if ext not in ext_groups:
            ext_groups[ext] = []
        ext_groups[ext].append(num)
    ranges = []
    for ext, numbers in ext_groups.items():
        numbers.sort()
        current_range = {"start": numbers[0], "end": numbers[0]}
        for num in numbers[1:]:
            if num == current_range["end"] + 1:
                current_range["end"] = num
            else:
                ranges.append(
                    {"r": f"{current_range['start']}-{current_range['end']}", "e": ext}
                )
                current_range = {"start": num, "end": num}
        ranges.append(
            {"r": f"{current_range['start']}-{current_range['end']}", "e": ext}
        )
    return {
        "s": base_data["s"],
        "t": base_data["t"],
        "tv": base_data["tv"],
        "p": base_data["p"],
        "c": base_data["c"],
        "v": base_data["v"],
        "q": base_data["q"],
        "r": ranges,
    }


# Add new helper functions for CSV operations
class CSVCache:
    def __init__(self):
        self.manga_dict = {}
        self.chapters_dict = {}
        self.is_loaded = False

    def load_data(self):
        if self.is_loaded:
            return

        if os.path.exists("exports/manga.csv"):
            with open("exports/manga.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self.manga_dict = {row["id"]: row for row in reader}

        if os.path.exists("exports/chapters.csv"):
            with open("exports/chapters.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    parent_id = row["parent_id"]
                    if parent_id not in self.chapters_dict:
                        self.chapters_dict[parent_id] = []
                    self.chapters_dict[parent_id].append(row)

        self.is_loaded = True


# Create global cache instance
csv_cache = CSVCache()


# Replace existing CSV loading functions with cached versions
async def get_manga_chapter_count(manga_id: str) -> int:
    """Get chapter count for a specific manga from CSV cache"""
    return len(csv_cache.chapters_dict.get(manga_id, []))


async def get_manga_chapters(manga_id: str) -> Set[str]:
    """Get existing chapter IDs for a specific manga from CSV cache"""
    return {str(chapter["id"]) for chapter in csv_cache.chapters_dict.get(manga_id, [])}


async def manga_exists(manga_id: str) -> bool:
    """Check if manga exists in CSV cache"""
    return manga_id in csv_cache.manga_dict


async def fetch_manga_and_chapters_cached(manga_id: str, semaphore: asyncio.Semaphore):
    cache_key = str(manga_id)
    if cache_key not in manga_cache:
        manga_cache[cache_key] = await fetch_manga_and_chapters(manga_id, semaphore)
    return manga_cache[cache_key]


async def process_manga_batch(
    manga_batch: List[dict],
    semaphore: asyncio.Semaphore,
) -> List[bool]:
    tasks = [update_manga_and_chapters(manga, semaphore) for manga in manga_batch]
    return await asyncio.gather(*tasks, return_exceptions=True)


# Modified update function to write to CSV
async def update_manga_and_chapters(
    manga: dict,
    semaphore: asyncio.Semaphore,
) -> bool:
    try:
        manga_data, chapters = await fetch_manga_and_chapters_cached(
            manga["id"], semaphore
        )

        # Get existing chapter IDs for this manga
        existing_chapters = await get_manga_chapters(manga["id"])

        # Filter new chapters
        new_chapters = [
            {**ch, "parent_id": ch["parentId"]}
            for ch in chapters
            if str(ch.get("id", "")) not in existing_chapters
        ]

        if new_chapters:
            print(f"Adding {len(new_chapters)} new chapters for manga {manga['id']}")
            export_to_csv(manga_data, new_chapters, False)

        return True

    except Exception as e:
        print(f"Error processing manga {manga['id']}: {str(e)}")
        return False


async def update_data():
    # Load CSV data once at the start
    csv_cache.load_data()
    manga_cache.clear()
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
                    exists = await manga_exists(manga_id)

                    if exists:
                        manga_data, _ = await fetch_manga_and_chapters_cached(
                            manga_id, semaphore
                        )
                        api_chapter_count = len(manga_data["chapters"])
                        db_chapter_count = await get_manga_chapter_count(manga_id)

                        if api_chapter_count == db_chapter_count:
                            consecutive_matches += 1
                            if consecutive_matches >= CONSECUTIVE_MATCHES_THRESHOLD:
                                if manga_batch:
                                    await process_manga_batch(manga_batch, semaphore)
                                return
                            continue

                    consecutive_matches = 0
                    manga_batch.append(manga)

                    if len(manga_batch) >= BATCH_SIZE:
                        await process_manga_batch(manga_batch, semaphore)
                        manga_batch = []

                page += 1

            except Exception as e:
                print(f"Error during update: {str(e)}")
                break

        if manga_batch:
            await process_manga_batch(manga_batch, semaphore)


# Remove or comment out these unused database functions
# async def update_database_manga(manga_data: Dict) -> None:
# async def update_database_chapters(chapters: List[Dict], parent_id: str) -> None:


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
    try:
        asyncio.run(continuous_update())
    except KeyboardInterrupt:
        print("\nUpdate process interrupted by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
