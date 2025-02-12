import aiohttp
import asyncio
from typing import Dict, List, Any
import json
import csv
from datetime import datetime, timezone  # Add timezone to imports
import os
from uuid import uuid4
import argparse
import time  # Add this import
import os.path  # Add this to existing imports if not present
import pandas as pd  # Add this import
import platform
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential
from scrape import scrape_manga_chapter
import asyncpg
from asyncpg.exceptions import UniqueViolationError
import os
from dotenv import load_dotenv

MAX_CONCURRENT_REQUESTS = 5
CHAPTER_BATCH_SIZE = 50
REQUEST_DELAY = 0
MAX_RETRIES = 5
RETRY_DELAY = 2

COOKIE_DATA = {
    "user_version": "2.2",
    "user_name": "Sn0w",
    "user_image": "https://user.manganelo.com/avt.png",
    "user_data": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.lNXdfJGbnJyeiojIlBXe09lcnJCLiwWYj9Gbp9lclNXdfJGb2MzNxEjI6ICZfJGbnJCLigDNyV2c19lclNXdyEzdw42ciojI19lYsdmIsIyMc2VyX2FwaSI6IiJ9.KnKwLVkLwBR30v9OaHAHtXmaIcSqcrwekA5g-gnpW3Y",
}
HEADERS = {"Cookie": f"user_acc={json.dumps(COOKIE_DATA)}"}
URL = "http://localhost:3000"

TIMEOUT = ClientTimeout(total=60)  # Increase from 30 to 60 seconds

if platform.system() == "Windows":
    # Use ProactorEventLoop on Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

# Database connection parameters
DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "manga_db"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


async def get_db_pool():
    """Create and return database connection pool with timezone handling"""
    return await asyncpg.create_pool(
        **DB_CONFIG,
        server_settings={"timezone": "UTC"},  # Ensure server uses UTC
    )


def normalize_datetime(dt):
    """Normalize datetime to UTC without timezone info"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None)  # Strip timezone info for PostgreSQL


def parse_datetime(date_str):
    """Convert any datetime string or object to UTC datetime without timezone info"""
    if isinstance(date_str, datetime):
        return normalize_datetime(
            date_str if date_str.tzinfo else date_str.replace(tzinfo=timezone.utc)
        )
    elif isinstance(date_str, str):
        try:
            # Try parsing ISO format
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return normalize_datetime(
                dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            )
        except ValueError:
            try:
                # Fallback: try parsing as simple datetime string
                dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                return normalize_datetime(dt.replace(tzinfo=timezone.utc))
            except ValueError:
                try:
                    # Another fallback for simpler format
                    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                    return normalize_datetime(dt.replace(tzinfo=timezone.utc))
                except ValueError:
                    return normalize_datetime(datetime.now(timezone.utc))
    return normalize_datetime(datetime.now(timezone.utc))


async def upsert_manga(pool, manga_data: Dict[str, Any]) -> None:
    """Insert or update manga record"""
    query = """
        INSERT INTO Manga (
            id, manga_id, story_data, image_url, titles, authors, genres,
            status, views, score, description, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (id) DO UPDATE SET
            manga_id = EXCLUDED.manga_id,
            story_data = EXCLUDED.story_data,
            image_url = EXCLUDED.image_url,
            titles = EXCLUDED.titles,
            authors = EXCLUDED.authors,
            genres = EXCLUDED.genres,
            status = EXCLUDED.status,
            views = EXCLUDED.views,
            score = EXCLUDED.score,
            description = EXCLUDED.description,
            updated_at = EXCLUDED.updated_at;
    """

    try:
        # Ensure timestamps are properly normalized
        default_time = datetime.now(timezone.utc)
        created_at = parse_datetime(manga_data.get("createdAt", default_time))
        updated_at = parse_datetime(manga_data.get("updatedAt", default_time))

        async with pool.acquire() as conn:
            await conn.execute(
                query,
                manga_data["id"],
                int(manga_data.get("mangaId", 0)),
                manga_data.get("storyData", ""),
                manga_data.get("imageUrl", ""),
                json.dumps(manga_data.get("titles", {})),
                manga_data.get("authors", []),
                manga_data.get("genres", []),
                manga_data.get("status", ""),
                manga_data.get("views", "0"),
                float(manga_data.get("score", 0)),
                manga_data.get("description", ""),
                created_at,  # Now normalized without timezone info
                updated_at,  # Now normalized without timezone info
            )
    except Exception as e:
        print(f"Error upserting manga {manga_data['id']}: {str(e)}")
        print(
            f"Debug info - createdAt: {manga_data.get('createdAt')}, updatedAt: {manga_data.get('updatedAt')}"
        )
        print(f"Parsed dates - created_at: {created_at}, updated_at: {updated_at}")
        raise


async def upsert_chapters(
    pool, chapters: List[Dict[str, Any]], manga_chapters_metadata: Dict[str, Any]
) -> None:
    """Insert or update chapter records"""
    query = """
        INSERT INTO Chapter (
            pk_id, id, parent_id, story_data, chapter_data, title,
            images, next_chapter, previous_chapter, views, pages,
            created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (pk_id) DO UPDATE SET
            story_data = EXCLUDED.story_data,
            chapter_data = EXCLUDED.chapter_data,
            title = EXCLUDED.title,
            images = EXCLUDED.images,
            next_chapter = EXCLUDED.next_chapter,
            previous_chapter = EXCLUDED.previous_chapter,
            views = EXCLUDED.views,
            pages = EXCLUDED.pages,
            updated_at = EXCLUDED.updated_at;
    """

    async with pool.acquire() as conn:
        async with conn.transaction():
            for chapter in chapters:
                try:
                    images = chapter.get("images", [])
                    if isinstance(images, str):
                        images = json.loads(images)

                    chapter_id = chapter.get("id", "")
                    chapter_metadata = manga_chapters_metadata.get(chapter_id, {})
                    timestamp = datetime.now(
                        timezone.utc
                    )  # Make default timestamp timezone-aware
                    created_at = parse_datetime(
                        chapter_metadata.get("createdAt", timestamp)
                    )
                    updated_at = parse_datetime(
                        chapter_metadata.get("updatedAt", timestamp)
                    )

                    await conn.execute(
                        query,
                        str(uuid4()),
                        chapter.get("id", ""),
                        chapter.get("parentId", ""),
                        chapter.get("storyData", ""),
                        chapter.get("chapterData", ""),
                        chapter.get("title", ""),
                        json.dumps(optimize_image_urls(images)),
                        chapter.get("nextChapter", ""),
                        chapter.get("previousChapter", ""),
                        chapter_metadata.get("views", "0"),
                        chapter.get("pages", 0),
                        created_at,  # Use the parsed datetime
                        updated_at,  # Use the parsed datetime
                    )
                except Exception as e:
                    print(f"Error upserting chapter {chapter.get('id', '')}: {str(e)}")


def transform_chapters(chapters):
    for chapter in chapters:
        chapter["images"] = json.loads(chapter["images"])


async def validate_response(response, chapter_id=None):
    """Validate response content and headers"""
    try:
        data = await response.json()
        if not data or (chapter_id and not data.get("images")):
            print(f"Invalid response content for {chapter_id or 'request'}")
            print(f"Response headers: {response.headers}")
            print(f"Request headers: {response.request_info.headers}")
            return False
        return True
    except Exception as e:
        print(f"Error validating response: {str(e)}")
        return False


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_DELAY, min=1, max=10),
)
async def fetch_chapter(
    session: aiohttp.ClientSession,
    manga_id: str,
    chapter_id: str,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with semaphore:
        chapter = await scrape_manga_chapter(
            manga_id, chapter_id, json.dumps(COOKIE_DATA)
        )
        print(chapter)
        await asyncio.sleep(REQUEST_DELAY)
        return chapter


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_DELAY, min=1, max=30),
)
async def fetch_manga_and_chapters(
    manga_id: str, semaphore: asyncio.Semaphore
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
            # Fetch manga details
            async with semaphore:
                request_headers = HEADERS.copy()
                try:
                    async with session.get(
                        f"{URL}/api/python/{manga_id}",
                        headers=request_headers,
                    ) as response:
                        if response.status != 200:
                            print(
                                f"Failed to fetch manga {manga_id}: Status {response.status}"
                            )
                            raise Exception(f"HTTP {response.status}")
                        manga_data = await response.json()
                        if not manga_data:
                            raise Exception("Empty manga data response")
                        await asyncio.sleep(REQUEST_DELAY)
                except asyncio.TimeoutError:
                    print(f"Timeout while fetching manga {manga_id}")
                    raise
                except Exception as e:
                    print(f"Error fetching manga {manga_id}: {str(e)}")
                    raise

            # Extract chapter IDs
            chapter_ids = [chapter["id"] for chapter in manga_data.get("chapters", [])]
            if not chapter_ids:
                print(f"No chapters found for manga {manga_id}")
                return manga_data, []

            # Process chapters in smaller batches
            valid_chapters = []
            batch_size = 10  # Reduce batch size
            for i in range(0, len(chapter_ids), batch_size):
                batch = chapter_ids[i : i + batch_size]
                tasks = []
                for chapter_id in batch:
                    task = asyncio.create_task(
                        scrape_manga_chapter(
                            manga_id, chapter_id, json.dumps(COOKIE_DATA)
                        )
                    )
                    tasks.append(task)

                try:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in batch_results:
                        if isinstance(result, Exception):
                            print(f"Chapter fetch error: {str(result)}")
                            continue
                        if result:
                            valid_chapters.append(result)
                    await asyncio.sleep(REQUEST_DELAY)
                except Exception as e:
                    print(f"Batch processing error: {str(e)}")
                    continue

            if not valid_chapters:
                print(f"No valid chapters fetched for manga {manga_id}")

            return manga_data, valid_chapters
    except Exception as e:
        print(f"Critical error in fetch_manga_and_chapters for {manga_id}: {str(e)}")
        raise


async def process_single_manga(
    manga: Dict[str, Any], pool: asyncpg.Pool, semaphore: asyncio.Semaphore
) -> None:
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            manga_data, chapters = await fetch_manga_and_chapters(
                manga["id"], semaphore
            )
            if not manga_data or not chapters:
                print(
                    f"Incomplete data for manga {manga['id']} on attempt {attempt + 1}"
                )
                if attempt < max_attempts - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return False

            print(
                f"Fetched manga: {manga_data['titles'].get('default', 'Unknown')} with {len(chapters)} chapters"
            )

            manga_chapters_metadata = {
                chapter["id"]: {
                    "views": chapter.get("views", "0"),
                    "createdAt": chapter.get(
                        "createdAt", datetime.now(timezone.utc).isoformat()
                    ),
                    "updatedAt": chapter.get(
                        "updatedAt", datetime.now(timezone.utc).isoformat()
                    ),
                }
                for chapter in manga_data.get("chapters", [])
            }

            await upsert_manga(pool, manga_data)
            if chapters:
                await upsert_chapters(pool, chapters, manga_chapters_metadata)
            return True

        except Exception as e:
            print(f"Attempt {attempt + 1} failed for manga {manga['id']}: {str(e)}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"All attempts failed for manga {manga['id']}")
                return False


async def fetch_latest_manga_list(
    session: aiohttp.ClientSession, page: int
) -> List[Dict[str, Any]]:
    async with session.get(
        f"{URL}/api/manga-list/latest?page={page}",
        headers=HEADERS,  # Add headers here
    ) as response:
        if response.status != 200:
            raise Exception(
                f"Failed to fetch manga list page {page}: {response.status}"
            )
        return await response.json()


def extract_temp_version(domain: str) -> str:
    """Extract version number from temp domain"""
    if "tempv" in domain:
        return domain.split("tempv")[1].split(".")[0]
    return ""


def parse_image_url(url: str) -> Dict[str, Any]:
    """Parse image URL into components"""
    parts = url.split("/")

    # Parse domain parts
    domain = parts[2]
    temp_ver = extract_temp_version(domain)  # e.g. 'v4' or ''
    subdomain = domain.split(".")[0]
    tab = parts[4].replace("tab_", "")

    # Get path and chapter
    path = "/".join(parts[5:-2])
    chapter = parts[-2].replace("chapter_", "")

    # Parse filename components safely
    filename = parts[-1]
    img_parts = filename.split("-")

    # Handle different formats:
    # 1-1738019084-o.jpg (3 parts)
    # 1-o.jpg (2 parts)
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
    """Optimize image URLs with version and quality"""
    if not images:
        return {}

    # Parse first URL for common data
    first_url = images[0]
    base_data = parse_image_url(first_url)

    # Group by extension
    ext_groups = {}
    for url in images:
        img_data = parse_image_url(url)
        num = int(img_data["n"])
        ext = img_data["e"]

        if ext not in ext_groups:
            ext_groups[ext] = []
        ext_groups[ext].append(num)

    # Create ranges for each extension
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


async def process_single_manga(
    manga: Dict[str, Any], pool: asyncpg.Pool, semaphore: asyncio.Semaphore
) -> None:
    try:
        manga_data, chapters = await fetch_manga_and_chapters(manga["id"], semaphore)
        print(
            f"Fetched manga: {manga_data['titles']['default']} with {len(chapters)} chapters"
        )

        # Get chapters metadata
        manga_chapters_metadata = {
            chapter["id"]: {
                "views": chapter["views"],
                "createdAt": chapter["createdAt"],
                "updatedAt": chapter["updatedAt"],
            }
            for chapter in manga_data.get("chapters", [])
        }

        await upsert_manga(pool, manga_data)
        await upsert_chapters(pool, chapters, manga_chapters_metadata)
        return True
    except Exception as e:
        print(f"Error processing manga {manga['id']}: {str(e)}")
        return False


async def main():
    parser = argparse.ArgumentParser(description="Fetch and export manga data")
    parser.add_argument(
        "--page-limit", type=int, default=1, help="Page limit (0 for no limit)"
    )
    args = parser.parse_args()

    start_time = time.time()
    try:
        pool = await get_db_pool()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async with aiohttp.ClientSession() as session:
            page = 1
            total_pages = None

            print(f"Fetching page {page}...")
            first_page = await fetch_latest_manga_list(session, page)
            if not first_page:
                print("No manga found")
                return

            full_total_pages = first_page.get("metaData", {}).get("totalPages", 1)

            # Get total pages from metadata if no limit is set
            if args.page_limit == 0:
                total_pages = first_page.get("metaData", {}).get("totalPages", 1)
                print(f"Total pages available: {total_pages}")
            else:
                total_pages = args.page_limit
                print(f"Using user-specified limit: {total_pages} pages")

            # Process first page
            tasks = []
            manga_items = first_page.get("mangaList", [])
            for manga in manga_items:
                tasks.append(process_single_manga(manga, pool, semaphore))
            await asyncio.gather(*tasks, return_exceptions=True)

            # Process remaining pages
            for page in range(2, total_pages + 1):
                print(f"Fetching page {page}/{total_pages}...")
                async with semaphore:
                    manga_list = await fetch_latest_manga_list(session, page)
                    await asyncio.sleep(REQUEST_DELAY)

                if not manga_list:
                    print("No more manga to fetch")
                    break

                tasks = []
                manga_items = manga_list.get("mangaList", [])
                for manga in manga_items:
                    tasks.append(process_single_manga(manga, pool, semaphore))

                await asyncio.gather(*tasks, return_exceptions=True)

        # Get statistics and print summary BEFORE closing the pool
        async with pool.acquire() as conn:
            manga_count = await conn.fetchval("SELECT COUNT(*) FROM Manga")
            chapter_count = await conn.fetchval("SELECT COUNT(*) FROM Chapter")

        # Close the connection pool
        await pool.close()

        # Calculate and print timing statistics
        elapsed_time = time.time() - start_time
        pages_processed = min(page, full_total_pages)
        time_per_page = elapsed_time / pages_processed
        estimated_total_time = time_per_page * full_total_pages

        # Print all statistics
        print(f"\nDatabase Statistics:")
        print(f"Total manga records: {manga_count}")
        print(f"Total chapter records: {chapter_count}")
        print(f"\nTiming Statistics:")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")
        print(f"Average time per page: {time_per_page:.2f} seconds")
        print(
            f"Estimated total time for all {full_total_pages} pages: {estimated_total_time:.2f} seconds "
            f"({estimated_total_time/60:.2f} minutes) ({estimated_total_time/3600:.2f} hours)"
        )

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
