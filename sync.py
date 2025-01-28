import aiohttp
import asyncio
from typing import Dict, List, Any
import json
import csv
from datetime import datetime
import os
from uuid import uuid4
import argparse
import time  # Add this import
import os.path  # Add this to existing imports if not present
from config import supabase  # Add to imports at top

# Add these constants at the top after imports
MAX_CONCURRENT_REQUESTS = 6
CHAPTER_BATCH_SIZE = 15
REQUEST_DELAY = 0

# Add this constant after the other constants
COOKIE_VALUE = '{"user_version":"2.2","user_name":"Sn0w","user_image":"https:\/\/user.manganelo.com\/avt.png","user_data":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.lNXdfJGbnJyeiojIlBXe09lcnJCLiwWYj9Gbp9lclNXdfJGb2MzNxEjI6ICZfJGbnJCLigDNyV2c19lclNXdyEzdw42ciojI19lYsdmIsIyMc2VyX2FwaSI6IiJ9.KnKwLVkLwBR30v9OaHAHtXmaIcSqcrwekA5g-gnpW3Y"}'
HEADERS = {"Cookie": f"user_acc={COOKIE_VALUE}"}


def transform_chapters(chapters):
    for chapter in chapters:
        chapter["images"] = json.loads(chapter["images"])


async def fetch_chapter(
    session: aiohttp.ClientSession,
    manga_id: str,
    chapter_id: str,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with semaphore:
        # Explicitly include headers in the request
        async with session.get(
            f"https://akari-psi.vercel.app/api/python/{manga_id}/{chapter_id}",
            headers=HEADERS,  # Add headers here
        ) as response:
            if response.status != 200:
                raise Exception(
                    f"Failed to fetch chapter {chapter_id}: {response.status}"
                )
            await asyncio.sleep(REQUEST_DELAY)  # Add delay between requests
            return await response.json()


async def fetch_manga_and_chapters(
    manga_id: str, semaphore: asyncio.Semaphore
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # Use the HEADERS constant instead of recreating it
    async with aiohttp.ClientSession() as session:
        # Fetch manga details
        async with semaphore:
            async with session.get(
                f"https://akari-psi.vercel.app/api/python/{manga_id}",
                headers=HEADERS,  # Add headers here
            ) as response:
                if response.status != 200:
                    raise Exception(
                        f"Failed to fetch manga {manga_id}: {response.status}"
                    )
                manga_data = await response.json()
                await asyncio.sleep(REQUEST_DELAY)

        # Extract chapter IDs
        chapter_ids = [chapter["id"] for chapter in manga_data["chapters"]]

        # Process chapters in batches
        valid_chapters = []
        for i in range(0, len(chapter_ids), CHAPTER_BATCH_SIZE):
            batch = chapter_ids[i : i + CHAPTER_BATCH_SIZE]
            tasks = [
                fetch_chapter(session, manga_id, chapter_id, semaphore)
                for chapter_id in batch
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out any failed chapter fetches
            valid_chapters.extend(
                [
                    chapter
                    for chapter in batch_results
                    if not isinstance(chapter, Exception)
                ]
            )

        return manga_data, valid_chapters


async def fetch_latest_manga_list(
    session: aiohttp.ClientSession, page: int
) -> List[Dict[str, Any]]:
    async with session.get(
        f"https://akari-psi.vercel.app/api/manga-list/latest?page={page}"
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


def export_to_csv(
    manga_data: Dict[str, Any], chapters: List[Dict[str, Any]], first_write: bool
) -> None:
    timestamp = datetime.utcnow().isoformat()
    os.makedirs("exports", exist_ok=True)

    description_encoded = manga_data["description"].replace("\n", "\\n")

    # Prepare manga data for both CSV and Supabase
    manga_row = {
        "id": manga_data["id"],
        "manga_id": manga_data["mangaId"],
        "image_url": manga_data["imageUrl"],
        "titles": json.dumps(manga_data["titles"]),
        "authors": json.dumps(manga_data["authors"]),
        "genres": json.dumps(manga_data["genres"]),  # Changed to use json.dumps
        "status": manga_data["status"],
        "views": manga_data["views"],
        "score": manga_data["score"],
        "description": description_encoded,
        "created_at": timestamp,
        "updated_at": timestamp,
    }

    # Update Supabase manga table
    try:
        supabase.table("manga").upsert(manga_row).execute()
    except Exception as e:
        print(f"Error updating manga in Supabase: {str(e)}")

    manga_fields = [
        "id",
        "manga_id",
        "image_url",
        "titles",
        "authors",
        "genres",
        "status",
        "views",
        "score",
        "description",
        "created_at",
        "updated_at",
    ]

    # Write/Append manga data
    mode = "w" if first_write else "a"
    with open("exports/manga.csv", mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=manga_fields)
        if first_write:
            writer.writeheader()
        writer.writerow(manga_row)

    chapter_fields = [
        "pk_id",
        "id",
        "title",
        "images",
        "next_chapter",
        "previous_chapter",
        "parent_id",
        "created_at",
        "updated_at",
        "pages",
    ]

    # Write/Append chapter data
    mode = "w" if first_write else "a"
    with open("exports/chapters.csv", mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=chapter_fields)
        if first_write:
            writer.writeheader()
        for chapter in chapters:
            chapter_row = {
                "pk_id": str(uuid4()),
                "id": chapter["id"],
                "title": chapter["title"],
                "images": json.dumps(optimize_image_urls(chapter["images"])),
                "next_chapter": chapter["nextChapter"],
                "previous_chapter": chapter["previousChapter"],
                "parent_id": chapter["parentId"],
                "created_at": timestamp,
                "updated_at": timestamp,
                "pages": chapter["pages"],
            }

            # Update Supabase chapters table
            try:
                supabase.table("chapters").upsert(chapter_row).execute()
            except Exception as e:
                print(f"Error updating chapter in Supabase: {str(e)}")

            writer.writerow(chapter_row)


async def process_single_manga(
    manga: Dict[str, Any], first_write: bool, semaphore: asyncio.Semaphore
) -> None:
    try:
        manga_data, chapters = await fetch_manga_and_chapters(manga["id"], semaphore)
        print(
            f"Fetched manga: {manga_data['titles']['default']} with {len(chapters)} chapters"
        )
        export_to_csv(manga_data, chapters, first_write)
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

    start_time = time.time()  # Start timing
    try:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession() as session:
            page = 1
            first_manga = True
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
            for i, manga in enumerate(manga_items):
                tasks.append(
                    process_single_manga(manga, first_manga and i == 0, semaphore)
                )
            await asyncio.gather(*tasks, return_exceptions=True)
            first_manga = False

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
                    tasks.append(process_single_manga(manga, False, semaphore))

                await asyncio.gather(*tasks, return_exceptions=True)

        elapsed_time = time.time() - start_time
        pages_processed = min(page, full_total_pages)
        time_per_page = elapsed_time / pages_processed
        estimated_total_time = time_per_page * full_total_pages

        # Get file sizes and estimates
        manga_size = os.path.getsize("exports/manga.csv") / (1024 * 1024)  # Size in MB
        chapters_size = os.path.getsize("exports/chapters.csv") / (
            1024 * 1024
        )  # Size in MB
        total_size = manga_size + chapters_size

        # Calculate size per page and estimated total size
        size_per_page = total_size / pages_processed
        estimated_total_size = size_per_page * full_total_pages

        print(f"\nExported data to exports/manga.csv and exports/chapters.csv")
        print(f"Manga.csv size: {manga_size:.2f} MB")
        print(f"Chapters.csv size: {chapters_size:.2f} MB")
        print(f"Current total size: {total_size:.2f} MB")
        print(
            f"Estimated final size: {estimated_total_size:.2f} MB ({estimated_total_size/1024:.2f} GB)"
        )
        print(f"Time elapsed: {elapsed_time:.2f} seconds")
        print(f"Average time per page: {time_per_page:.2f} seconds")
        print(
            f"Estimated total time for all {full_total_pages} pages: {estimated_total_time:.2f} seconds ({estimated_total_time/60:.2f} minutes) ({estimated_total_time/3600:.2f} hours)"
        )

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
