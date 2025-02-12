import asyncio
import argparse
import time
from datetime import datetime, timezone
import logging
from sync import (
    get_db_pool,
    fetch_latest_manga_list,
    process_single_manga,
    HEADERS,
    URL,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("auto_sync.log"), logging.StreamHandler()],
)


async def get_last_update_time(pool):
    """Get the most recent update time from the database"""
    async with pool.acquire() as conn:
        query = """
        SELECT MAX(updated_at)
        FROM (
            SELECT updated_at FROM manga
            UNION ALL
            SELECT updated_at FROM chapter
        ) as updates;
        """
        return await conn.fetchval(query)


async def get_manga_update_time(manga_data):
    """Get the most recent update time from manga data"""
    try:
        updated_at = manga_data.get("updatedAt")
        if updated_at:
            return datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        return None
    except Exception:
        return None


async def auto_sync(interval_minutes: int = 30, min_pages: int = 3):
    """
    Continuously monitor and sync the database with dynamic page fetching
    :param interval_minutes: Minutes to wait between sync operations
    :param min_pages: Minimum number of pages to check in each sync operation
    """
    while True:
        try:
            start_time = time.time()
            logging.info(f"Starting sync operation at {datetime.now()}")

            # Create a new connection pool for this sync operation
            pool = await get_db_pool()
            semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

            # Get last update time from database
            last_update = await get_last_update_time(pool)
            if last_update is None:
                last_update = datetime.min.replace(tzinfo=timezone.utc)
            logging.info(f"Last database update: {last_update}")

            async with aiohttp.ClientSession() as session:
                page = 1
                consecutive_no_updates = 0
                total_updates = 0

                while True:
                    logging.info(f"Checking page {page}")

                    try:
                        manga_list = await fetch_latest_manga_list(session, page)
                        if not manga_list:
                            logging.warning(f"No manga found on page {page}")
                            break

                        manga_items = manga_list.get("mangaList", [])
                        if not manga_items:
                            break

                        # Check if any manga in this page is newer than our last update
                        page_has_updates = False
                        tasks = []

                        for manga in manga_items:
                            manga_update_time = await get_manga_update_time(manga)
                            if manga_update_time and manga_update_time > last_update:
                                page_has_updates = True
                                tasks.append(
                                    process_single_manga(manga, pool, semaphore)
                                )
                            elif manga_update_time:
                                logging.debug(
                                    f"Skipping manga {manga.get('id')}: no new updates"
                                )

                        if tasks:
                            results = await asyncio.gather(
                                *tasks, return_exceptions=True
                            )
                            successes = sum(1 for r in results if r is True)
                            failures = sum(1 for r in results if r is False)
                            total_updates += successes
                            logging.info(
                                f"Page {page} results: {successes} succeeded, {failures} failed"
                            )

                        # Update consecutive no updates counter
                        if page_has_updates:
                            consecutive_no_updates = 0
                        else:
                            consecutive_no_updates += 1

                        # Break conditions:
                        # 1. We've processed minimum pages AND found no updates in last 2 pages
                        # 2. We've processed 1 consecutive pages with no updates
                        # 3. We've reached page 100 (safety limit)
                        if (
                            (page >= min_pages and consecutive_no_updates >= 2)
                            or consecutive_no_updates >= 1
                            or page >= 100
                        ):
                            logging.info(
                                f"Stopping page fetch: reached stop condition at page {page}"
                            )
                            break

                        await asyncio.sleep(1)  # Small delay between pages
                        page += 1

                    except Exception as e:
                        logging.error(f"Error processing page {page}: {str(e)}")
                        break

            # Get updated statistics
            async with pool.acquire() as conn:
                manga_count = await conn.fetchval("SELECT COUNT(*) FROM Manga")
                chapter_count = await conn.fetchval("SELECT COUNT(*) FROM Chapter")

            # Close the pool
            await pool.close()

            # Log statistics
            elapsed_time = time.time() - start_time
            logging.info(f"Sync completed in {elapsed_time:.2f} seconds")
            logging.info(f"Pages processed: {page}")
            logging.info(f"Total updates: {total_updates}")
            logging.info(
                f"Database status: {manga_count} manga, {chapter_count} chapters"
            )

            # Calculate next sync time
            next_sync = datetime.now() + timedelta(minutes=interval_minutes)
            logging.info(f"Next sync scheduled for: {next_sync}")

            # Wait for the next interval
            await asyncio.sleep(interval_minutes * 60)

        except Exception as e:
            logging.error(f"Critical error in sync operation: {str(e)}")
            logging.info("Waiting 5 minutes before retry...")
            await asyncio.sleep(300)


async def main():
    parser = argparse.ArgumentParser(
        description="Automatic manga database synchronization"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Sync interval in minutes (default: 5)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of pages to check per sync (default: 1)",
    )
    args = parser.parse_args()

    logging.info(f"Starting auto-sync service with {args.interval} minute interval")
    logging.info(f"Checking {args.pages} pages per sync")

    try:
        await auto_sync(args.interval, args.pages)
    except KeyboardInterrupt:
        logging.info("Auto-sync service stopped by user")
    except Exception as e:
        logging.error(f"Auto-sync service crashed: {str(e)}")


if __name__ == "__main__":
    # Additional imports needed for the script to run
    import aiohttp
    from datetime import timedelta

    asyncio.run(main())
