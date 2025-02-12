import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "manga_db"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


async def setup_database():
    """Set up the database schema"""
    try:
        print(f"Attempting to connect to database with config:")
        print(f"Host: {DB_CONFIG['host']}")
        print(f"Port: {DB_CONFIG['port']}")
        print(f"Database: {DB_CONFIG['database']}")
        print(f"User: {DB_CONFIG['user']}")

        # Connect to the database
        conn = await asyncpg.connect(**DB_CONFIG)

        # Create Manga table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Manga (
                id TEXT PRIMARY KEY,
                manga_id INT UNIQUE NOT NULL,
                story_data TEXT,
                image_url TEXT,
                titles JSONB,
                authors TEXT[],
                genres TEXT[],
                status VARCHAR(50),
                views TEXT,
                score DECIMAL(3,2) DEFAULT 0.0,
                description TEXT,
                search_vector tsvector,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create Chapter table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Chapter (
                pk_id UUID UNIQUE PRIMARY KEY,
                id TEXT NOT NULL,
                parent_id TEXT REFERENCES Manga(id) ON DELETE CASCADE,
                story_data TEXT,
                chapter_data TEXT,
                title TEXT,
                images JSONB,
                next_chapter TEXT,
                previous_chapter TEXT,
                views TEXT,
                pages INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create search trigger function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION manga_search_update_trigger() RETURNS trigger AS $$
            begin
                new.search_vector :=
                    setweight(to_tsvector('pg_catalog.english', coalesce(new.titles->>'default', '')), 'A') ||
                    setweight(to_tsvector('pg_catalog.english', coalesce(new.description, '')), 'B');
                return new;
            end
            $$ LANGUAGE plpgsql;
        """)

        # Create trigger
        await conn.execute("""
            DROP TRIGGER IF EXISTS manga_search_vector_update ON Manga;
            CREATE TRIGGER manga_search_vector_update
            BEFORE INSERT OR UPDATE ON Manga
            FOR EACH ROW
            EXECUTE FUNCTION manga_search_update_trigger();
        """)

        # Create indexes
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manga_created_at ON Manga(created_at DESC);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manga_updated_at ON Manga(updated_at DESC);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manga_views ON Manga(views);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chapter_parent_id ON Chapter(parent_id);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manga_genres ON Manga USING GIN(genres);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manga_search_vector ON Manga USING GIN(search_vector);"
        )

        print("Database setup completed successfully!")

        # Close the connection
        await conn.close()

    except asyncpg.exceptions.CannotConnectNowError:
        print("Failed to connect: PostgreSQL server is not running")
        raise
    except asyncpg.exceptions.InvalidPasswordError:
        print("Failed to connect: Invalid password")
        raise
    except asyncpg.exceptions.InvalidCatalogNameError:
        print(f"Database '{DB_CONFIG['database']}' does not exist")
        raise
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise


async def main():
    try:
        await setup_database()
    except Exception as e:
        print(f"Failed to set up database: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
