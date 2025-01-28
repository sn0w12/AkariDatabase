from supabase import create_client

SUPABASE_URL = "your-supabase-url"
SUPABASE_KEY = "your-supabase-key"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
