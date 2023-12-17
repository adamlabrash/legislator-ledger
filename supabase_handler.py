from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
url: str = os.environ["SUPABASE_URL"]
key: str = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(url, key)
