import json
import sys
from pathlib import Path

# Ensure project root is on sys.path so we can import scraper
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from scraper import supabase

resp = supabase.table("news").select("rss_url,last_article").execute()
if isinstance(resp, dict):
    data = resp.get("data", [])
else:
    data = getattr(resp, "data", []) or []

print(json.dumps(data, default=str, ensure_ascii=False, indent=2))
