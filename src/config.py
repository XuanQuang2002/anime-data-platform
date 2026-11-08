import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT/'.env')
ANILIST_USERNAME = os.getenv('ANILIST_USERNAME')
MAL_USERNAME = os.getenv('MAL_USERNAME')
DATABASE_URL=os.getenv('DATABASE_URL')
RAW_DIR = PROJECT_ROOT / "data" / "raw"

SOURCES = {"anilist", "mal"}
SEASONS = {"WINTER", "SPRING", "SUMMER", "FALL"}

ANILIST_URL = "https://graphql.anilist.co"
ANILIST_RATE_LIMIT_SLEEP = 2.1  