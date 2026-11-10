from src.logger import get_logger
from datetime import date

from src.ingestion.anilist_fetcher import (
    fetch_anilist_data,
    fetch_anilist_airings,
)

logger = get_logger("extract")

RAW_DIR = "/opt/airflow/data/raw"
BRONZE_DIR = "/opt/airflow/data/bronze"


def extract_backfill(year: int, season: str) -> str:
    """Fetch a seasonal snapshot and return its raw file path."""
    return str(fetch_anilist_data(year, season))

def extract_daily(
    target_date: date,
    force_refresh: bool = False,
) -> str:
    """Fetch daily airings, optionally creating a fresh raw snapshot."""
    return str(
        fetch_anilist_airings(
            target_date,
            force_refresh=force_refresh,
        )
    )

