import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

from src.config import ANILIST_URL, ANILIST_RATE_LIMIT_SLEEP, RAW_DIR, SEASONS
from src.ingestion.base_fetcher import fetch_data, store_data_raw_payload
from src.logger import get_logger

logger = get_logger("anilist_fetcher")

YEARLY_QUERY = """
query ($yearPrefix: String!, $page: Int!, $perPage: Int!) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      hasNextPage
    }
    media(
      type: ANIME
      startDate_like: $yearPrefix
      sort: ID
    ) {
      id
      title { romaji english native }
      format
      status
      episodes
      duration
      season
      seasonYear
      averageScore
      meanScore
      popularity
      favourites
      genres
      source
      isAdult
      studios { nodes { name isAnimationStudio } }
      tags { name rank }
      startDate { year month day }
      endDate { year month day }
    }
  }
}
"""

SEASONAL_QUERY = """
query ($season: MediaSeason, $year: Int, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      currentPage
      lastPage
      hasNextPage       
    }
    media(season: $season, seasonYear: $year, type: ANIME) {
      id
      title { romaji english native }
      format
      status
      episodes
      duration
      season
      seasonYear
      averageScore
      meanScore
      popularity
      favourites
      genres
      source
      isAdult
      studios { nodes { name isAnimationStudio } }
      tags { name rank }
      startDate { year month day }
      endDate   { year month day }
    }
  }
}
"""

DAILY_QUERY = """
query ($after: Int!, $before: Int!, $page: Int!) {
  Page(page: $page, perPage: 50) {
    pageInfo {
      hasNextPage
    }
    airingSchedules(
      airingAt_greater: $after
      airingAt_lesser: $before
    ) {
      id
      episode
      airingAt
      media {
        id
        title { romaji english native }
        format
        status
        episodes
        duration
        season
        seasonYear
        averageScore
        meanScore
        popularity
        favourites
        genres
        source
        isAdult
        studios {
          nodes {
            name
            isAnimationStudio
          }
        }
        tags { name rank }
        startDate { year month day }
        endDate { year month day }
      }
    }
  }
}
"""

USERLIST_QUERY = """
query ($userName: String) {
  MediaListCollection(userName: $userName, type: ANIME) {
    user { id name }
    lists {
      name
      status
      entries {
        score(format: POINT_10_DECIMAL)
        status
        progress
        repeat
        notes
        startedAt  { year month day }
        completedAt { year month day }
        updatedAt
        media {
          id
          title { romaji english native }
          format
          episodes
          averageScore
          genres
          season
          seasonYear
        }
      }
    }
  }
}
"""

def fetch_anilist_data(year: int, season: str) -> Path:
    """Fetch seasonal pages once per Vietnam day and return the timestamped path."""
    season = season.strip().upper()
    if season not in SEASONS:
        raise ValueError(f"Invalid season: {season}")
    if year < 1900 or year > datetime.now().year:
        raise ValueError(f"Invalid year: {year}")
    fetched_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    today = fetched_at.strftime("%Y%m%d")
    raw_dir = RAW_DIR / "anilist" / str(year) / season.lower()
    raw_path = raw_dir / f"{today}.json"

    if not raw_path.is_file():
        cached_paths = sorted(raw_dir.glob(f"{today}_*.json"))
        if cached_paths:
            raw_path = cached_paths[0]

    if raw_path.is_file():
        with raw_path.open("r", encoding="utf-8") as file:
            json.load(file)
        logger.info(f"Using cached data from {raw_path}")
        return raw_path

    raw_path = raw_dir / f"{fetched_at.strftime('%Y%m%d_%H%M%S')}.json"
    page = 1
    pages = []
    while True:
        logger.info("Fetching AniList %s %s page %d", year, season, page)
        payload = fetch_data(
            url=ANILIST_URL, 
            method="POST",
            min_request_interval=ANILIST_RATE_LIMIT_SLEEP,
            json={
                "query": SEASONAL_QUERY, 
                "variables": {
                    "season": season, 
                    "year": year, 
                    "page": page, 
                    "perPage": 50,
                },
            },
        )
        if not payload:
            raise RuntimeError(f"Empty response from AniList on page {page}")
        if payload.get("errors"):
            raise RuntimeError(f"AniList GraphQL errors: {payload['errors']}")
        try:
            page_data = payload["data"]["Page"]
            has_next_page = page_data["pageInfo"]["hasNextPage"]
        except (KeyError, TypeError) as e:
            raise RuntimeError(
                f"Unexpected AniList response on page {page}"
            ) from e
        if not isinstance(has_next_page, bool):
            raise RuntimeError(f"Invalid hasNextPage on page {page}")

        pages.append(payload)
        if not has_next_page:
            break
        page += 1
    store_data_raw_payload(
        payload={
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "pages": pages,
        },
        path=raw_path,
    )
    return raw_path

def fetch_anilist_airings(
        target_date: date,
        force_refresh: bool = False,
    ) -> Path:
    """Fetch schedules for a date and return a path recording the Vietnam pull time."""
    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    start = datetime.combine(
        target_date,
        datetime.min.time(),
        tzinfo=tz,
    )
    end = start + timedelta(days=1)
    after = int(start.timestamp()) - 1
    before = int(end.timestamp())

    if type(force_refresh) is not bool:
        raise ValueError("force_refresh must be a boolean")
    
    target_day = target_date.strftime("%Y%m%d")
    raw_dir = RAW_DIR / "anilist" / "airings"
    prefix = f"{target_day}_"

    snapshot_paths = [
        path
        for path in raw_dir.glob(f"{prefix}*.json")
        if path.stem[len(prefix):].isdigit()
    ]
    snapshot_paths.sort(
        key=lambda path: int(path.stem[len(prefix):]),
        reverse=True,
    )

    if not force_refresh:
        cached_paths = snapshot_paths

        if not cached_paths:
            cached_paths = sorted(
                raw_dir.glob(f"{target_day}_fetched_*.json"),
                reverse=True,
            )

        legacy_path = raw_dir / f"{target_day}.json"

        if cached_paths:
            raw_path = cached_paths[0]
        elif legacy_path.is_file():
            raw_path = legacy_path
        else:
            raw_path = None

        if raw_path is not None:
            with raw_path.open("r", encoding="utf-8") as file:
                json.load(file)

            logger.info("Using cached data from %s", raw_path)
            return raw_path

    revision = 1
    if snapshot_paths:
        revision = int(snapshot_paths[0].stem[len(prefix):]) + 1

    fetched_at = datetime.now(tz)
    raw_path = raw_dir / f"{target_day}_{revision:02d}.json"

    page = 1
    pages = []
    while True:
        logger.info("Fetching AniList airings for %s page %d", target_date, page)
        payload = fetch_data(
            url=ANILIST_URL,
            method="POST",
            min_request_interval=ANILIST_RATE_LIMIT_SLEEP,
            json={
                "query": DAILY_QUERY,
                "variables": {
                    "after": after,
                    "before": before,
                    "page": page,
                },
            },
        )
        if not payload:
            raise RuntimeError(f"Empty response from AniList on page {page}")
        if payload.get("errors"):
            raise RuntimeError(f"AniList GraphQL errors: {payload['errors']}")
        try:
            page_data = payload["data"]["Page"]
            has_next_page = page_data["pageInfo"]["hasNextPage"]
        except (KeyError, TypeError) as e:
            raise RuntimeError(
                f"Unexpected AniList response on page {page}"
            ) from e
        if not isinstance(has_next_page, bool):
            raise RuntimeError(f"Invalid hasNextPage on page {page}")

        pages.append(payload)
        if not has_next_page:
            break
        page += 1
    store_data_raw_payload(
        payload={
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "pages": pages,
        },
        path=raw_path,
    )
    return raw_path

def fetch_anilist_year(year: int) -> Path:
    """Fetch anime by first-release year into one immutable yearly JSON."""
    fetched_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))

    if type(year) is not int or not (1900 <= year <= fetched_at.year):
        raise ValueError(
            f"year must be an integer between 1900 and {fetched_at.year}"
        )

    selection = "start_date_year"
    year_prefix = f"{year}%"
    raw_dir = RAW_DIR / "yearly" / str(year)
    today = fetched_at.strftime("%Y%m%d")

    cached_paths = sorted(
        raw_dir.glob(f"{today}_*.json"),
        reverse=True
    )

    if cached_paths:
        raw_path = cached_paths[0]

        with raw_path.open("r", encoding="utf-8") as file:
            cached = json.load(file)

        if (
            not isinstance(cached, dict)
            or cached.get("year") != year
            or cached.get("selection") != selection
            or not isinstance(cached.get("pages"), list)
            or not cached["pages"]
        ):
            raise ValueError(
                "Yearly cache is invalid or uses a different selection: "
                f"{raw_path}"
            )
        logger.info("Using yearly raw cache: %s", raw_path)
        return raw_path
    page = 1
    pages = []

    while True:
        logger.info(
            "Fetching AniList year=%s page=%s",
            year,
            page,
        )

        payload = fetch_data(
            method="POST",
            url=ANILIST_URL,
            min_request_interval=ANILIST_RATE_LIMIT_SLEEP,
            json={
                "query": YEARLY_QUERY,
                "variables": {
                    "yearPrefix": year_prefix,
                    "page": page,
                    "perPage": 50,
                },
            },
        )

        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Invalid AniList response on page {page}"
            )

        if payload.get("errors"):
            raise RuntimeError(
                f"AniList GraphQL errors: {payload['errors']}"
            )

        try:
            page_data = payload["data"]["Page"]
            has_next_page = page_data["pageInfo"]["hasNextPage"]
            media = page_data["media"]
        except (KeyError, TypeError) as error:
            raise RuntimeError(
                f"Unexpected AniList response on page {page}"
            ) from error

        if (
            type(has_next_page) is not bool
            or not isinstance(media, list)
        ):
            raise RuntimeError(
                f"Invalid AniList page fields on page {page}"
            )

        pages.append(media)

        if not has_next_page:
            break

        # AniList documents a maximum pagination depth of 5,000 entries.
        if page >= 100:
            raise RuntimeError(
                "Yearly extraction reached AniList's 5,000-entry limit. "
                "Split the query before treating this year as complete."
            )

        # Increment page for the next iteration
        page += 1

    raw_path = (
        raw_dir
        / f"{fetched_at.strftime('%Y%m%d_%H%M%S')}.json"
    )

    store_data_raw_payload(
        payload={
            "year": year,
            "selection": selection,
            "year_prefix": year_prefix,
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "pages": pages,
        },
        path=raw_path
    )
    logger.info(
        "Yearly extraction complete: year=%s pages=%s path=%s",
        year,
        len(pages),
        raw_path,
    )
    return raw_path

