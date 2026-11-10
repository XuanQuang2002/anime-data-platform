from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

REQUIRED_MEDIA_FIELDS = (
    "id", "title", "format", "status", "episodes", "duration",
    "season", "seasonYear", "averageScore", "meanScore",
    "popularity", "favourites", "genres", "source", "isAdult",
    "studios", "tags", "startDate", "endDate",
)

def flatten_anime_fields(media: dict) -> dict:
    """Extract anime fields, date components, and relationship data."""
    if not isinstance(media, dict):
        raise ValueError("media must be a JSON object")

    missing_fields = [
        name for name in REQUIRED_MEDIA_FIELDS if name not in media
    ]

    if missing_fields:
        raise ValueError(
            "Media payload is missing requested fields: "
            + ", ".join(missing_fields)
        )

    title = media.get("title")
    if title is None:
        title = {}
    if not isinstance(title, dict):
        raise ValueError("title must be a JSON object or null")

    row = {"anilist_id": media.get("id")}
    for name in ("romaji", "english", "native"):
        if name in title:
            row[f"title_{name}"] = title[name]
    field_names = {
        "format": "format",
        "status": "status",
        "source": "source_material",
        "episodes": "episodes",
        "duration": "duration_min",
        "isAdult": "is_adult",
        "season": "season",
        "seasonYear": "season_year",
        "averageScore": "average_score",
        "meanScore": "mean_score",
        "popularity": "popularity",
        "favourites": "favourites",
        "genres": "genres",
        "studios": "studios",
        "tags": "tags",
    }

    for api_name, column_name in field_names.items():
        if api_name in media:
            row[column_name] = media[api_name]

    for api_name, prefix in (
        ("startDate", "start"),
        ("endDate", "end")
    ):
        parts = media.get(api_name)
        if parts is None:
            parts = {}
        if not isinstance(parts, dict):
            raise ValueError(f"{api_name} must be a JSON object or null")

        for part in ("year", "month", "day"):
            row[f"{prefix}_{part}"] = parts.get(part)

    return row

def build_silver_tables(rows: list[dict], fetched_at: datetime) -> dict:
    """Split validated source records into flat, table-shaped row lists."""
    tables = {
        "anime": [],
        "anime_genres": [],
        "anime_studios": [],
        "anime_tags": [],
        "airings": []
    }
    seen_anime = set()

    for row in rows:
        anilist_id = row["anilist_id"]
        if "airing_id" in row:
            tables["airings"].append({
                "airing_id": row["airing_id"],
                "anilist_id": anilist_id,
                "episode": row["episode"],
                "airing_at": row["airing_at"],
            })
        if anilist_id in seen_anime:
            continue
        seen_anime.add(anilist_id)

        anime = {}
        for field in (
            "anilist_id",
            "title_romaji",
            "title_english",
            "title_native",
            "format",
            "status",
            "source_material",
            "episodes",
            "duration_min",
            "is_adult",
            "season",
            "season_year",
            "average_score",
            "mean_score",
            "popularity",
            "favourites",
        ):
            anime[field] = row.get(field)

        for prefix in ("start", "end"):
            for part in ("year", "month", "day"):
                anime[f"{prefix}_{part}"] = row.get(f"{prefix}_{part}")
            parts = [
                anime[f"{prefix}_year"],
                anime[f"{prefix}_month"],
                anime[f"{prefix}_day"]
            ]
            anime[f"{prefix}_date"] = None
            if all(value is not None for value in parts):
                anime[f"{prefix}_date"] = date(*parts)

        anime["fetched_at"] = fetched_at.astimezone(timezone.utc)
        anime["snapshot_date"] = fetched_at.astimezone(
            ZoneInfo("Asia/Ho_Chi_Minh")
        ).date()

        tables["anime"].append(anime)

        genres = {}
        for name in row.get("genres") or []:
            name = name.strip()
            genres[name] = {
                "anilist_id": anilist_id,
                "genre_name": name,
            }
        tables["anime_genres"].extend(genres.values())

        studios = {}
        connection = row.get("studios") or {}

        for studio in connection.get("nodes", []):
            name = studio.get("name").strip()
            studios[name] = {
                "anilist_id": anilist_id,
                "studio_name": name,
                "is_animation_studio": studio.get("isAnimationStudio"),
                "is_main_studio": None,
            }
        tables["anime_studios"].extend(studios.values())

        tags = {}
        for tag in row.get("tags") or []:
            name = tag["name"].strip()
            tags[name] = {
                "anilist_id": anilist_id,
                "tag_name": name,
                "rank": tag.get("rank"),
            }
        tables["anime_tags"].extend(tags.values())

    return tables

def flatten_records(raw_payload: dict, record_type: str) -> list:
    """Return AniList media or airing entries from stored pages without dropping rows."""
    if record_type not in ("seasonal", "airings"):
        raise ValueError("record_type must be 'seasonal' or 'airings'")

    if not isinstance(raw_payload, dict):
        raise ValueError("Raw payload must be a JSON object")

    pages = raw_payload.get("pages")
    if not isinstance(pages, list) or not pages:
        raise ValueError("Raw payload must contain a nonempty pages list")

    field = "media" if record_type == "seasonal" else "airingSchedules"
    records = []

    for page_number, response in enumerate(pages, start=1):
        if not isinstance(response, dict):
            raise ValueError(
                f"Raw page {page_number} must be a JSON object"
            )

        if response.get("errors"):
            raise ValueError(
                f"Raw page {page_number} contains GraphQL errors"
            )

        data = response.get("data")
        if not isinstance(data, dict):
            raise ValueError(f"Raw page {page_number} has invalid data")

        page = data.get("Page")
        if not isinstance(page, dict):
            raise ValueError(f"Raw page {page_number} is missing Page")

        entries = page.get(field)
        if not isinstance(entries, list):
            raise ValueError(
                f"Raw page {page_number}: {field} must be a list"
            )

        records.extend(entries)

    return records