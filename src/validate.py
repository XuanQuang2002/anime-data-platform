from datetime import date, datetime
from zoneinfo import ZoneInfo

MAX_INT = 2_147_483_647

def validate_anime_metadata(row: dict) -> list[str]:
    """Check optional anime metadata against the current schema rules."""
    errors = []

    choices = {
        "source_material": (
            "original", "manga", "light_novel", "visual_novel",
            "video_game", "novel", "doujinshi", "anime", "web_novel",
            "live_action", "game", "comic", "multimedia_project", "other",
        ),
        "season": ("winter", "spring", "summer", "fall"),
    }

    for field, allowed in choices.items():
        value = row.get(field)
        if value is not None:
            if not isinstance(value, str) or value not in allowed:
                errors.append(f"Invalid {field}")

    for field in ("episodes", "duration_min", "popularity", "favourites"):
        value = row.get(field)
        if value is not None:
            if type(value) is not int or not 0 <= value <= MAX_INT:
                errors.append(
                    f"{field} must be an integer between 0 and {MAX_INT}"
                )

    year = row.get("season_year")
    if year is not None:
        if type(year) is not int or not 1 <= year <= 9999:
            errors.append("season_year must be an integer between 1 and 9999")

    is_adult = row.get("is_adult")
    if is_adult is not None and type(is_adult) is not bool:
        errors.append("is_adult must be a boolean")

    return errors

def validate_anime_relations(row: dict) -> list[str]:
    """Check genre names and the studios connection preserved by normalization."""
    errors = []

    genres = row.get("genres")
    if genres is not None:
        if not isinstance(genres, list):
            errors.append("genres must be a list")
        else:
            for genre in genres:
                if not isinstance(genre, str) or not genre.strip():
                    errors.append("Each genre must be nonempty text")

    studios = row.get("studios")
    if studios is None:
        return errors

    if not isinstance(studios, dict):
        errors.append("studios must be a JSON object")
        return errors

    nodes = studios.get("nodes")
    if not isinstance(nodes, list):
        errors.append("studios.nodes must be a list")
        return errors

    for studio in nodes:
        if not isinstance(studio, dict):
            errors.append("Each studio must be a JSON object")
            continue

        name = studio.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append("Each studio needs a nonempty name")

        flag = studio.get("isAnimationStudio")
        if flag is not None and type(flag) is not bool:
            errors.append("isAnimationStudio must be a boolean")

    return errors

def validate_anime(row: dict, record_type: str) -> list[str]:
    """Return validation reasons for a normalized anime or airing record."""
    if record_type not in ("seasonal", "airings"):
        raise ValueError("Unknown record_type")

    if not isinstance(row, dict):
        return ["Record must be a JSON object"]
    
    errors = []

    anilist_id = row.get("anilist_id")
    if type(anilist_id) is not int or not 1 <= anilist_id <= MAX_INT:
        errors.append(
            f"anilist_id must be an integer between 1 and {MAX_INT}"
        )

    formats = {"tv", "tv_short", "movie", "ova", "ona", "special", "music"}
    statuses = (
        "finished", "releasing", "not_yet_released", "cancelled", "hiatus"
    )

    value = row.get("format")
    if value is not None:
        if not isinstance(value, str) or value not in formats:
            errors.append("Invalid format")

    if row.get("status") is not None and row["status"] not in statuses:
        errors.append("Invalid status")

    for field in ("title_romaji", "title_english", "title_native"):
        if row.get(field) is not None and not isinstance(row[field], str):
            errors.append(f"{field} must be text or null")

    for field in ("average_score", "mean_score"):
        value = row.get(field)
        if value is not None:
            if type(value) not in (int, float) or not 0 <= value <= 100:
                errors.append(f"{field} must be between 0 and 100")

    if record_type == "airings":
        for field in ("airing_id", "episode"):
            value = row.get(field)
            if type(value) is not int or not 1 <= value <= MAX_INT:
                errors.append(
                    f"{field} must be an integer between 1 and {MAX_INT}"
                )

        airing_at = row.get("airing_at")
        if (
            not isinstance(airing_at, datetime)
            or airing_at.utcoffset() is None
        ):
            errors.append("airing_at must be a timezone-aware datetime")
            
    errors.extend(validate_anime_metadata(row))
    errors.extend(validate_anime_relations(row))
    errors.extend(validate_anime_dates(row))
    errors.extend(validate_anime_tags(row))
    return errors

def validate_anime_dates(row: dict) -> list[str]:
    """Validate complete or partial release dates and any derived full date."""
    errors = []

    for prefix in ("start", "end"):
        values = []
        valid_parts = True

        for part, low, high in (
            ("year", 1, 9999),
            ("month", 1, 12),
            ("day", 1, 31),
        ):
            value = row.get(f"{prefix}_{part}")
            values.append(value)
            if value is not None:
                if type(value) is not int or not low <= value <= high:
                    errors.append(f"Invalid {prefix}_{part}")
                    valid_parts = False

        if not valid_parts:
            continue

        expected = None
        if all(value is not None for value in values):
            try:
                expected = date(*values)
            except ValueError:
                errors.append(f"Invalid calendar date for {prefix}")
                continue

        date_field = f"{prefix}_date"
        if date_field in row and row[date_field] != expected:
            errors.append(f"{date_field} does not match its date components")

    if "snapshot_date" in row or "fetched_at" in row:
        fetched = row.get("fetched_at")

        if not isinstance(fetched, datetime) or fetched.utcoffset() is None:
            errors.append("fetched_at must be a timezone-aware datetime")
        elif row.get("snapshot_date") != fetched.astimezone(
            ZoneInfo("Asia/Ho_Chi_Minh")
        ).date():
            errors.append(
                "snapshot_date must match the Vietnam collection date"
            )

    return errors

def validate_anime_tags(row: dict) -> list[str]:
    """Validate optional tag names and relevance ranks."""
    errors = []
    tags = row.get("tags")

    if tags is None:
        return errors
    if not isinstance(tags, list):
        return ["tags must be a list"]

    for tag in tags:
        if not isinstance(tag, dict):
            errors.append("Each tag must be a JSON object")
            continue

        name = tag.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append("Each tag needs a nonempty name")

        rank = tag.get("rank")
        if rank is not None:
            if type(rank) is not int or not 0 <= rank <= 100:
                errors.append(
                    "Tag rank must be an integer between 0 and 100"
                )

    return errors
        