import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from src.config import PROJECT_ROOT
from src.flatten import (
    build_silver_tables,
    flatten_anime_fields,
    flatten_records,
)
from src.logger import get_logger
from src.validate import (
    validate_anime,
    validate_anime_relations,
    validate_anime_tags,
)

logger = get_logger("transform")

SILVER_SCHEMAS = {
    "anime": {
        "anilist_id": pl.Int64,
        "title_romaji": pl.String,
        "title_english": pl.String,
        "title_native": pl.String,
        "format": pl.String,
        "status": pl.String,
        "source_material": pl.String,
        "episodes": pl.Int64,
        "duration_min": pl.Int64,
        "is_adult": pl.Boolean,
        "season": pl.String,
        "season_year": pl.Int64,
        "average_score": pl.Float64,
        "mean_score": pl.Float64,
        "popularity": pl.Int64,
        "favourites": pl.Int64,
        "start_year": pl.Int64,
        "start_month": pl.Int64,
        "start_day": pl.Int64,
        "start_date": pl.Date,
        "end_year": pl.Int64,
        "end_month": pl.Int64,
        "end_day": pl.Int64,
        "end_date": pl.Date,
        "fetched_at": pl.Datetime("us", "UTC"),
        "snapshot_date": pl.Date,
    },
    "anime_genres": {
        "anilist_id": pl.Int64,
        "genre_name": pl.String,
    },
    "anime_studios": {
        "anilist_id": pl.Int64,
        "studio_name": pl.String,
        "is_animation_studio": pl.Boolean,
        "is_main_studio": pl.Boolean,
    },
    "anime_tags": {
        "anilist_id": pl.Int64,
        "tag_name": pl.String,
        "rank": pl.Int64,
    },
    "airings": {
        "airing_id": pl.Int64,
        "anilist_id": pl.Int64,
        "episode": pl.Int64,
        "airing_at": pl.Datetime("us", "UTC"),
    },
}

REJECTED_SCHEMA = {
    "source_file": pl.String,
    "row_number": pl.Int64,
    "record_type": pl.String,
    "raw_record_json": pl.String,
    "reasons": pl.List(pl.String),
}

TABLE_KEYS = {
    "anime": ("anilist_id",),
    "anime_genres": ("anilist_id", "genre_name"),
    "anime_studios": ("anilist_id", "studio_name"),
    "anime_tags": ("anilist_id", "tag_name"),
    "airings": ("airing_id",),
}

def normalize_anime(media: dict) -> dict:
    """Flatten anime fields and normalize text without inventing defaults."""
    row = flatten_anime_fields(media)

    for field in ("title_romaji", "title_english", "title_native"):
        value = row.get(field)
        if isinstance(value, str):
            row[field] = value.strip()

    for field in ("format", "status", "source_material", "season"):
        value = row.get(field)
        if isinstance(value, str):
            row[field] = value.strip().lower()

    return row

def normalize_airing(event: dict) -> dict:
    """Normalize an airing and retain its anime ID and UTC timestamp."""
    if not isinstance(event, dict):
        raise ValueError("Airing event must be a JSON object")

    media = event.get("media")
    if media is None:
        raise ValueError("Airing event must include media")
    
    row = normalize_anime(media)
    row["airing_id"] = event.get("id")
    row["episode"] = event.get("episode")

    timestamp = event.get("airingAt")
    if type(timestamp) is not int:
        raise ValueError("airingAt must be an integer Unix timestamp")

    try:
        row["airing_at"] = datetime.fromtimestamp(
            timestamp, timezone.utc
        )
    except (ValueError, OverflowError, OSError) as error:
        raise ValueError(
            "airingAt is outside the datetime range"
        ) from error

    return row

def clean_to_silver(
    raw_path: str,
    run_id: str,
    record_type: str,
) -> dict:
    """Validate one raw snapshot, write Parquet tables, and return a report."""
    if record_type not in ("seasonal", "airings"):
        raise ValueError("Unknown record_type")

    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or run_id in (".", "..")
        or Path(run_id).name != run_id
    ):
        raise ValueError("run_id must be a nonempty single folder name")

    source_path = Path(raw_path).resolve()

    with source_path.open(encoding="utf-8") as file:
        raw_payload = json.load(file)

    records = flatten_records(raw_payload, record_type)

    fetched_at = datetime.fromisoformat(raw_payload["fetched_at"])
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a timezone")

    valid_rows = []
    rejected_rows = []
    seen = set()

    key_field = (
        "airing_id" if record_type == "airings" else "anilist_id"
    )

    for number, record in enumerate(records, start=1):
        try:
            if record_type == "seasonal":
                row = normalize_anime(record)
            else:
                row = normalize_airing(record)
        except ValueError as error:
            rejected_rows.append({
            "source_file": str(source_path),
            "row_number": number,
            "record_type": record_type,
            "raw_record_json": json.dumps(
                record, ensure_ascii=False
            ),
            "reasons": [str(error)],
            })
            continue

        errors = validate_anime(row, record_type)

        if not errors and row[key_field] in seen:
            errors.append(
                f"Duplicate {key_field}: {row[key_field]}"
            )

        if errors:
            rejected_rows.append({
                "source_file": str(source_path),
                "row_number": number,
                "record_type": record_type,
                "raw_record_json": json.dumps(
                    record, ensure_ascii=False
                ),
                "reasons": errors,
            })
        else:
            seen.add(row[key_field])
            valid_rows.append(row)

    if len(records) != len(valid_rows) + len(rejected_rows):
        raise RuntimeError("Reconciliation failed")

    tables = build_silver_tables(valid_rows, fetched_at)

    frames = {}
    for name, schema in SILVER_SCHEMAS.items():
        frames[name] = pl.DataFrame(tables[name], schema=schema)

    rejected_frame = pl.DataFrame(
        rejected_rows,
        schema=REJECTED_SCHEMA,
    )

    # A retry gets a new directory, preserving previous outputs.
    attempt = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )
    silver_dir = (
        PROJECT_ROOT / "data" / "silver" / run_id / attempt
    )
    rejected_dir = (
        PROJECT_ROOT / "data" / "rejected" / run_id / attempt
    )

    rejected_dir.mkdir(parents=True, exist_ok=False)
    silver_dir.mkdir(parents=True, exist_ok=False)

    rejected_path = rejected_dir / "rejected.parquet"
    rejected_frame.write_parquet(rejected_path)

    silver_paths = {}

    for name, frame in frames.items():
        path = silver_dir / f"{name}.parquet"
        frame.write_parquet(path)
        silver_paths[name] = str(path)

    logger.info(
        "run=%s input=%d valid=%d rejected=%d",
        run_id,
        len(records),
        len(valid_rows),
        len(rejected_rows),
    )

    return {
        "run_id": run_id,
        "record_type": record_type,
        "fetched_at": fetched_at.isoformat(),
        "input_count": len(records),
        "valid_count": len(valid_rows),
        "rejected_count": len(rejected_rows),
        "silver_paths": silver_paths,
        "rejected_path": str(rejected_path),
    }

def check_quality(result: dict) -> dict:
    """Check saved schemas, keys, references, and record reconciliation."""
    record_type = result["record_type"]

    if record_type not in ("seasonal", "airings"):
        raise ValueError("Unknown record_type")

    for field in ("input_count", "valid_count", "rejected_count"):
        if type(result[field]) is not int or result[field] < 0:
            raise ValueError(f"Invalid {field}")

    frames = {}

    for name, schema in SILVER_SCHEMAS.items():
        frame = pl.read_parquet(result["silver_paths"][name])

        if frame.schema != schema:
            raise RuntimeError(f"Unexpected schema: {name}")

        frames[name] = frame

    rejected = pl.read_parquet(result["rejected_path"])

    if rejected.schema != REJECTED_SCHEMA:
        raise RuntimeError("Unexpected rejected schema")

    primary = (
        "airings" if record_type == "airings" else "anime"
    )
    valid_count = frames[primary].height

    if valid_count != result["valid_count"]:
        raise RuntimeError(
            "Saved valid count does not match the report"
        )

    if rejected.height != result["rejected_count"]:
        raise RuntimeError(
            "Saved rejected count does not match the report"
        )

    if result["input_count"] != valid_count + rejected.height:
        raise RuntimeError(
            "Reconciliation failed: input != valid + rejected"
        )

    anime_ids = set(
        frames["anime"]["anilist_id"].to_list()
    )

    for name, frame in frames.items():
        seen = set()

        for row in frame.to_dicts():
            if name == "anime":
                errors = validate_anime(row, "seasonal")

            elif name == "airings":
                errors = validate_anime(row, "airings")

            elif name == "anime_genres":
                errors = validate_anime_relations({
                    "genres": [row["genre_name"]]
                })

            elif name == "anime_studios":
                errors = validate_anime_relations({
                    "studios": {
                        "nodes": [{
                            "name": row["studio_name"],
                            "isAnimationStudio": (
                                row["is_animation_studio"]
                            ),
                        }]
                    }
                })

            else:
                errors = validate_anime_tags({
                    "tags": [{
                        "name": row["tag_name"],
                        "rank": row["rank"],
                    }]
                })

            if errors:
                raise RuntimeError(
                    f"Invalid {name} row: {errors}"
                )

            key = tuple(
                row[field] for field in TABLE_KEYS[name]
            )

            if key in seen:
                raise RuntimeError(
                    f"Duplicate key in {name}: {key}"
                )
            seen.add(key)

            if (
                name != "anime"
                and row["anilist_id"] not in anime_ids
            ):
                raise RuntimeError(
                    f"Unknown anime reference in {name}"
                )

    if rejected.height:
        raise RuntimeError(
            f"{rejected.height} rejected records need review"
        )

    logger.info(
        "Quality checks passed: input=%d valid=%d",
        result["input_count"],
        valid_count,
    )

    return {
        "input_count": result["input_count"],
        "valid_count": valid_count,
        "rejected_count": rejected.height,
    }