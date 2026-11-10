import polars as pl
from sqlalchemy import create_engine, text

from src.config import DATABASE_URL
from src.logger import get_logger

logger = get_logger("load")


def execute_rows(connection, sql: str, rows: list[dict]) -> int | None:
    """Execute parameterized rows; return None when the driver has no count."""
    if not rows:
        return 0

    result = connection.execution_options(
        preserve_rowcount=True
    ).execute(text(sql), rows)

    return result.rowcount if result.rowcount >= 0 else None


def load_dimensions(connection, tables: dict) -> dict:
    """Upsert anime, genre, studio, tag, and season dimensions."""
    counts = {}

    counts["dim_anime"] = execute_rows(connection, """
        INSERT INTO gold.dim_anime (
            anilist_id, title_romaji, title_english, title_native,
            format, status, source_material, episodes, duration_min,
            is_adult, start_year, start_month, start_day, start_date,
            end_year, end_month, end_day, end_date
        )
        VALUES (
            :anilist_id, :title_romaji, :title_english, :title_native,
            :format, :status, :source_material, :episodes, :duration_min,
            :is_adult, :start_year, :start_month, :start_day, :start_date,
            :end_year, :end_month, :end_day, :end_date
        )
        ON CONFLICT (anilist_id) DO UPDATE SET
            title_romaji = EXCLUDED.title_romaji,
            title_english = EXCLUDED.title_english,
            title_native = EXCLUDED.title_native,
            format = EXCLUDED.format,
            status = EXCLUDED.status,
            source_material = EXCLUDED.source_material,
            episodes = EXCLUDED.episodes,
            duration_min = EXCLUDED.duration_min,
            is_adult = EXCLUDED.is_adult,
            start_year = EXCLUDED.start_year,
            start_month = EXCLUDED.start_month,
            start_day = EXCLUDED.start_day,
            start_date = EXCLUDED.start_date,
            end_year = EXCLUDED.end_year,
            end_month = EXCLUDED.end_month,
            end_day = EXCLUDED.end_day,
            end_date = EXCLUDED.end_date,
            updated_at = NOW()
    """, tables["anime"])

    counts["dim_genre"] = execute_rows(connection, """
        INSERT INTO gold.dim_genre (genre_name)
        VALUES (:genre_name)
        ON CONFLICT (genre_name) DO NOTHING
    """, tables["anime_genres"])

    counts["dim_studio"] = execute_rows(connection, """
        INSERT INTO gold.dim_studio (
            studio_name, is_animation_studio
        )
        VALUES (:studio_name, :is_animation_studio)
        ON CONFLICT (studio_name) DO UPDATE SET
            is_animation_studio = COALESCE(
                EXCLUDED.is_animation_studio,
                gold.dim_studio.is_animation_studio
            )
    """, tables["anime_studios"])

    counts["dim_tag"] = execute_rows(connection, """
        INSERT INTO gold.dim_tag (tag_name)
        VALUES (:tag_name)
        ON CONFLICT (tag_name) DO NOTHING
    """, tables["anime_tags"])

    seasons = []

    for row in tables["anime"]:
        if row["season"] is not None and row["season_year"] is not None:
            seasons.append({
                "season": row["season"],
                "year": row["season_year"],
            })

    counts["dim_season"] = execute_rows(connection, """
        INSERT INTO gold.dim_season (season, year)
        VALUES (:season, :year)
        ON CONFLICT (season, year) DO NOTHING
    """, seasons)

    return counts


def load_bridges(connection, tables: dict) -> dict:
    """Resolve database IDs and upsert anime relationship rows."""
    counts = {}

    counts["bridge_anime_genre"] = execute_rows(connection, """
        INSERT INTO gold.bridge_anime_genre (
            anime_id, genre_id
        )
        SELECT a.anime_id, g.genre_id
        FROM gold.dim_anime AS a
        CROSS JOIN gold.dim_genre AS g
        WHERE a.anilist_id = :anilist_id
          AND g.genre_name = :genre_name
        ON CONFLICT (anime_id, genre_id) DO NOTHING
    """, tables["anime_genres"])

    counts["bridge_anime_studio"] = execute_rows(connection, """
        INSERT INTO gold.bridge_anime_studio (
            anime_id, studio_id, is_main_studio
        )
        SELECT a.anime_id, s.studio_id, :is_main_studio
        FROM gold.dim_anime AS a
        CROSS JOIN gold.dim_studio AS s
        WHERE a.anilist_id = :anilist_id
          AND s.studio_name = :studio_name
        ON CONFLICT (anime_id, studio_id) DO UPDATE SET
            is_main_studio = COALESCE(
                EXCLUDED.is_main_studio,
                gold.bridge_anime_studio.is_main_studio
            )
    """, tables["anime_studios"])

    counts["bridge_anime_tag"] = execute_rows(connection, """
        INSERT INTO gold.bridge_anime_tag (
            anime_id, tag_id, rank
        )
        SELECT a.anime_id, t.tag_id, :rank
        FROM gold.dim_anime AS a
        CROSS JOIN gold.dim_tag AS t
        WHERE a.anilist_id = :anilist_id
          AND t.tag_name = :tag_name
        ON CONFLICT (anime_id, tag_id) DO UPDATE SET
            rank = EXCLUDED.rank
    """, tables["anime_tags"])

    return counts


def load_facts(connection, tables: dict) -> dict:
    """Upsert collection-day score snapshots and episode airing events."""
    counts = {}

    counts["fact_anime_snapshot"] = execute_rows(connection, """
        INSERT INTO gold.fact_anime_snapshot (
            anime_id, season_id, average_score, mean_score,
            popularity, favourites, snapshot_date, fetched_at
        )
        SELECT
            a.anime_id, s.season_id, :average_score, :mean_score,
            :popularity, :favourites, :snapshot_date, :fetched_at
        FROM gold.dim_anime AS a
        LEFT JOIN gold.dim_season AS s
          ON s.season = :season
         AND s.year = :season_year
        WHERE a.anilist_id = :anilist_id
        ON CONFLICT (anime_id, snapshot_date) DO UPDATE SET
            season_id = EXCLUDED.season_id,
            average_score = EXCLUDED.average_score,
            mean_score = EXCLUDED.mean_score,
            popularity = EXCLUDED.popularity,
            favourites = EXCLUDED.favourites,
            fetched_at = EXCLUDED.fetched_at,
            ingested_at = NOW()
        WHERE EXCLUDED.fetched_at > gold.fact_anime_snapshot.fetched_at
    """, tables["anime"])

    counts["fact_anime_airing"] = execute_rows(connection, """
        INSERT INTO gold.fact_anime_airing (
            airing_id, anime_id, episode, airing_at
        )
        SELECT :airing_id, a.anime_id, :episode, :airing_at
        FROM gold.dim_anime AS a
        WHERE a.anilist_id = :anilist_id
        ON CONFLICT (airing_id) DO UPDATE SET
            anime_id = EXCLUDED.anime_id,
            episode = EXCLUDED.episode,
            airing_at = EXCLUDED.airing_at
    """, tables["airings"])

    return counts


def load_to_gold(result: dict) -> dict:
    """Load quality-checked silver files in one PostgreSQL transaction."""
    if result["record_type"] not in ("seasonal", "airings"):
        raise ValueError("Unknown record_type")

    if result["rejected_count"] != 0:
        raise ValueError(
            "Rejected records must be reviewed before loading"
        )

    if result["input_count"] != result["valid_count"]:
        raise ValueError(
            "Input and valid counts must match before loading"
        )

    tables = {}

    for name in (
        "anime",
        "anime_genres",
        "anime_studios",
        "anime_tags",
        "airings",
    ):
        tables[name] = pl.read_parquet(
            result["silver_paths"][name]
        ).to_dicts()

    primary = (
        "airings" if result["record_type"] == "airings" else "anime"
    )

    if len(tables[primary]) != result["valid_count"]:
        raise ValueError(
            "Saved valid count does not match the report"
        )

    if result["valid_count"] == 0:
        logger.info(
            "run=%s: no records to load",
            result["run_id"],
        )
        return {
            "run_id": result["run_id"],
            "status": "no_data",
            "database_committed": False,
            "affected_rows": {},
        }

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not configured")

    engine = create_engine(
        DATABASE_URL,
        hide_parameters=True,
    )

    try:
        with engine.begin() as connection:
            counts = load_dimensions(connection, tables)
            counts.update(load_bridges(connection, tables))
            counts.update(load_facts(connection, tables))

    except Exception:
        logger.error(
            "run=%s: gold load failed; transaction rolled back",
            result["run_id"],
        )
        raise

    finally:
        engine.dispose()

    logger.info(
        "run=%s: gold load committed; affected_rows=%s",
        result["run_id"],
        counts,
    )

    return {
        "run_id": result["run_id"],
        "status": "loaded",
        "database_committed": True,
        "affected_rows": counts,
    }