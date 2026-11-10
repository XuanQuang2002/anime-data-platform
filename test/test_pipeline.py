import json
from datetime import date
from unittest.mock import MagicMock, Mock
import polars as pl
import pytest
from src import load, transform
from src.flatten import REQUIRED_MEDIA_FIELDS
from src.ingestion import anilist_fetcher
from src.ingestion.base_fetcher import store_data_raw_payload
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def complete_media() -> dict:
    """Return a complete query-shaped anime with legitimate unknown values."""
    media = {name: None for name in REQUIRED_MEDIA_FIELDS}
    media.update({
        "id": 42,
        "title": {
            "romaji": "Example",
            "english": None,
            "native": None,
        },
        "format": "TV",
        "status": "RELEASING",
        "genres": [],
        "studios": {"nodes": []},
        "tags": [],
    })
    return media


def raw_payload(records: list, record_type: str) -> dict:
    """Wrap test records in the same page structure as raw API snapshots."""
    field = "media" if record_type == "seasonal" else "airingSchedules"

    return {
        "fetched_at": "2026-11-10T00:05:00+07:00",
        "pages": [{
            "data": {
                "Page": {
                    "pageInfo": {"hasNextPage": False},
                    field: records,
                }
            }
        }],
    }


def test_missing_query_field_is_rejected():
    """A missing score field differs from an explicitly unknown score."""
    media = complete_media()
    del media["meanScore"]

    with pytest.raises(ValueError, match="meanScore"):
        transform.normalize_anime(media)


def test_unknown_scores_are_accepted():
    """Unrated anime remain valid when the API explicitly returns null."""
    row = transform.normalize_anime(complete_media())

    assert transform.validate_anime(row, "seasonal") == []


@pytest.mark.parametrize("score", [-1, 101, True, float("nan")])
def test_invalid_scores_are_rejected(score):
    """Reject out-of-range scores and values that are not real scores."""
    media = complete_media()
    media["averageScore"] = score
    row = transform.normalize_anime(media)

    assert any(
        "average_score" in error
        for error in transform.validate_anime(row, "seasonal")
    )


def test_partial_dates_do_not_invent_a_day():
    """Preserve a known release year while leaving the full date unknown."""
    media = complete_media()
    media["startDate"] = {
        "year": 2023,
        "month": None,
        "day": None,
    }
    row = transform.normalize_anime(media)

    assert row["start_year"] == 2023
    assert row["start_month"] is None
    assert row["start_day"] is None
    assert transform.validate_anime(row, "seasonal") == []


def test_legacy_rows_are_quarantined(tmp_path, monkeypatch):
    """Preserve rejected source rows and stop the quality gate."""
    monkeypatch.setattr(transform, "PROJECT_ROOT", tmp_path)

    legacy = {"id": 43, "title": {"romaji": "Old query"}}
    payload = raw_payload([complete_media(), legacy], "seasonal")

    source = tmp_path / "source.json"
    source.write_text(json.dumps(payload), encoding="utf-8")

    result = transform.clean_to_silver(
        str(source), "legacy_test", "seasonal"
    )

    assert result["input_count"] == 2
    assert result["valid_count"] == 1
    assert result["rejected_count"] == 1

    rejected = pl.read_parquet(result["rejected_path"]).to_dicts()

    assert json.loads(rejected[0]["raw_record_json"]) == legacy
    assert "missing requested fields" in rejected[0]["reasons"][0]

    with pytest.raises(RuntimeError, match="rejected records"):
        transform.check_quality(result)

    engine = Mock()
    monkeypatch.setattr(load, "create_engine", engine)

    with pytest.raises(ValueError, match="Rejected records"):
        load.load_to_gold(result)

    engine.assert_not_called()


def test_empty_daily_batch_needs_no_database(tmp_path, monkeypatch):
    """An empty day produces typed Parquet and an honest no-data result."""
    monkeypatch.setattr(transform, "PROJECT_ROOT", tmp_path)

    source = tmp_path / "empty.json"
    source.write_text(
        json.dumps(raw_payload([], "airings")),
        encoding="utf-8",
    )

    result = transform.clean_to_silver(
        str(source), "empty_test", "airings"
    )

    assert transform.check_quality(result)["input_count"] == 0

    engine = Mock()
    monkeypatch.setattr(load, "create_engine", engine)

    report = load.load_to_gold(result)

    assert report["status"] == "no_data"
    assert report["database_committed"] is False
    engine.assert_not_called()


def test_raw_file_cannot_be_overwritten(tmp_path):
    """Exclusive creation keeps the original raw snapshot unchanged."""
    path = tmp_path / "raw.json"
    store_data_raw_payload({"original": True}, path)

    original = path.read_bytes()

    with pytest.raises(FileExistsError):
        store_data_raw_payload({"replacement": True}, path)

    assert path.read_bytes() == original


def test_cache_and_explicit_refresh(tmp_path, monkeypatch):
    """Default fetching reuses raw; explicit refresh preserves the old file."""
    monkeypatch.setattr(anilist_fetcher, "RAW_DIR", tmp_path)

    directory = tmp_path / "anilist" / "airings"
    directory.mkdir(parents=True)

    old_timestamp = (
        datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        - timedelta(days=1)
    ).strftime("%Y%m%d_%H%M%S")

    old_path = directory / f"20261109_fetched_{old_timestamp}.json"
    old_path.write_text(
        json.dumps(raw_payload([], "airings")),
        encoding="utf-8",
    )
    original = old_path.read_bytes()

    event = {
        "id": 1001,
        "episode": 1,
        "airingAt": 1794186000,
        "media": complete_media(),
    }

    response = raw_payload([event], "airings")["pages"][0]
    api = Mock(return_value=response)
    monkeypatch.setattr(anilist_fetcher, "fetch_data", api)

    day = date(2026, 11, 9)

    assert anilist_fetcher.fetch_anilist_airings(day) == old_path
    api.assert_not_called()

    new_path = anilist_fetcher.fetch_anilist_airings(
        day, force_refresh=True
    )

    assert new_path != old_path
    assert old_path.read_bytes() == original
    assert new_path.is_file()

    assert anilist_fetcher.fetch_anilist_airings(day) == new_path
    assert api.call_count == 1


def test_load_error_is_not_swallowed(tmp_path, monkeypatch):
    """A load failure reaches the transaction context and closes the engine."""
    monkeypatch.setattr(transform, "PROJECT_ROOT", tmp_path)

    source = tmp_path / "valid.json"
    source.write_text(
        json.dumps(raw_payload([complete_media()], "seasonal")),
        encoding="utf-8",
    )

    result = transform.clean_to_silver(
        str(source), "rollback_test", "seasonal"
    )

    engine = MagicMock()
    engine.begin.return_value.__exit__.return_value = False

    monkeypatch.setattr(
        load, "DATABASE_URL", "unused-in-this-mocked-test"
    )
    monkeypatch.setattr(
        load, "create_engine", Mock(return_value=engine)
    )
    monkeypatch.setattr(
        load,
        "load_dimensions",
        Mock(side_effect=RuntimeError("simulated load failure")),
    )

    bridges = Mock()
    facts = Mock()
    monkeypatch.setattr(load, "load_bridges", bridges)
    monkeypatch.setattr(load, "load_facts", facts)

    with pytest.raises(RuntimeError, match="simulated load failure"):
        load.load_to_gold(result)

    assert (
        engine.begin.return_value.__exit__.call_args.args[0]
        is RuntimeError
    )
    engine.dispose.assert_called_once()
    bridges.assert_not_called()
    facts.assert_not_called()