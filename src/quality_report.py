import json
from datetime import datetime, timezone
from pathlib import Path

from src.config import PROJECT_ROOT
from src.logger import get_logger

logger = get_logger("quality_report")


def write_json(path: Path, value: dict) -> None:
    """Write JSON through a temporary file so readers see a complete file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")

    try:
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(value, file, indent=2, ensure_ascii=False)

        temporary.replace(path)

    finally:
        temporary.unlink(missing_ok=True)


def write_quality_report(
    result: dict,
    passed: bool,
    error: str | None = None,
) -> str:
    """Save source-record counts and the outcome of custom validation."""
    report = {
        "run_id": result["run_id"],
        "record_type": result["record_type"],
        "partition": result["partition"],
        "validation_method": "custom Python validators",
        "passed": passed,
        "error": error,
        "input_count": result["input_count"],
        "valid_count": result["valid_count"],
        "rejected_count": result["rejected_count"],
        "reconciliation_ok": (
            result["input_count"]
            == result["valid_count"] + result["rejected_count"]
        ),
        "rejected_path": result["rejected_path"],
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    path = (
        PROJECT_ROOT
        / "data"
        / "reports"
        / result["run_id"]
        / "quality.json"
    )
    write_json(path, report)

    logger.info("Saved quality report: %s", path)
    return str(path)


def write_execution_report(
    result: dict,
    quality: dict,
    load: dict,
) -> str:
    """Save a successful execution report after loading or a valid empty run."""
    if load["run_id"] != result["run_id"]:
        raise ValueError("Load report belongs to another run")

    for field in ("input_count", "valid_count", "rejected_count"):
        if quality[field] != result[field]:
            raise ValueError(f"Quality report count mismatch: {field}")

    if (
        result["rejected_count"] != 0
        or result["input_count"] != result["valid_count"]
    ):
        raise ValueError(
            "A successful report requires reconciled valid records"
        )

    expected_status = (
        "loaded" if result["valid_count"] > 0 else "no_data"
    )

    if load["status"] != expected_status:
        raise ValueError("Unexpected load status")

    if load["database_committed"] != (result["valid_count"] > 0):
        raise ValueError("Unexpected database commit status")

    report = {
        "run_id": result["run_id"],
        "status": "success",
        "record_type": result["record_type"],
        "partition": result["partition"],
        "raw_path": result["raw_path"],
        "fetched_at": result["fetched_at"],
        "validation_method": "custom Python validators",
        "source_counts": quality,
        "load": load,
        "silver_paths": result["silver_paths"],
        "rejected_path": result["rejected_path"],
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    path = (
        PROJECT_ROOT
        / "data"
        / "reports"
        / result["run_id"]
        / "execution.json"
    )
    write_json(path, report)

    logger.info("Saved execution report: %s", path)
    return str(path)


def save_checkpoint(result: dict, report_path: str) -> str:
    """Record a completed source partition using a one-slot Airflow pool."""
    with Path(report_path).open(encoding="utf-8") as file:
        report = json.load(file)

    if (
        report["run_id"] != result["run_id"]
        or report["status"] != "success"
    ):
        raise ValueError(
            "Checkpoint requires this run's successful report"
        )

    path = PROJECT_ROOT / "metadata" / "pipeline_state.json"

    state = {}
    if path.exists():
        with path.open(encoding="utf-8") as file:
            state = json.load(file)

    partitions = state.setdefault(result["record_type"], {})
    previous = partitions.get(result["partition"])

    if previous is not None:
        previous_time = datetime.fromisoformat(previous["fetched_at"])
        incoming_time = datetime.fromisoformat(result["fetched_at"])

        if previous_time > incoming_time:
            return str(path)

    partitions[result["partition"]] = {
        "run_id": result["run_id"],
        "fetched_at": result["fetched_at"],
        "raw_path": result["raw_path"],
        "report_path": report_path,
        "completed_at": report["completed_at"],
    }

    write_json(path, state)

    logger.info(
        "Saved checkpoint: %s / %s",
        result["record_type"],
        result["partition"],
    )
    return str(path)