import requests
import time
import json
from src.logger import get_logger
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from datetime import datetime, timezone, date
from src.config import RAW_DIR, SOURCES, SEASONS

logger = get_logger("base_fetcher")

_next_request_at: dict[str, float] = {}

def _rate_limit_wait(headers):
    """Return seconds to wait from server headers, or 60 seconds."""
    retry_after = headers.get("Retry-After", "").strip()
    if retry_after.isdecimal():
        return max(1.0, float(retry_after))
    if retry_after:
        try:
            retry_at = parsedate_to_datetime(retry_after)
            return max(1.0, retry_at.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            logger.warning("Invalid Retry-After; checking X-RateLimit-Reset")
    reset_at = headers.get("X-RateLimit-Reset", "").strip()
    if reset_at.isdecimal():
        return max(1.0, float(reset_at) - time.time() + 1)
    return 60.0

def fetch_data(
    method: str,
    url: str,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    timeout: int = 10,
    min_request_interval: float = 0.0,
    **kwargs
)-> dict | None:
    """
    Retry HTTP requests with per-URL pacing and server-directed rate-limit waits.
    max_retries is the total attempt limit. Other request failures use exponential
    backoff; HTTP 429 waits for Retry-After/X-RateLimit-Reset or a full minute.
    Pacing is shared by sequential calls in this process, not across processes.
    """
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")
    for attempt in range(1, max_retries + 1):
        rate_key = url.rstrip("/")
        wait = _next_request_at.get(rate_key, 0.0) - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _next_request_at[rate_key] = time.monotonic() + min_request_interval
        try:
            response = requests.request(
                method = method, 
                url = url, 
                timeout=timeout, 
                **kwargs)
            response.raise_for_status()
            payload =response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Expected a JSON object from {url}")
            return payload
        except requests.RequestException as e:
            response = e.response
            if response is not None and response.status_code == 429:
                sleep_time = _rate_limit_wait(response.headers)
            elif response is not None and response.status_code in (500, 502, 503, 504) or isinstance(
                e, requests.ConnectionError
            ):
                sleep_time = backoff_seconds * (2 ** (attempt - 1))
            else:
                raise
            _next_request_at[rate_key] = max(
                _next_request_at[rate_key],
                time.monotonic() + sleep_time,
            )

            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed to fetch data after {max_retries} attempts"
                ) from e

            logger.warning(
                "Attempt %d/%d failed; retrying in %.1fs",
                attempt, max_retries, sleep_time,
            )

def store_data_raw_payload(payload: dict[str, Any], path: Path ) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")

def raw_payload_path(source: str, year: int, season: str) -> Path:
    source = source.lower()
    season = season.lower()

    if source not in SOURCES:
        raise ValueError(f"Unknown source: {source}")
    if season not in SEASONS:
        raise ValueError(f"Unknown season: {season}")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    return RAW_DIR / source / str(year) / season / f"{timestamp}.json"
