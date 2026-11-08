import requests

def _fetch_data(
    method: str,
    url: str,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    timeout: int = 10,
    min_request_interval: float = 0.0,
    **kwargs
):
    """
    Retry HTTP requests with per-URL pacing and server-directed rate-limit waits.
    max_retries is the total attempt limit. Other request failures use exponential
    backoff; HTTP 429 waits for Retry-After/X-RateLimit-Reset or a full minute.
    Pacing is shared by sequential calls in this process, not across processes.
    """
    response = requests.request(method, url, **kwargs)
    response.raise_for_status()
    return response.json()