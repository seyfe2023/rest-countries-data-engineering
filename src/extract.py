import requests
from datetime import datetime, timezone
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Request only needed fields (smaller + more stable)
FIELDS = ",".join(
    [
        "cca2",
        "name",
        "region",
        "subregion",
        "capital",
        "population",
        "area",
        "independent",
        "landlocked",
        "flags",
    ]
)

REST_COUNTRIES_URL = f"https://restcountries.com/v3.1/all?fields={FIELDS}"


def _session_with_retries() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def fetch_countries(timeout: int = 30) -> List[Dict]:
    """
    Fetch countries from REST Countries API.
    """
    session = _session_with_retries()
    headers = {
        "Accept": "application/json",
        "User-Agent": "rest-countries-de-airflow/1.0 (+https://localhost)",
    }

    resp = session.get(REST_COUNTRIES_URL, headers=headers, timeout=timeout)

    # If still failing, show a helpful message in Airflow logs
    if resp.status_code >= 400:
        raise requests.HTTPError(
            f"HTTP {resp.status_code} for {resp.url}. "
            f"Response (first 300 chars): {resp.text[:300]}",
            response=resp,
        )

    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected API response format (expected list).")
    return data


def extract_payload() -> Dict:
    return {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "source": "restcountries",
        "data": fetch_countries(),
    }
