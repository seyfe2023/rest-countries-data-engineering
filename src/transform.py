from typing import Dict, List, Any
import pandas as pd


def _safe_get(d: dict, path: List[str], default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur


def transform_to_dim_country(raw: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transform raw REST Countries JSON into a curated dim_country dataframe.
    """
    rows = []
    for c in raw:
        cca2 = c.get("cca2")  # 2-letter code
        name_common = _safe_get(c, ["name", "common"])
        name_official = _safe_get(c, ["name", "official"])
        region = c.get("region")
        subregion = c.get("subregion")
        capital = None
        caps = c.get("capital")
        if isinstance(caps, list) and caps:
            capital = caps[0]

        population = c.get("population")
        area = c.get("area")
        independent = c.get("independent")
        landlocked = c.get("landlocked")
        flag_png = _safe_get(c, ["flags", "png"])

        # Skip records without a country code (rare but safe)
        if not cca2:
            continue

        rows.append(
            {
                "country_code": cca2,
                "name_common": name_common,
                "name_official": name_official,
                "region": region,
                "subregion": subregion,
                "capital": capital,
                "population": population,
                "area": area,
                "independent": independent,
                "landlocked": landlocked,
                "flag_png": flag_png,
            }
        )

    df = pd.DataFrame(rows)

    # Basic type cleaning
    if not df.empty:
        df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")
        df["area"] = pd.to_numeric(df["area"], errors="coerce")
        df["country_code"] = df["country_code"].astype(str)

    return df
