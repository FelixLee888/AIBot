#!/usr/bin/env python3
"""Adaptive Scotland mountain weather briefing with daily source benchmarking.

Daily run behavior:
1) Store one-day-ahead forecasts from multiple sources.
2) Store yesterday actuals (reference: Open-Meteo archive daily values).
3) Benchmark each source against actuals and update confidence.
4) Recompute source weights for ensemble forecast logic.
5) Print concise Telegram-ready briefing for tomorrow.

Optional API keys / credentials:
- OPENWEATHER_API_KEY (OpenWeather One Call 3.0 / Forecast endpoints)
- GOOGLE_WEATHER_API_KEY (Google Weather API forecast.days.lookup endpoint)
- GOOGLE_WEATHER_ACCESS_TOKEN (preferred for Google Weather OAuth2)

Met Office (non-atmospheric) forecast metrics are scraped from public Met Office
forecast UI pages (no API key required).
"""

from __future__ import annotations

import argparse
import datetime as dt
import base64
import csv
import html
import json
import math
import os
import re
import sqlite3
import subprocess
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import requests

DEFAULT_LOCATIONS = [
    {"name": "Glencoe", "lat": 56.68, "lon": -5.10},
    {"name": "Ben Nevis", "lat": 56.7969, "lon": -5.0036},
    {"name": "Glenshee", "lat": 56.8526, "lon": -3.4258},
    {"name": "Cairngorms", "lat": 57.1, "lon": -3.7},
]
LOCATIONS = [dict(loc) for loc in DEFAULT_LOCATIONS]

TZ = ZoneInfo("Europe/London")


def resolve_data_dir() -> Path:
    override = os.getenv("WEATHER_BENCHMARK_DATA_DIR", "").strip()
    candidates: List[Path] = []
    if override:
        candidates.append(Path(override))

    candidates.extend([
        Path("/home/felixlee/Desktop/aibot/data"),
        Path.home() / "Desktop/aibot/data",
        Path(__file__).resolve().parent.parent / "data",
    ])

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except Exception:
            continue

    fallback = Path("./data")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATA_DIR = resolve_data_dir()
DB_PATH = DATA_DIR / "weather_benchmark.sqlite3"


def resolve_memory_root() -> Path:
    override = os.getenv("AIBOT_MEMORY_ROOT", "").strip()
    candidates: List[Path] = []
    if override:
        candidates.append(Path(override))

    candidates.extend([
        DATA_DIR.parent,
        Path("/home/felixlee/Desktop/aibot"),
        Path.home() / "Desktop/aibot",
        Path(__file__).resolve().parent.parent,
    ])

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except Exception:
            continue

    fallback = Path(__file__).resolve().parent.parent
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


MEMORY_ROOT = resolve_memory_root()
MEMORY_DIR = MEMORY_ROOT / "memory"
HEARTBEAT_STATE_PATH = MEMORY_DIR / "heartbeat-state.json"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Scottish mountain weather briefings in full or compact form.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "compact"),
        default="full",
        help="Select output style (default: full)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Shortcut for --mode compact",
    )
    parser.add_argument(
        "--add-city",
        default="",
        help="Add a city/location into Google Sheet weather_watchlist and exit.",
    )
    parser.add_argument(
        "--city-country",
        default="",
        help="Optional ISO-2 country code used for geocoding (e.g., GB).",
    )
    return parser.parse_args()
SOURCE_OPEN_METEO = "open_meteo"
SOURCE_MET_NO = "met_no"
SOURCE_MET_OFFICE = "met_office"
SOURCE_MET_OFFICE_ATMOSPHERIC = "met_office_atmospheric"
SOURCE_OPENWEATHER = "openweather"
SOURCE_GOOGLE_WEATHER = "google_weather"

SOURCE_LABELS = {
    SOURCE_OPEN_METEO: "Open-Meteo",
    SOURCE_MET_NO: "MET Norway",
    SOURCE_MET_OFFICE: "UK Met Office",
    SOURCE_MET_OFFICE_ATMOSPHERIC: "UK Met Office (Atmospheric Models)",
    SOURCE_OPENWEATHER: "OpenWeather",
    SOURCE_GOOGLE_WEATHER: "Google Weather",
}
RETIRED_SOURCES = (SOURCE_MET_OFFICE_ATMOSPHERIC,)

OPENMETEO_FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1/archive"
MET_NO_BASE = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
MET_NO_USER_AGENT = "AIBot-WeatherBenchmark/1.0 (felixlee@example.com)"

ENV_FALLBACK_FILES = [
    Path(__file__).resolve().parent.parent / ".env",
    Path("/home/felixlee/.openclaw/.env"),
    Path("/home/felixlee/Desktop/aibot/.env"),
    Path("/home/felixlee/Desktop/YuenYuenWeatherSite/.env"),
    Path("/home/felixlee/Desktop/chief-fafa/.env"),
    Path.home() / ".openclaw/.env",
    Path.home() / "Desktop/aibot/.env",
    Path.home() / "Desktop/YuenYuenWeatherSite/.env",
    Path.home() / "Desktop/chief-fafa/.env",
]


def read_env_value(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value

    for env_file in ENV_FALLBACK_FILES:
        try:
            exists = env_file.exists()
        except OSError:
            continue
        if not exists:
            continue
        try:
            for raw_line in env_file.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, raw_val = line.split("=", 1)
                if key.strip() != name:
                    continue
                cleaned = raw_val.strip().strip("'\"")
                if cleaned:
                    return cleaned
        except Exception:
            continue

    return default


def read_int_env(name: str, default: int) -> int:
    raw = read_env_value(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def read_bool_env(name: str, default: bool) -> bool:
    raw = read_env_value(name, "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


# Legacy key is still read so existing environments can continue to use it as a
# fallback for METOFFICE_ATMOS_API_KEY.
METOFFICE_API_KEY = read_env_value("METOFFICE_API_KEY", "")
METOFFICE_UI_FORECAST_BASE = read_env_value("METOFFICE_UI_FORECAST_BASE", "https://weather.metoffice.gov.uk/forecast").strip() or "https://weather.metoffice.gov.uk/forecast"
METOFFICE_UI_GEOHASH_PRECISION = max(7, min(12, read_int_env("METOFFICE_UI_GEOHASH_PRECISION", 9)))
METOFFICE_UI_ENABLED = read_env_value("METOFFICE_UI_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
METOFFICE_ATMOSPHERIC_BASE = "https://data.hub.api.metoffice.gov.uk/atmospheric-models/1.0.0"
METOFFICE_ATMOS_API_KEY = read_env_value("METOFFICE_ATMOS_API_KEY", METOFFICE_API_KEY)
METOFFICE_ATMOS_ORDER_ID = read_env_value("METOFFICE_ATMOS_ORDER_ID", "").strip()
METOFFICE_ATMOS_MAX_FILES = max(1, read_int_env("METOFFICE_ATMOS_MAX_FILES", 8))
METOFFICE_ATMOS_MAX_FILE_MB = max(5, read_int_env("METOFFICE_ATMOS_MAX_FILE_MB", 150))

# OpenWeather endpoints. `OPENWEATHER_MODE=auto` tries One Call 3.0 first, then forecast 2.5 fallback.
OPENWEATHER_ONECALL_BASE = "https://api.openweathermap.org/data/3.0/onecall"
OPENWEATHER_FORECAST_BASE = "https://api.openweathermap.org/data/2.5/forecast"
OPENWEATHER_API_KEY = read_env_value("OPENWEATHER_API_KEY", "")
OPENWEATHER_MODE = read_env_value("OPENWEATHER_MODE", "auto").strip().lower() or "auto"

# Google Weather API daily forecast endpoint.
GOOGLE_WEATHER_BASE = "https://weather.googleapis.com/v1/forecast/days:lookup"
GOOGLE_WEATHER_API_KEY = read_env_value("GOOGLE_WEATHER_API_KEY", "")
GOOGLE_WEATHER_ACCESS_TOKEN = read_env_value("GOOGLE_WEATHER_ACCESS_TOKEN", "")
GOOGLE_WEATHER_UNITS_SYSTEM = read_env_value("GOOGLE_WEATHER_UNITS_SYSTEM", "METRIC").strip().upper() or "METRIC"
GOOGLE_WEATHER_LANGUAGE_CODE = read_env_value("GOOGLE_WEATHER_LANGUAGE_CODE", "en-GB").strip() or "en-GB"
GOOGLE_WEATHER_QUOTA_PROJECT = (
    read_env_value("GOOGLE_WEATHER_QUOTA_PROJECT", "")
    or read_env_value("GOOGLE_CLOUD_PROJECT", "")
    or read_env_value("GCLOUD_PROJECT", "")
    or read_env_value("GOOGLE_PROJECT_ID", "")
)

WATCHLIST_SPREADSHEET_URL_DEFAULT = "https://docs.google.com/spreadsheets/d/1g9_1I1xyt7iO922yNXckPswnqV5ATIzLo3NQ6IJ4O5k/edit?usp=sharing"
WATCHLIST_SPREADSHEET_URL = read_env_value("WEATHER_WATCHLIST_SPREADSHEET_URL", WATCHLIST_SPREADSHEET_URL_DEFAULT).strip() or WATCHLIST_SPREADSHEET_URL_DEFAULT
WATCHLIST_SPREADSHEET_ID = (
    read_env_value("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    or read_env_value("WEATHER_WATCHLIST_SPREADSHEET_ID", "").strip()
)
WATCHLIST_WORKSHEET = (
    read_env_value("GOOGLE_SHEETS_WATCHLIST_WORKSHEET", "").strip()
    or read_env_value("WEATHER_WATCHLIST_WORKSHEET", "weather_watchlist").strip()
    or "weather_watchlist"
)
GOOGLE_SHEETS_API_KEY = read_env_value("GOOGLE_SHEETS_API_KEY", "").strip()
GOOGLE_SHEETS_ACCESS_TOKEN = (
    read_env_value("GOOGLE_OAUTH_ACCESS_TOKEN", "").strip()
    or read_env_value("GOOGLE_SHEETS_ACCESS_TOKEN", "").strip()
)
OPENMETEO_GEOCODING_BASE = "https://geocoding-api.open-meteo.com/v1/search"

RUNTIME_SOURCE_NOTES: Dict[str, str] = {}

LOOKBACK_DAYS = 14
REQUEST_TIMEOUT = 20
DEFAULT_NONE_METRICS = {
    "temp_max": None,
    "temp_min": None,
    "wind_max": None,
    "rain_chance": None,
    "wind_dir": None,
}
METOFFICE_ATMOS_CACHE: Dict[str, Dict[Tuple[float, float], Dict[str, Optional[float]]]] = {}
METOFFICE_ATMOS_CACHE_DIR = DATA_DIR / "metoffice_atmos_grib"

WEATHER_SITE_SYNC_ENABLED = read_bool_env("WEATHER_SITE_SYNC_ENABLED", True)
WEATHER_SITE_REPO_PATH = read_env_value("WEATHER_SITE_REPO_PATH", "").strip()
WEATHER_SITE_REPO_URL = read_env_value("WEATHER_SITE_REPO_URL", "https://github.com/FelixLee888/YuenYuenWeatherSite.git").strip()
WEATHER_SITE_GIT_REMOTE = read_env_value("WEATHER_SITE_GIT_REMOTE", "origin").strip() or "origin"
WEATHER_SITE_GIT_BRANCH = read_env_value("WEATHER_SITE_GIT_BRANCH", "main").strip() or "main"
WEATHER_SITE_DATA_SUBDIR = read_env_value("WEATHER_SITE_DATA_SUBDIR", "public/data").strip().strip("/") or "public/data"
WEATHER_SITE_HISTORY_DAYS = max(7, read_int_env("WEATHER_SITE_HISTORY_DAYS", 30))
WEATHER_SITE_GIT_PUSH_ENABLED = read_bool_env("WEATHER_SITE_GIT_PUSH_ENABLED", True)


def extract_google_sheet_id(sheet_url: str) -> str:
    text = str(sheet_url or "").strip()
    if not text:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", text)
    return m.group(1) if m else ""


def parse_bool_cell(value: object, default: bool = True) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def parse_header_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def watchlist_sheet_id() -> str:
    explicit = WATCHLIST_SPREADSHEET_ID.strip()
    if explicit:
        return explicit
    from_url = extract_google_sheet_id(WATCHLIST_SPREADSHEET_URL)
    if from_url:
        return from_url
    return extract_google_sheet_id(WATCHLIST_SPREADSHEET_URL_DEFAULT)


def parse_watchlist_csv_rows(text: str) -> List[List[str]]:
    if not text:
        return []
    rows: List[List[str]] = []
    reader = csv.reader(StringIO(text))
    for row in reader:
        rows.append([str(cell or "").strip() for cell in row])
    return rows


def fetch_watchlist_rows_from_sheets_api(spreadsheet_id: str, worksheet: str) -> List[List[str]]:
    if not spreadsheet_id:
        return []
    if not GOOGLE_SHEETS_API_KEY and not GOOGLE_SHEETS_ACCESS_TOKEN:
        return []

    range_name = f"{worksheet}!A:Z"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{quote(range_name, safe='!')}"
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if GOOGLE_SHEETS_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {GOOGLE_SHEETS_ACCESS_TOKEN}"
    if GOOGLE_SHEETS_API_KEY:
        params["key"] = GOOGLE_SHEETS_API_KEY
    resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        return []
    payload = resp.json() if resp.content else {}
    values = payload.get("values")
    if not isinstance(values, list):
        return []

    out: List[List[str]] = []
    for row in values:
        if not isinstance(row, list):
            continue
        out.append([str(cell or "").strip() for cell in row])
    return out


def fetch_watchlist_rows_from_public_csv(spreadsheet_id: str, worksheet: str) -> List[List[str]]:
    if not spreadsheet_id:
        return []
    urls = [
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={quote(worksheet)}",
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&sheet={quote(worksheet)}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        except Exception:
            continue
        if resp.status_code >= 400:
            continue
        rows = parse_watchlist_csv_rows(resp.text)
        if rows:
            return rows
    return []


def fetch_watchlist_rows() -> List[List[str]]:
    spreadsheet_id = watchlist_sheet_id()
    worksheet = WATCHLIST_WORKSHEET
    rows = fetch_watchlist_rows_from_sheets_api(spreadsheet_id, worksheet)
    if rows:
        return rows
    return fetch_watchlist_rows_from_public_csv(spreadsheet_id, worksheet)


def parse_google_api_error(response: requests.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            msg = str(err.get("message") or "").strip()
            if msg:
                return msg
        msg2 = str(payload.get("message") or "").strip()
        if msg2:
            return msg2
    text = (response.text or "").strip()
    return text[:400] if text else f"HTTP {response.status_code}"


def watchlist_has_header(rows: Sequence[Sequence[object]]) -> bool:
    if not rows:
        return False
    first = [parse_header_key(x) for x in rows[0]]
    return any(
        key in {"city", "name", "location", "zone", "place", "latitude", "lat", "longitude", "lon", "lng"}
        for key in first
    )


def watchlist_find_index(header_keys: Sequence[str], candidates: Sequence[str]) -> int:
    for idx, key in enumerate(header_keys):
        if key in candidates:
            return idx
    return -1


def extract_watchlist_location_names(rows: Sequence[Sequence[object]]) -> List[str]:
    if not rows:
        return []
    parsed_rows = [[str(cell or "").strip() for cell in row] for row in rows]
    parsed_rows = [row for row in parsed_rows if any(cell for cell in row)]
    if not parsed_rows:
        return []
    has_header = watchlist_has_header(parsed_rows)
    header_keys = [parse_header_key(x) for x in parsed_rows[0]] if has_header else []
    start_idx = 1 if has_header else 0
    name_idx = watchlist_find_index(header_keys, ("city", "name", "location", "zone", "place")) if has_header else 0
    names: List[str] = []
    seen: set[str] = set()
    for row in parsed_rows[start_idx:]:
        candidate = row[name_idx].strip() if 0 <= name_idx < len(row) else ""
        if not candidate:
            for cell in row:
                if cell.strip():
                    candidate = cell.strip()
                    break
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(candidate)
    return names


def resolve_watchlist_write_token() -> str:
    for key in (
        "GOOGLE_SHEETS_ACCESS_TOKEN",
        "GOOGLE_OAUTH_ACCESS_TOKEN",
    ):
        value = read_env_value(key, "").strip()
        if value:
            return value
    return ""


def fetch_watchlist_rows_via_token(
    spreadsheet_id: str,
    worksheet: str,
    access_token: str,
) -> List[List[str]]:
    range_name = f"{worksheet}!A:Z"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{quote(range_name, safe='!')}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        raise RuntimeError(parse_google_api_error(resp))
    payload = resp.json() if resp.content else {}
    values = payload.get("values")
    if not isinstance(values, list):
        return []
    out: List[List[str]] = []
    for row in values:
        if isinstance(row, list):
            out.append([str(cell or "").strip() for cell in row])
    return out


def append_watchlist_rows_via_token(
    spreadsheet_id: str,
    worksheet: str,
    access_token: str,
    values: List[List[str]],
) -> None:
    if not values:
        return
    range_name = f"{worksheet}!A:Z"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{quote(range_name, safe='!')}:append"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    params = {
        "valueInputOption": "USER_ENTERED",
        "insertDataOption": "INSERT_ROWS",
    }
    payload = {
        "majorDimension": "ROWS",
        "values": values,
    }
    resp = requests.post(url, headers=headers, params=params, json=payload, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        raise RuntimeError(parse_google_api_error(resp))


def build_watchlist_append_rows(
    city_name: str,
    country_code: str,
    existing_rows: Sequence[Sequence[object]],
) -> Tuple[List[List[str]], str]:
    now_utc = utc_now_iso()
    city = str(city_name or "").strip()
    country = str(country_code or "").strip().upper()[:2]
    geocoded = geocode_location(city, country=country)
    lat = to_float((geocoded or {}).get("lat"))
    lon = to_float((geocoded or {}).get("lon"))
    resolved_name = str((geocoded or {}).get("name") or city).strip() or city

    rows = [[str(cell or "").strip() for cell in row] for row in existing_rows]
    rows = [row for row in rows if any(cell for cell in row)]
    if not rows:
        header = ["location_order", "location", "latitude", "longitude", "enabled", "updated_at_utc"]
        row = [
            "1",
            resolved_name,
            f"{lat:.6f}" if lat is not None else "",
            f"{lon:.6f}" if lon is not None else "",
            "TRUE",
            now_utc,
        ]
        return [header, row], resolved_name

    if watchlist_has_header(rows):
        header = rows[0]
        header_keys = [parse_header_key(x) for x in header]
        row = [""] * len(header)

        name_idx = watchlist_find_index(header_keys, ("city", "name", "location", "zone", "place"))
        lat_idx = watchlist_find_index(header_keys, ("latitude", "lat"))
        lon_idx = watchlist_find_index(header_keys, ("longitude", "lon", "lng", "long"))
        country_idx = watchlist_find_index(header_keys, ("country", "countrycode", "cc", "iso2"))
        enabled_idx = watchlist_find_index(header_keys, ("enabled", "active", "include", "use"))
        updated_idx = watchlist_find_index(header_keys, ("updatedatutc", "updated", "updatedat", "lastupdated"))
        order_idx = watchlist_find_index(header_keys, ("locationorder", "order", "seq", "sequence", "id"))

        if 0 <= name_idx < len(row):
            row[name_idx] = resolved_name
        elif row:
            row[0] = resolved_name

        if 0 <= lat_idx < len(row):
            row[lat_idx] = f"{lat:.6f}" if lat is not None else ""
        if 0 <= lon_idx < len(row):
            row[lon_idx] = f"{lon:.6f}" if lon is not None else ""
        if 0 <= country_idx < len(row):
            row[country_idx] = country
        if 0 <= enabled_idx < len(row):
            row[enabled_idx] = "TRUE"
        if 0 <= updated_idx < len(row):
            row[updated_idx] = now_utc
        if 0 <= order_idx < len(row):
            max_order = 0
            for src_row in rows[1:]:
                if order_idx >= len(src_row):
                    continue
                iv = int(to_float(src_row[order_idx]) or 0)
                if iv > max_order:
                    max_order = iv
            row[order_idx] = str(max_order + 1 if max_order > 0 else len(rows))
        return [row], resolved_name

    row = [
        resolved_name,
        f"{lat:.6f}" if lat is not None else "",
        f"{lon:.6f}" if lon is not None else "",
    ]
    return [row], resolved_name


def add_city_to_watchlist_sheet(city_name: str, country_code: str = "") -> Tuple[bool, str]:
    city = str(city_name or "").strip()
    if not city:
        return False, "add-city failed: city is empty"
    country = str(country_code or "").strip().upper()[:2]

    if not country:
        options = ambiguous_city_country_options(city)
        if options:
            hint = ", ".join(options[:6])
            return (
                False,
                f"add-city needs country for ambiguous city '{city}'. "
                f"Please retry with --city-country <ISO2>. Options: {hint}",
            )

    spreadsheet_id = watchlist_sheet_id()
    worksheet = WATCHLIST_WORKSHEET
    if not spreadsheet_id:
        return False, "add-city failed: watchlist spreadsheet id not configured"

    access_token = resolve_watchlist_write_token()
    if not access_token:
        return False, "add-city failed: missing GOOGLE_OAUTH_ACCESS_TOKEN/GOOGLE_SHEETS_ACCESS_TOKEN for Google Sheets write"

    try:
        rows = fetch_watchlist_rows_via_token(spreadsheet_id=spreadsheet_id, worksheet=worksheet, access_token=access_token)
    except Exception as exc:
        return False, f"add-city failed: unable to read watchlist sheet ({exc})"

    existing_names = {name.strip().lower() for name in extract_watchlist_location_names(rows)}
    if city.lower() in existing_names:
        return True, f"watchlist unchanged: '{city}' already exists in sheet '{worksheet}'"

    append_rows, resolved_name = build_watchlist_append_rows(city_name=city, country_code=country, existing_rows=rows)
    try:
        append_watchlist_rows_via_token(
            spreadsheet_id=spreadsheet_id,
            worksheet=worksheet,
            access_token=access_token,
            values=append_rows,
        )
    except Exception as exc:
        return False, f"add-city failed: unable to append city to sheet ({exc})"

    return True, f"watchlist updated: added '{resolved_name}' to sheet '{worksheet}'"


def geocode_location_candidates(name: str, country: str = "", count: int = 8) -> List[Dict[str, object]]:
    text = str(name or "").strip()
    if not text:
        return []
    params = {
        "name": text,
        "count": max(1, min(20, int(count))),
        "language": "en",
        "format": "json",
    }
    if country:
        params["countryCode"] = str(country).strip()[:2].upper()
    try:
        resp = requests.get(OPENMETEO_GEOCODING_BASE, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
    except Exception:
        return []
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return []
    out: List[Dict[str, object]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        lat = to_float(item.get("latitude"))
        lon = to_float(item.get("longitude"))
        if lat is None or lon is None:
            continue
        out.append(
            {
                "name": str(item.get("name") or text).strip() or text,
                "lat": lat,
                "lon": lon,
                "country": str(item.get("country") or "").strip(),
                "country_code": str(item.get("country_code") or "").strip().upper(),
                "admin1": str(item.get("admin1") or "").strip(),
            }
        )
    return out


def geocode_location(name: str, country: str = "") -> Optional[Dict[str, float]]:
    candidates = geocode_location_candidates(name=name, country=country, count=1)
    if not candidates:
        return None
    first = candidates[0]
    lat = to_float(first.get("lat"))
    lon = to_float(first.get("lon"))
    if lat is None or lon is None:
        return None
    resolved_name = str(first.get("name") or str(name or "").strip()).strip() or str(name or "").strip()
    return {"name": resolved_name, "lat": lat, "lon": lon}


def ambiguous_city_country_options(city_name: str) -> List[str]:
    city = str(city_name or "").strip()
    if not city:
        return []
    candidates = geocode_location_candidates(name=city, country="", count=12)
    if not candidates:
        return []
    target = normalize_key(city)
    exact_matches = [c for c in candidates if normalize_key(str(c.get("name") or "")) == target]
    if len(exact_matches) < 2:
        return []

    options: List[str] = []
    seen: set[str] = set()
    for item in exact_matches:
        code = str(item.get("country_code") or "").strip().upper()
        country = str(item.get("country") or "").strip()
        if not code:
            continue
        key = code
        if key in seen:
            continue
        seen.add(key)
        label = f"{code} ({country})" if country else code
        options.append(label)

    return options if len(options) > 1 else []


def parse_watchlist_locations(rows: Sequence[Sequence[object]]) -> List[Dict[str, float]]:
    if not rows:
        return []

    parsed_rows = [[str(cell or "").strip() for cell in row] for row in rows]
    parsed_rows = [row for row in parsed_rows if any(cell for cell in row)]
    if not parsed_rows:
        return []

    header_keys = [parse_header_key(x) for x in parsed_rows[0]]
    has_header = any(
        key in {"city", "name", "location", "zone", "place", "latitude", "lat", "longitude", "lon", "lng"}
        for key in header_keys
    )

    def find_index(candidates: Sequence[str]) -> int:
        for idx, key in enumerate(header_keys):
            if key in candidates:
                return idx
        return -1

    start_idx = 1 if has_header else 0
    name_idx = find_index(("city", "name", "location", "zone", "place")) if has_header else 0
    lat_idx = find_index(("latitude", "lat")) if has_header else 1
    lon_idx = find_index(("longitude", "lon", "lng", "long")) if has_header else 2
    country_idx = find_index(("country", "countrycode", "cc", "iso2")) if has_header else -1
    enabled_idx = find_index(("enabled", "active", "include", "use")) if has_header else -1

    out: List[Dict[str, float]] = []
    seen_names: set[str] = set()
    geocode_cache: Dict[Tuple[str, str], Optional[Dict[str, float]]] = {}

    for row in parsed_rows[start_idx:]:
        if enabled_idx >= 0 and enabled_idx < len(row):
            if not parse_bool_cell(row[enabled_idx], default=True):
                continue

        name = row[name_idx].strip() if 0 <= name_idx < len(row) else ""
        if not name:
            for cell in row:
                if cell:
                    name = cell
                    break
        if not name:
            continue

        lat = to_float(row[lat_idx]) if 0 <= lat_idx < len(row) else None
        lon = to_float(row[lon_idx]) if 0 <= lon_idx < len(row) else None
        country = row[country_idx].strip() if 0 <= country_idx < len(row) else ""

        if lat is None or lon is None:
            key = (name.lower(), country.upper())
            if key not in geocode_cache:
                geocode_cache[key] = geocode_location(name=name, country=country)
            resolved = geocode_cache.get(key)
            if not resolved:
                continue
            lat = to_float(resolved.get("lat"))
            lon = to_float(resolved.get("lon"))
            if lat is None or lon is None:
                continue
            if has_header and name_idx >= 0:
                preferred_name = str(row[name_idx]).strip()
                if preferred_name:
                    name = preferred_name
            else:
                name = str(resolved.get("name") or name).strip() or name

        normalized_name = name.strip()
        if not normalized_name:
            continue
        key_name = normalized_name.lower()
        if key_name in seen_names:
            continue
        seen_names.add(key_name)
        out.append({"name": normalized_name, "lat": float(lat), "lon": float(lon)})

    return out


def load_locations_from_watchlist() -> List[Dict[str, float]]:
    try:
        rows = fetch_watchlist_rows()
        locations = parse_watchlist_locations(rows)
        if locations:
            return locations
    except Exception:
        pass
    return [dict(loc) for loc in DEFAULT_LOCATIONS]


def london_today() -> dt.date:
    return dt.datetime.now(TZ).date()


def iso(d: dt.date) -> str:
    return d.isoformat()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def memory_clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def append_daily_memory_note(run_date: str, lines: Sequence[str]) -> None:
    try:
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        daily_path = MEMORY_DIR / f"{run_date}.md"
        if daily_path.exists():
            base = daily_path.read_text(encoding="utf-8", errors="ignore").rstrip() + "\n\n"
        else:
            base = f"# Memory Log - {run_date}\n\n"
        cleaned_lines = [memory_clean_text(x) for x in lines]
        entry = "\n".join([x for x in cleaned_lines if x]).strip()
        if not entry:
            return
        daily_path.write_text(base + entry + "\n", encoding="utf-8")
    except Exception:
        return


def update_heartbeat_state(check_name: str, status: str, details: Optional[Dict[str, object]] = None) -> None:
    try:
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, object] = {}
        if HEARTBEAT_STATE_PATH.exists():
            try:
                payload = json.loads(HEARTBEAT_STATE_PATH.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
        checks_obj = payload.get("checks", {})
        checks: Dict[str, object] = checks_obj if isinstance(checks_obj, dict) else {}

        now_iso = utc_now_iso()
        check_obj = checks.get(check_name, {})
        check = check_obj if isinstance(check_obj, dict) else {}
        check["status"] = status
        check["last_run_utc"] = now_iso
        if status == "ok":
            check["last_success_utc"] = now_iso
        if isinstance(details, dict) and details:
            check["details"] = details
        checks[check_name] = check

        payload["version"] = 1
        payload["updated_at_utc"] = now_iso
        payload["checks"] = checks
        HEARTBEAT_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    except Exception:
        return


def persist_weather_memory_entry(
    run_date: str,
    forecast_date: str,
    eval_date: str,
    active_sources: Sequence[str],
    available_sources: Sequence[str],
    missing_sources: Sequence[str],
    skipped_error_sources: Sequence[str],
) -> None:
    active_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in active_sources) or "(none)"
    available_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in available_sources) or "(none)"
    missing_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in missing_sources) or "(none)"
    skipped_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in skipped_error_sources) or "(none)"
    now_iso = utc_now_iso()
    lines = [
        f"- [{now_iso}] weather_briefing run",
        f"  forecast_date: {forecast_date}, eval_date: {eval_date}",
        f"  sources_active: {active_labels}",
        f"  sources_available: {available_labels}",
        f"  sources_missing: {missing_labels}",
        f"  sources_skipped_error: {skipped_labels}",
    ]
    append_daily_memory_note(run_date, lines)


def persist_weather_site_sync_note(run_date: str, sync_result: Dict[str, object]) -> None:
    if not sync_result:
        return
    status = memory_clean_text(sync_result.get("status") or "unknown")
    repo = memory_clean_text(sync_result.get("repo") or "(not found)")
    files = sync_result.get("changed_files") or sync_result.get("files") or []
    file_text = ", ".join(memory_clean_text(x) for x in files if memory_clean_text(x)) or "(none)"
    lines = [
        f"- [{utc_now_iso()}] weather_site_sync {status}",
        f"  repo: {repo}",
        f"  files: {file_text}",
    ]
    err = memory_clean_text(sync_result.get("error"))
    if err:
        lines.append(f"  error: {err}")
    append_daily_memory_note(run_date, lines)


def resolve_weather_site_repo_dir() -> Optional[Path]:
    candidates: List[Path] = []
    if WEATHER_SITE_REPO_PATH:
        candidates.append(Path(WEATHER_SITE_REPO_PATH))
    candidates.extend(
        [
            Path("/home/felixlee/Desktop/YuenYuenWeatherSite"),
            Path.home() / "Desktop/YuenYuenWeatherSite",
            Path.home() / "Documents/YuenYuenWeatherSite",
            Path("/Users/felixlee/Documents/YuenYuenWeatherSite"),
        ]
    )
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        try:
            if candidate.exists() and (candidate / ".git").exists():
                return candidate
        except Exception:
            continue
    return None


def round_metric(value: Optional[float], ndigits: int = 2) -> Optional[float]:
    v = to_float(value)
    if v is None:
        return None
    return round(v, ndigits)


def json_write_if_changed(path: Path, payload: Dict[str, object]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=True, indent=2) + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8", errors="ignore") == body:
            return False
    except Exception:
        pass
    path.write_text(body, encoding="utf-8")
    return True


def run_git(repo_dir: Path, args: Sequence[str]) -> Tuple[int, str, str]:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(repo_dir),
            check=False,
            capture_output=True,
            text=True,
        )
        return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()
    except Exception as exc:
        return 999, "", f"{exc.__class__.__name__}: {exc}"


def history_window_start(run_date: str, history_days: int) -> str:
    try:
        run_d = dt.date.fromisoformat(run_date)
    except Exception:
        run_d = london_today()
    start_d = run_d - dt.timedelta(days=max(1, history_days) - 1)
    return iso(start_d)


def build_weather_site_history_payload(conn: sqlite3.Connection, run_date: str) -> Dict[str, object]:
    start_date = history_window_start(run_date, WEATHER_SITE_HISTORY_DAYS)

    score_rows = conn.execute(
        """
        SELECT date, source, mae_temp_max, mae_temp_min, mae_wind_max, composite_error, confidence, sample_count
        FROM source_scores
        WHERE date >= ?
        ORDER BY date DESC, source ASC
        """,
        (start_date,),
    ).fetchall()

    weight_rows = conn.execute(
        """
        SELECT date, source, weight, rolling_confidence, lookback_days
        FROM source_weights
        WHERE date >= ?
        ORDER BY date DESC, source ASC
        """,
        (start_date,),
    ).fetchall()

    actual_rows = conn.execute(
        """
        SELECT date, location, lat, lon, temp_max, temp_min, wind_max
        FROM actuals
        WHERE date >= ?
        ORDER BY date DESC, location ASC
        """,
        (start_date,),
    ).fetchall()

    forecast_rows = conn.execute(
        """
        SELECT run_date, target_date, source, location, temp_max, temp_min, wind_max
        FROM forecasts
        WHERE run_date >= ?
        ORDER BY run_date DESC, target_date DESC, source ASC, location ASC
        """,
        (start_date,),
    ).fetchall()

    return {
        "generated_at_utc": utc_now_iso(),
        "run_date": run_date,
        "window_days": WEATHER_SITE_HISTORY_DAYS,
        "start_date": start_date,
        "source_scores": [
            {
                "date": str(r["date"]),
                "source": str(r["source"]),
                "source_label": SOURCE_LABELS.get(str(r["source"]), str(r["source"])),
                "mae_temp_max": round_metric(r["mae_temp_max"]),
                "mae_temp_min": round_metric(r["mae_temp_min"]),
                "mae_wind_max": round_metric(r["mae_wind_max"]),
                "composite_error": round_metric(r["composite_error"], 4),
                "confidence": round_metric(r["confidence"], 1),
                "sample_count": int(r["sample_count"]) if r["sample_count"] is not None else 0,
            }
            for r in score_rows
        ],
        "source_weights": [
            {
                "date": str(r["date"]),
                "source": str(r["source"]),
                "source_label": SOURCE_LABELS.get(str(r["source"]), str(r["source"])),
                "weight": round_metric(r["weight"], 6),
                "weight_pct": round_metric((to_float(r["weight"]) or 0.0) * 100.0, 2),
                "rolling_confidence": round_metric(r["rolling_confidence"], 1),
                "lookback_days": int(r["lookback_days"]) if r["lookback_days"] is not None else 0,
            }
            for r in weight_rows
        ],
        "actuals": [
            {
                "date": str(r["date"]),
                "location": str(r["location"]),
                "lat": round_metric(r["lat"], 4),
                "lon": round_metric(r["lon"], 4),
                "temp_max": round_metric(r["temp_max"]),
                "temp_min": round_metric(r["temp_min"]),
                "wind_max": round_metric(r["wind_max"]),
            }
            for r in actual_rows
        ],
        "forecasts": [
            {
                "run_date": str(r["run_date"]),
                "target_date": str(r["target_date"]),
                "source": str(r["source"]),
                "source_label": SOURCE_LABELS.get(str(r["source"]), str(r["source"])),
                "location": str(r["location"]),
                "temp_max": round_metric(r["temp_max"]),
                "temp_min": round_metric(r["temp_min"]),
                "wind_max": round_metric(r["wind_max"]),
            }
            for r in forecast_rows
        ],
    }


def build_weather_site_payloads(
    conn: sqlite3.Connection,
    mode: str,
    run_date: str,
    forecast_date: str,
    eval_date: str,
    configured_sources: Sequence[str],
    available_sources: Sequence[str],
    skipped_error_sources: Sequence[str],
    missing_sources: Sequence[str],
    zone_rows: Dict[str, Dict[str, Optional[float]]],
    forecasts: Dict[str, Dict[str, Dict[str, Optional[float]]]],
    rolling: Dict[str, Dict[str, float]],
    weights: Dict[str, float],
    eval_results: Dict[str, Dict[str, Optional[float]]],
    mwis_links: Sequence[str],
) -> Dict[str, Dict[str, object]]:
    zones: List[Dict[str, object]] = []
    report_sources = [s for s in configured_sources if s in available_sources]

    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        source_rows = forecasts.get(name, {})
        suitability = activity_suitability(row.get("temp_min"), row.get("temp_max"), row.get("wind_max"))
        zones.append(
            {
                "name": name,
                "lat": round_metric(loc["lat"], 4),
                "lon": round_metric(loc["lon"], 4),
                "ensemble": {
                    "temp_min": round_metric(row.get("temp_min")),
                    "temp_max": round_metric(row.get("temp_max")),
                    "wind_max": round_metric(row.get("wind_max")),
                    "rain_chance": round_metric(row.get("rain_chance"), 1),
                    "wind_dir": round_metric(row.get("wind_dir"), 0),
                    "wind_dir_cardinal": direction_to_cardinal(row.get("wind_dir")),
                    "spread_temp": round_metric(row.get("spread_temp")),
                    "spread_wind": round_metric(row.get("spread_wind")),
                    "next_7_days": [
                        {
                            "date": str(day.get("date")),
                            "temp_min": round_metric(day.get("temp_min")),
                            "temp_max": round_metric(day.get("temp_max")),
                            "wind_max": round_metric(day.get("wind_max")),
                            "rain_chance": round_metric(day.get("rain_chance"), 1),
                            "wind_dir": round_metric(day.get("wind_dir"), 0),
                            "wind_dir_cardinal": direction_to_cardinal(day.get("wind_dir")),
                        }
                        for day in (row.get("next_7_days") or [])
                        if isinstance(day, dict)
                    ],
                },
                "briefing": zone_briefing_line(
                    name=name,
                    tmin=row.get("temp_min"),
                    tmax=row.get("temp_max"),
                    wind_kmh=row.get("wind_max"),
                    rain_chance=row.get("rain_chance"),
                    wind_dir=row.get("wind_dir"),
                    spread_temp=row.get("spread_temp"),
                    spread_wind=row.get("spread_wind"),
                ),
                "suitability": suitability,
                "source_forecasts": {
                    source: {
                        "source_label": SOURCE_LABELS.get(source, source),
                        "temp_min": round_metric(metrics.get("temp_min")),
                        "temp_max": round_metric(metrics.get("temp_max")),
                        "wind_max": round_metric(metrics.get("wind_max")),
                        "rain_chance": round_metric(metrics.get("rain_chance"), 1),
                        "wind_dir": round_metric(metrics.get("wind_dir"), 0),
                        "wind_dir_cardinal": direction_to_cardinal(metrics.get("wind_dir")),
                        "next_7_days": [
                            {
                                "date": str(day.get("date")),
                                "temp_min": round_metric(day.get("temp_min")),
                                "temp_max": round_metric(day.get("temp_max")),
                                "wind_max": round_metric(day.get("wind_max")),
                                "rain_chance": round_metric(day.get("rain_chance"), 1),
                                "wind_dir": round_metric(day.get("wind_dir"), 0),
                                "wind_dir_cardinal": direction_to_cardinal(day.get("wind_dir")),
                            }
                            for day in (metrics.get("next_7_days") or [])
                            if isinstance(day, dict)
                        ],
                    }
                    for source, metrics in source_rows.items()
                },
            }
        )

    latest_report_payload: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "mode": mode,
        "run_date": run_date,
        "forecast_date": forecast_date,
        "eval_date": eval_date,
        "lookback_days": LOOKBACK_DAYS,
        "sources": {
            "configured": list(configured_sources),
            "available": list(available_sources),
            "used_for_report": report_sources,
            "missing_api_keys": list(missing_sources),
            "skipped_errors": list(skipped_error_sources),
        },
        "zones": zones,
        "mwis_pdf_links": list(mwis_links),
    }

    benchmark_payload: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "run_date": run_date,
        "eval_date": eval_date,
        "lookback_days": LOOKBACK_DAYS,
        "sources": [
            {
                "source": source,
                "source_label": SOURCE_LABELS.get(source, source),
                "is_available": source in available_sources,
                "is_missing_api_key": source in missing_sources,
                "is_skipped_error": source in skipped_error_sources,
                "runtime_note": RUNTIME_SOURCE_NOTES.get(source, ""),
                "latest_confidence": round_metric((eval_results.get(source) or {}).get("confidence"), 1),
                "mae_temp_max": round_metric((eval_results.get(source) or {}).get("mae_temp_max")),
                "mae_temp_min": round_metric((eval_results.get(source) or {}).get("mae_temp_min")),
                "mae_wind_max": round_metric((eval_results.get(source) or {}).get("mae_wind_max")),
                "composite_error": round_metric((eval_results.get(source) or {}).get("composite_error"), 4),
                "sample_count": int(round_metric((eval_results.get(source) or {}).get("sample_count"), 0) or 0),
                "rolling_confidence": round_metric((rolling.get(source) or {}).get("rolling_confidence"), 1),
                "rolling_error": round_metric((rolling.get(source) or {}).get("rolling_error"), 4),
                "rolling_samples": int(round_metric((rolling.get(source) or {}).get("samples"), 0) or 0),
                "ensemble_weight": round_metric(weights.get(source), 6),
                "ensemble_weight_pct": round_metric((weights.get(source, 0.0) * 100.0), 2),
            }
            for source in configured_sources
        ],
    }

    history_payload = build_weather_site_history_payload(conn=conn, run_date=run_date)

    return {
        "weather_latest_report.json": latest_report_payload,
        "weather_benchmarks_latest.json": benchmark_payload,
        "weather_history_recent.json": history_payload,
    }


def publish_weather_site_json(
    payloads: Dict[str, Dict[str, object]],
    run_date: str,
    forecast_date: str,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "enabled": bool(WEATHER_SITE_SYNC_ENABLED),
        "status": "disabled",
        "repo": "",
        "repo_url": WEATHER_SITE_REPO_URL,
        "branch": WEATHER_SITE_GIT_BRANCH,
        "remote": WEATHER_SITE_GIT_REMOTE,
        "data_subdir": WEATHER_SITE_DATA_SUBDIR,
        "files": sorted(payloads.keys()),
        "changed_files": [],
        "error": "",
        "push_enabled": bool(WEATHER_SITE_GIT_PUSH_ENABLED),
    }

    if not WEATHER_SITE_SYNC_ENABLED:
        return result

    repo_dir = resolve_weather_site_repo_dir()
    if repo_dir is None:
        result["status"] = "skipped_repo_not_found"
        result["error"] = "target repo directory not found; set WEATHER_SITE_REPO_PATH"
        return result

    result["repo"] = str(repo_dir)

    output_dir = repo_dir / WEATHER_SITE_DATA_SUBDIR
    rel_paths: List[str] = []
    changed_files: List[str] = []
    for filename, payload in payloads.items():
        rel_path = (Path(WEATHER_SITE_DATA_SUBDIR) / filename).as_posix()
        target = output_dir / filename
        changed = json_write_if_changed(target, payload)
        rel_paths.append(rel_path)
        if changed:
            changed_files.append(rel_path)
    result["files"] = rel_paths
    result["changed_files"] = changed_files

    add_rc, _, add_err = run_git(repo_dir, ["add", "--", *rel_paths])
    if add_rc != 0:
        result["status"] = "failed_git_add"
        result["error"] = add_err or f"git add failed ({add_rc})"
        return result

    diff_rc, _, diff_err = run_git(repo_dir, ["diff", "--cached", "--quiet", "--", *rel_paths])
    if diff_rc == 0:
        result["status"] = "up_to_date"
        return result
    if diff_rc != 1:
        result["status"] = "failed_git_diff"
        result["error"] = diff_err or f"git diff failed ({diff_rc})"
        return result

    commit_msg = f"chore(weather-data): refresh weather JSON for {run_date} -> {forecast_date}"
    commit_rc, commit_out, commit_err = run_git(repo_dir, ["commit", "-m", commit_msg, "--", *rel_paths])
    if commit_rc != 0:
        result["status"] = "failed_git_commit"
        result["error"] = commit_err or commit_out or f"git commit failed ({commit_rc})"
        return result
    result["status"] = "committed"

    if not WEATHER_SITE_GIT_PUSH_ENABLED:
        return result

    push_rc, push_out, push_err = run_git(
        repo_dir,
        ["push", WEATHER_SITE_GIT_REMOTE, f"HEAD:{WEATHER_SITE_GIT_BRANCH}"],
    )
    if push_rc != 0:
        result["status"] = "committed_push_failed"
        result["error"] = push_err or push_out or f"git push failed ({push_rc})"
        return result
    result["status"] = "pushed"
    return result


def to_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def value_at(values, idx: int) -> Optional[float]:
    if not isinstance(values, list) or idx < 0 or idx >= len(values):
        return None
    return to_float(values[idx])


def rounded_coord(lat: float, lon: float) -> Tuple[float, float]:
    return (round(float(lat), 4), round(float(lon), 4))


def none_metrics() -> Dict[str, Optional[float]]:
    out = dict(DEFAULT_NONE_METRICS)
    out["next_7_days"] = []
    return out


def has_any_metric(metrics: Dict[str, Optional[float]]) -> bool:
    return any(metrics.get(k) is not None for k in ("temp_max", "temp_min", "wind_max"))


CARDINAL_TO_DEGREES = {
    "N": 0.0,
    "NNE": 22.5,
    "NE": 45.0,
    "ENE": 67.5,
    "E": 90.0,
    "ESE": 112.5,
    "SE": 135.0,
    "SSE": 157.5,
    "S": 180.0,
    "SSW": 202.5,
    "SW": 225.0,
    "WSW": 247.5,
    "W": 270.0,
    "WNW": 292.5,
    "NW": 315.0,
    "NNW": 337.5,
}
CARDINAL_ORDER = list(CARDINAL_TO_DEGREES.keys())


def clamp_probability_percent(value) -> Optional[float]:
    v = to_float(value)
    if v is None:
        return None
    if 0.0 <= v <= 1.0:
        v *= 100.0
    return max(0.0, min(100.0, v))


def parse_wind_direction_degrees(value) -> Optional[float]:
    if isinstance(value, str):
        raw = value.strip().upper()
        if not raw:
            return None
        cardinal = re.sub(r"[^A-Z]", "", raw)
        if cardinal in CARDINAL_TO_DEGREES:
            return CARDINAL_TO_DEGREES[cardinal]
        m = re.search(r"(-?\d+(?:\.\d+)?)", raw)
        if m:
            d = to_float(m.group(1))
            if d is not None:
                return d % 360.0
        return None
    d = to_float(value)
    if d is None:
        return None
    return d % 360.0


def direction_to_cardinal(direction_deg: Optional[float]) -> str:
    d = to_float(direction_deg)
    if d is None:
        return "?"
    idx = int(((d % 360.0) + 11.25) // 22.5) % 16
    return CARDINAL_ORDER[idx]


def mean_direction_deg(values: Iterable[float]) -> Optional[float]:
    usable = [to_float(v) for v in values]
    usable = [v for v in usable if v is not None]
    if not usable:
        return None
    sin_sum = sum(math.sin(math.radians(v)) for v in usable)
    cos_sum = sum(math.cos(math.radians(v)) for v in usable)
    if abs(sin_sum) < 1e-9 and abs(cos_sum) < 1e-9:
        return None
    return (math.degrees(math.atan2(sin_sum, cos_sum)) + 360.0) % 360.0


def weighted_direction_deg(values: Dict[str, Optional[float]], weights: Dict[str, float]) -> Optional[float]:
    usable = [(src, parse_wind_direction_degrees(val)) for src, val in values.items()]
    usable = [(src, val) for src, val in usable if val is not None]
    if not usable:
        return None

    total_w = sum(weights.get(src, 0.0) for src, _ in usable)
    if total_w <= 0:
        total_w = float(len(usable))
        weighted = [(1.0, val) for _, val in usable]
    else:
        weighted = [(weights.get(src, 0.0), val) for src, val in usable]

    sin_sum = sum(w * math.sin(math.radians(val)) for w, val in weighted)
    cos_sum = sum(w * math.cos(math.radians(val)) for w, val in weighted)
    if abs(sin_sum) < 1e-9 and abs(cos_sum) < 1e-9:
        return None
    return (math.degrees(math.atan2(sin_sum, cos_sum)) + 360.0) % 360.0


def normalize_daily_forecast_item(
    date_str: str,
    temp_max: Optional[float],
    temp_min: Optional[float],
    wind_max: Optional[float],
    rain_chance: Optional[float],
    wind_dir: Optional[float],
) -> Dict[str, Optional[float]]:
    return {
        "date": str(date_str),
        "temp_max": to_float(temp_max),
        "temp_min": to_float(temp_min),
        "wind_max": to_float(wind_max),
        "rain_chance": clamp_probability_percent(rain_chance),
        "wind_dir": parse_wind_direction_degrees(wind_dir),
    }


def mps_to_kmh(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 3.6


def mph_to_kmh(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 1.60934


def fahrenheit_to_celsius(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return (value - 32.0) * 5.0 / 9.0


def request_json(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[Dict]:
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def request_json_with_meta(url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[Optional[int], Optional[Dict], str]:
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        status = resp.status_code
        try:
            data = resp.json()
        except Exception:
            data = None
        message = ""
        if isinstance(data, dict):
            raw_msg = data.get("message")
            if not isinstance(raw_msg, str):
                err_obj = data.get("error")
                if isinstance(err_obj, dict):
                    raw_msg = err_obj.get("message")
            if isinstance(raw_msg, str):
                message = raw_msg
        return status, data if isinstance(data, dict) else None, message
    except Exception as exc:
        return None, None, str(exc)


def request_text_with_meta(url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[Optional[int], Optional[str], str]:
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        return resp.status_code, resp.text, ""
    except Exception as exc:
        return None, None, str(exc)


def set_runtime_note_once(source: str, message: str) -> None:
    if message and source not in RUNTIME_SOURCE_NOTES:
        RUNTIME_SOURCE_NOTES[source] = message


def decode_jwt_payload(token: str) -> Optional[Dict]:
    if not token:
        return None
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload_b64 = parts[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def metoffice_subscription_hint(token: str) -> str:
    payload = decode_jwt_payload(token)
    if not payload:
        return ""

    subscribed = payload.get("subscribedAPIs", [])
    if not isinstance(subscribed, list) or not subscribed:
        return ""

    contexts: List[str] = []
    names: List[str] = []
    for api in subscribed:
        if not isinstance(api, dict):
            continue
        context = str(api.get("context", "") or "")
        name = str(api.get("name", "") or "")
        if context:
            contexts.append(context)
        if name:
            names.append(name)

    if any("/sitespecific/" in c for c in contexts):
        return ""

    if names:
        return f"token subscribed to {', '.join(dict.fromkeys(names))}, not SiteSpecificForecast"
    if contexts:
        return f"token subscribed to {', '.join(dict.fromkeys(contexts))}, not /sitespecific/v0"
    return ""


def token_has_api_context(token: str, context_fragment: str) -> bool:
    payload = decode_jwt_payload(token)
    if not payload:
        return False

    subscribed = payload.get("subscribedAPIs", [])
    if not isinstance(subscribed, list):
        return False

    frag = context_fragment.lower()
    for api in subscribed:
        if not isinstance(api, dict):
            continue
        context = str(api.get("context", "") or "").lower()
        name = str(api.get("name", "") or "").lower()
        if frag in context or frag in name:
            return True
    return False


def normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def flatten_numeric(obj, prefix: str = "") -> Iterable[Tuple[str, float]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            next_prefix = f"{prefix}.{k}" if prefix else str(k)
            yield from flatten_numeric(v, next_prefix)
        return
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            next_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
            yield from flatten_numeric(v, next_prefix)
        return

    v = to_float(obj)
    if v is not None and prefix:
        yield prefix, v


def pick_value_from_obj(obj, aliases: Sequence[str], avoid_tokens: Sequence[str] = ()) -> Optional[float]:
    items = list(flatten_numeric(obj))
    if not items:
        return None

    normalized_aliases = [normalize_key(a) for a in aliases]
    normalized_avoid = [normalize_key(a) for a in avoid_tokens]

    for alias in normalized_aliases:
        for path, value in items:
            norm_path = normalize_key(path)
            if alias and alias in norm_path:
                if any(token in norm_path for token in normalized_avoid):
                    continue
                return value

    return None


def extract_open_meteo_daily(payload: Dict, target_date: str) -> Dict[str, Optional[float]]:
    daily = payload.get("daily", {}) if isinstance(payload, dict) else {}
    times = daily.get("time", []) if isinstance(daily, dict) else []

    if target_date not in times:
        out = none_metrics()
        out["next_7_days"] = extract_open_meteo_next_7_days(payload)
        return out

    idx = times.index(target_date)
    out = {
        "temp_max": value_at(daily.get("temperature_2m_max", []), idx),
        "temp_min": value_at(daily.get("temperature_2m_min", []), idx),
        "wind_max": value_at(daily.get("wind_speed_10m_max", []), idx),
        "rain_chance": clamp_probability_percent(value_at(daily.get("precipitation_probability_max", []), idx)),
        "wind_dir": parse_wind_direction_degrees(value_at(daily.get("wind_direction_10m_dominant", []), idx)),
    }
    out["next_7_days"] = extract_open_meteo_next_7_days(payload)
    return out


def extract_open_meteo_next_7_days(payload: Dict) -> List[Dict[str, Optional[float]]]:
    daily = payload.get("daily", {}) if isinstance(payload, dict) else {}
    times = daily.get("time", []) if isinstance(daily, dict) else []
    if not isinstance(times, list):
        return []

    out: List[Dict[str, Optional[float]]] = []
    for idx, date_str in enumerate(times[:7]):
        if not isinstance(date_str, str):
            continue
        out.append(
            normalize_daily_forecast_item(
                date_str=date_str,
                temp_max=value_at(daily.get("temperature_2m_max", []), idx),
                temp_min=value_at(daily.get("temperature_2m_min", []), idx),
                wind_max=value_at(daily.get("wind_speed_10m_max", []), idx),
                rain_chance=value_at(daily.get("precipitation_probability_max", []), idx),
                wind_dir=value_at(daily.get("wind_direction_10m_dominant", []), idx),
            )
        )
    return out


def fetch_open_meteo_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "temperature_2m_max,temperature_2m_min,wind_speed_10m_max,"
            "precipitation_probability_max,wind_direction_10m_dominant"
        ),
        "forecast_days": 7,
        "timezone": "Europe/London",
    }
    url = f"{OPENMETEO_FORECAST_BASE}?{urlencode(params)}"
    payload = request_json(url)
    if not payload:
        return none_metrics()
    return extract_open_meteo_daily(payload, target_date)


def fetch_open_meteo_actual(lat: float, lon: float, date_str: str) -> Dict[str, Optional[float]]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": "temperature_2m_max,temperature_2m_min,wind_speed_10m_max",
        "timezone": "Europe/London",
    }
    url = f"{OPENMETEO_ARCHIVE_BASE}?{urlencode(params)}"
    payload = request_json(url)
    if not payload:
        return none_metrics()
    out = extract_open_meteo_daily(payload, date_str)
    out["next_7_days"] = []
    return out


def fetch_met_no_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    url = f"{MET_NO_BASE}?lat={lat}&lon={lon}"
    payload = request_json(url, headers={"User-Agent": MET_NO_USER_AGENT})
    if not payload:
        return none_metrics()

    timeseries = (
        payload.get("properties", {})
        .get("timeseries", [])
        if isinstance(payload, dict)
        else []
    )
    day_buckets: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: {"temps": [], "winds": [], "dirs": [], "rain_prob": []}
    )

    for item in timeseries:
        if not isinstance(item, dict):
            continue
        raw_time = item.get("time")
        if not isinstance(raw_time, str):
            continue

        try:
            ts = dt.datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(TZ)
        except ValueError:
            continue

        details = item.get("data", {}).get("instant", {}).get("details", {})
        if not isinstance(details, dict):
            continue

        t = to_float(details.get("air_temperature"))
        w = to_float(details.get("wind_speed"))
        wd = parse_wind_direction_degrees(details.get("wind_from_direction"))

        precip_amount = None
        rain_detail = (
            item.get("data", {}).get("next_1_hours", {}).get("details", {})
            if isinstance(item.get("data", {}).get("next_1_hours"), dict)
            else {}
        )
        if isinstance(rain_detail, dict):
            precip_amount = to_float(rain_detail.get("precipitation_amount"))

        day_key = ts.date().isoformat()
        bucket = day_buckets[day_key]
        if t is not None:
            bucket["temps"].append(t)
        if w is not None:
            bucket["winds"].append(mps_to_kmh(w))
        if wd is not None:
            bucket["dirs"].append(wd)
        if precip_amount is not None:
            bucket["rain_prob"].append(100.0 if precip_amount > 0.05 else 0.0)

    next_7_days: List[Dict[str, Optional[float]]] = []
    for date_str in sorted(day_buckets.keys())[:7]:
        bucket = day_buckets[date_str]
        next_7_days.append(
            normalize_daily_forecast_item(
                date_str=date_str,
                temp_max=max(bucket["temps"]) if bucket["temps"] else None,
                temp_min=min(bucket["temps"]) if bucket["temps"] else None,
                wind_max=max(bucket["winds"]) if bucket["winds"] else None,
                rain_chance=mean(bucket["rain_prob"]) if bucket["rain_prob"] else None,
                wind_dir=mean_direction_deg(bucket["dirs"]),
            )
        )

    target_day = next((x for x in next_7_days if x.get("date") == target_date), None)
    if target_day:
        out = {
            "temp_max": target_day.get("temp_max"),
            "temp_min": target_day.get("temp_min"),
            "wind_max": target_day.get("wind_max"),
            "rain_chance": target_day.get("rain_chance"),
            "wind_dir": target_day.get("wind_dir"),
            "next_7_days": next_7_days,
        }
        return out

    out = none_metrics()
    out["next_7_days"] = next_7_days
    return out


def fetch_met_office_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not METOFFICE_UI_ENABLED:
        return none_metrics()

    # Met Office forecast URLs accept geohash-like location IDs. The values
    # below are generated from target coordinates, then fetched as rendered HTML.
    geohash_id = geohash_encode(lat, lon, METOFFICE_UI_GEOHASH_PRECISION)
    if not geohash_id:
        set_runtime_note_once(SOURCE_MET_OFFICE, "Failed to generate Met Office forecast geohash")
        return none_metrics()

    base = METOFFICE_UI_FORECAST_BASE.rstrip("/")
    url = f"{base}/{geohash_id}"
    status, html_text, message = request_text_with_meta(
        url,
        headers={
            # The weather UI is public web content; send a browser-like UA.
            "User-Agent": "Mozilla/5.0 (compatible; AIBot-WeatherBenchmark/1.0)",
        },
    )

    if status is None:
        set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office UI request failed ({message or 'network error'})")
        return none_metrics()

    if status >= 400:
        set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office UI HTTP {status} ({message or 'request failed'})")
        return none_metrics()

    if not html_text:
        set_runtime_note_once(SOURCE_MET_OFFICE, "Met Office UI payload unavailable")
        return none_metrics()

    hourly_tables = extract_metoffice_hourly_tables_by_date(html_text)
    hourly_table_html = hourly_tables.get(target_date, "")
    if not hourly_table_html and target_date not in hourly_tables:
        set_runtime_note_once(SOURCE_MET_OFFICE, "Met Office UI missing hourly table for target date")
    temps = extract_metoffice_temperatures_from_hourly_table(hourly_table_html) if hourly_table_html else []
    winds_mph = extract_metoffice_winds_mph_from_hourly_table(hourly_table_html) if hourly_table_html else []
    rains = extract_metoffice_rain_chance_from_hourly_table(hourly_table_html) if hourly_table_html else []
    wind_dirs = extract_metoffice_wind_dirs_from_hourly_table(hourly_table_html) if hourly_table_html else []

    next_7_days: List[Dict[str, Optional[float]]] = []
    for date_str in sorted(hourly_tables.keys())[:7]:
        table = hourly_tables[date_str]
        tvals = extract_metoffice_temperatures_from_hourly_table(table)
        wvals = extract_metoffice_winds_mph_from_hourly_table(table)
        rvals = extract_metoffice_rain_chance_from_hourly_table(table)
        dvals = extract_metoffice_wind_dirs_from_hourly_table(table)
        next_7_days.append(
            normalize_daily_forecast_item(
                date_str=date_str,
                temp_max=max(tvals) if tvals else None,
                temp_min=min(tvals) if tvals else None,
                wind_max=mph_to_kmh(max(wvals) if wvals else None),
                rain_chance=max(rvals) if rvals else None,
                wind_dir=mean_direction_deg(dvals),
            )
        )

    metrics = {
        "temp_max": max(temps) if temps else None,
        "temp_min": min(temps) if temps else None,
        "wind_max": mph_to_kmh(max(winds_mph) if winds_mph else None),
        "rain_chance": max(rains) if rains else None,
        "wind_dir": mean_direction_deg(wind_dirs),
        "next_7_days": next_7_days,
    }
    if not has_any_metric(metrics):
        set_runtime_note_once(SOURCE_MET_OFFICE, "No target-day Met Office metrics found in hourly table")
    return metrics


def geohash_encode(lat: float, lon: float, precision: int = 9) -> str:
    try:
        lat_v = float(lat)
        lon_v = float(lon)
    except Exception:
        return ""

    if not (-90.0 <= lat_v <= 90.0 and -180.0 <= lon_v <= 180.0):
        return ""

    alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
    bits = [16, 8, 4, 2, 1]
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bit_idx = 0
    ch = 0
    even = True
    out: List[str] = []

    while len(out) < precision:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2.0
            if lon_v > mid:
                ch |= bits[bit_idx]
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2.0
            if lat_v > mid:
                ch |= bits[bit_idx]
                lat_range[0] = mid
            else:
                lat_range[1] = mid
        even = not even
        if bit_idx < 4:
            bit_idx += 1
        else:
            out.append(alphabet[ch])
            bit_idx = 0
            ch = 0
    return "".join(out)


def strip_html_tags(text: str) -> str:
    raw = re.sub(r"<[^>]+>", " ", text, flags=re.S)
    return re.sub(r"\s+", " ", html.unescape(raw)).strip()


def extract_metoffice_hourly_table_for_date(html_text: str, target_date: str) -> str:
    pattern = re.compile(
        rf"<table\b(?=[^>]*\bclass=\"[^\"]*\bhourly-table\b[^\"]*\")(?=[^>]*\bdata-date=\"{re.escape(target_date)}\")[^>]*>(.*?)</table>",
        re.I | re.S,
    )
    match = pattern.search(html_text)
    return match.group(1) if match else ""


def extract_metoffice_hourly_tables_by_date(html_text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    pattern = re.compile(
        r"<table\b(?=[^>]*\bclass=\"[^\"]*\bhourly-table\b[^\"]*\")(?=[^>]*\bdata-date=\"([^\"]+)\")[^>]*>(.*?)</table>",
        re.I | re.S,
    )
    for match in pattern.finditer(html_text or ""):
        date_str = str(match.group(1) or "").strip()
        table_html = str(match.group(2) or "")
        if not date_str or not table_html:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            out[date_str] = table_html
    return out


def extract_row_cells_from_table(table_html: str, class_fragment: str) -> List[str]:
    row_match = re.search(
        rf"<tr\b[^>]*\bclass=\"[^\"]*{re.escape(class_fragment)}[^\"]*\"[^>]*>(.*?)</tr>",
        table_html,
        re.I | re.S,
    )
    if not row_match:
        return []
    row_html = row_match.group(1)
    return re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, re.I | re.S)


def extract_metoffice_temperatures_from_hourly_table(table_html: str) -> List[float]:
    values: List[float] = []
    for cell_html in extract_row_cells_from_table(table_html, "weather-temperature-row"):
        text = strip_html_tags(cell_html)
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*°", text)
        if not m:
            continue
        val = to_float(m.group(1))
        if val is not None:
            values.append(val)
    return values


def extract_metoffice_winds_mph_from_hourly_table(table_html: str) -> List[float]:
    values: List[float] = []
    for cell_html in extract_row_cells_from_table(table_html, "wind-row"):
        text = strip_html_tags(cell_html)
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*mph\b", text, re.I)
        if not m:
            continue
        val = to_float(m.group(1))
        if val is not None:
            values.append(val)
    return values


def extract_metoffice_wind_dirs_from_hourly_table(table_html: str) -> List[float]:
    values: List[float] = []
    for cell_html in extract_row_cells_from_table(table_html, "wind-row"):
        text = strip_html_tags(cell_html)
        direction = None
        m_deg = re.search(r"(-?\d+(?:\.\d+)?)\s*°", text)
        if m_deg:
            direction = parse_wind_direction_degrees(m_deg.group(1))
        if direction is None:
            m_card = re.search(r"\b(N|NNE|NE|ENE|E|ESE|SE|SSE|S|SSW|SW|WSW|W|WNW|NW|NNW)\b", text, re.I)
            if m_card:
                direction = parse_wind_direction_degrees(m_card.group(1))
        if direction is not None:
            values.append(direction)
    return values


def extract_metoffice_rain_chance_from_hourly_table(table_html: str) -> List[float]:
    row_classes = (
        "precipitation-probability-row",
        "rain-probability-row",
        "precipitation-row",
        "chance-of-rain-row",
        "rain-row",
        "probability-row",
    )
    values: List[float] = []
    for row_class in row_classes:
        cells = extract_row_cells_from_table(table_html, row_class)
        if not cells:
            continue
        for cell_html in cells:
            text = strip_html_tags(cell_html)
            m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", text)
            if not m:
                continue
            prob = clamp_probability_percent(m.group(1))
            if prob is not None:
                values.append(prob)
        if values:
            return values
    return values


def sanitize_filename_fragment(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text)).strip("._")
    if not cleaned:
        return "file"
    return cleaned[:140]


def normalize_atmos_order_id(order_id: str) -> str:
    return str(order_id or "").strip().lower()


def get_eccodes_module():
    try:
        import eccodes  # type: ignore
        return eccodes
    except Exception:
        return None


def grib_get_safe(eccodes_module, gid, key: str):
    try:
        return eccodes_module.codes_get(gid, key)
    except Exception:
        return None


def parse_yyyymmdd_hhmm_utc(date_raw, time_raw) -> Optional[dt.datetime]:
    date_num = to_float(date_raw)
    time_num = to_float(time_raw)
    if date_num is None or time_num is None:
        return None

    date_int = int(date_num)
    time_int = int(time_num)
    year = date_int // 10000
    month = (date_int // 100) % 100
    day = date_int % 100
    hour = (time_int // 100) % 100
    minute = time_int % 100

    try:
        return dt.datetime(year, month, day, hour, minute, tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def grib_forecast_timedelta(forecast_time_raw, unit_raw) -> Optional[dt.timedelta]:
    ft = to_float(forecast_time_raw)
    if ft is None:
        return None
    ft_i = int(ft)
    unit_val = to_float(unit_raw)
    unit_i = int(unit_val if unit_val is not None else 1)

    if unit_i == 0:
        return dt.timedelta(minutes=ft_i)
    if unit_i == 1:
        return dt.timedelta(hours=ft_i)
    if unit_i == 2:
        return dt.timedelta(days=ft_i)
    if unit_i == 10:
        return dt.timedelta(hours=3 * ft_i)
    if unit_i == 11:
        return dt.timedelta(hours=6 * ft_i)
    if unit_i == 12:
        return dt.timedelta(hours=12 * ft_i)
    if unit_i == 13:
        return dt.timedelta(seconds=ft_i)
    return dt.timedelta(hours=ft_i)


def grib_valid_datetime_utc(eccodes_module, gid) -> Optional[dt.datetime]:
    direct = parse_yyyymmdd_hhmm_utc(
        grib_get_safe(eccodes_module, gid, "validityDate"),
        grib_get_safe(eccodes_module, gid, "validityTime"),
    )
    if direct:
        return direct

    base = parse_yyyymmdd_hhmm_utc(
        grib_get_safe(eccodes_module, gid, "dataDate"),
        grib_get_safe(eccodes_module, gid, "dataTime"),
    )
    if not base:
        return None

    delta = grib_forecast_timedelta(
        grib_get_safe(eccodes_module, gid, "forecastTime"),
        grib_get_safe(eccodes_module, gid, "indicatorOfUnitOfTimeRange"),
    )
    if delta is None:
        return base
    return base + delta


def classify_atmospheric_grib_message(eccodes_module, gid) -> Tuple[Optional[str], str]:
    short_name = str(grib_get_safe(eccodes_module, gid, "shortName") or "")
    name = str(grib_get_safe(eccodes_module, gid, "name") or grib_get_safe(eccodes_module, gid, "parameterName") or "")
    units = str(grib_get_safe(eccodes_module, gid, "units") or "")
    level_type = str(grib_get_safe(eccodes_module, gid, "typeOfLevel") or "")
    level = to_float(grib_get_safe(eccodes_module, gid, "level"))

    sn = normalize_key(short_name)
    nm = normalize_key(name)
    lt = normalize_key(level_type)

    if "gust" in nm or "gust" in sn:
        return None, units

    is_2m = (lt == "heightaboveground" and level is not None and abs(level - 2.0) < 0.2) or sn in ("2t", "t2m")
    is_10m = (lt == "heightaboveground" and level is not None and abs(level - 10.0) < 0.2) or sn in ("10u", "u10", "10v", "v10", "10si", "si10")

    if is_2m and ("temperature" in nm or sn in ("2t", "t2m", "2tmp")):
        return "temp", units

    if is_10m and (sn in ("10si", "si10", "10ws", "ws10") or "windspeed" in nm):
        return "wind", units

    if is_10m and (sn in ("10u", "u10") or "ucomponentofwind" in nm):
        return "u", units

    if is_10m and (sn in ("10v", "v10") or "vcomponentofwind" in nm):
        return "v", units

    if "2metretemperature" in nm:
        return "temp", units
    if "10metrewindspeed" in nm:
        return "wind", units
    if "10metreucomponentofwind" in nm:
        return "u", units
    if "10metrevcomponentofwind" in nm:
        return "v", units
    return None, units


def temperature_to_celsius(value: Optional[float], units: str) -> Optional[float]:
    v = to_float(value)
    if v is None:
        return None
    unit = normalize_key(units or "")
    if unit in ("k", "kelvin"):
        return v - 273.15
    if unit in ("f", "fahrenheit", "degf"):
        return fahrenheit_to_celsius(v)
    if v > 170.0:
        return v - 273.15
    return v


def wind_speed_to_mps(value: Optional[float], units: str) -> Optional[float]:
    v = to_float(value)
    if v is None:
        return None

    unit = normalize_key(units or "")
    if not unit:
        return v
    if "kilometreperhour" in unit or unit in ("kmh", "kph", "kmph"):
        return v / 3.6
    if "mileperhour" in unit or unit == "mph":
        return v / 2.2369362921
    if "knot" in unit or unit in ("kt", "kn"):
        return v * 0.514444
    if "metrepersecond" in unit or "meterpersecond" in unit or unit in ("ms", "mps", "ms1"):
        return v
    return v


def nearest_grib_value(eccodes_module, gid, lat: float, lon: float) -> Optional[float]:
    try:
        nearest = eccodes_module.codes_grib_find_nearest(gid, lat, lon)
    except Exception:
        return None

    if isinstance(nearest, dict):
        return to_float(nearest.get("value"))
    if isinstance(nearest, (list, tuple)) and nearest:
        first = nearest[0]
        if isinstance(first, dict):
            return to_float(first.get("value"))
        if isinstance(first, (list, tuple)) and first:
            return to_float(first[0])
    return None


def init_atmos_samples() -> Dict[Tuple[float, float], Dict[str, List[float]]]:
    out: Dict[Tuple[float, float], Dict[str, List[float]]] = {}
    for loc in LOCATIONS:
        out[rounded_coord(loc["lat"], loc["lon"])] = {"temps": [], "winds": []}
    return out


def merge_atmos_samples(
    target: Dict[Tuple[float, float], Dict[str, List[float]]],
    incoming: Dict[Tuple[float, float], Dict[str, List[float]]],
) -> None:
    for coord, sample in incoming.items():
        bucket = target.setdefault(coord, {"temps": [], "winds": []})
        bucket["temps"].extend(sample.get("temps", []))
        bucket["winds"].extend(sample.get("winds", []))


def samples_to_metrics(samples: Dict[Tuple[float, float], Dict[str, List[float]]]) -> Dict[Tuple[float, float], Dict[str, Optional[float]]]:
    out: Dict[Tuple[float, float], Dict[str, Optional[float]]] = {}
    for coord, sample in samples.items():
        temps = [v for v in sample.get("temps", []) if v is not None]
        winds = [v for v in sample.get("winds", []) if v is not None]
        if not temps and not winds:
            continue
        out[coord] = {
            "temp_max": max(temps) if temps else None,
            "temp_min": min(temps) if temps else None,
            "wind_max": max(winds) if winds else None,
        }
    return out


def parse_atmospheric_grib_file(
    grib_path: Path,
    target_date: str,
    eccodes_module,
) -> Dict[Tuple[float, float], Dict[str, List[float]]]:
    samples = init_atmos_samples()
    uv_components: Dict[Tuple[float, float], Dict[str, Dict[str, float]]] = defaultdict(dict)

    with grib_path.open("rb") as fh:
        while True:
            gid = eccodes_module.codes_grib_new_from_file(fh)
            if gid is None:
                break
            try:
                valid_utc = grib_valid_datetime_utc(eccodes_module, gid)
                if valid_utc is None:
                    continue
                if valid_utc.astimezone(TZ).date().isoformat() != target_date:
                    continue

                category, units = classify_atmospheric_grib_message(eccodes_module, gid)
                if not category:
                    continue

                ts_key = valid_utc.isoformat()
                for loc in LOCATIONS:
                    lat = float(loc["lat"])
                    lon = float(loc["lon"])
                    coord = rounded_coord(lat, lon)
                    raw_value = nearest_grib_value(eccodes_module, gid, lat, lon)
                    if raw_value is None:
                        continue

                    if category == "temp":
                        t_c = temperature_to_celsius(raw_value, units)
                        if t_c is not None:
                            samples[coord]["temps"].append(t_c)
                        continue

                    mps = wind_speed_to_mps(raw_value, units)
                    if mps is None:
                        continue

                    if category == "wind":
                        samples[coord]["winds"].append(mps_to_kmh(mps))
                        continue

                    comps = uv_components[coord].setdefault(ts_key, {})
                    comps[category] = mps
            finally:
                eccodes_module.codes_release(gid)

    for coord, by_ts in uv_components.items():
        for components in by_ts.values():
            u = components.get("u")
            v = components.get("v")
            if u is None or v is None:
                continue
            samples[coord]["winds"].append(mps_to_kmh(math.sqrt(u * u + v * v)))

    return samples


def resolve_atmos_order_id(payload: Dict) -> Optional[str]:
    orders = payload.get("orders", []) if isinstance(payload, dict) else []
    if not isinstance(orders, list):
        return None
    if not orders:
        return None

    order_ids = [normalize_atmos_order_id(o.get("orderId", "")) for o in orders if isinstance(o, dict)]
    order_ids = [o for o in order_ids if o]
    if not order_ids:
        return None

    if METOFFICE_ATMOS_ORDER_ID:
        pinned = normalize_atmos_order_id(METOFFICE_ATMOS_ORDER_ID)
        if pinned in order_ids:
            return pinned
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Configured METOFFICE_ATMOS_ORDER_ID '{METOFFICE_ATMOS_ORDER_ID}' not found in /orders",
        )
        return None

    if len(order_ids) > 1:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Multiple atmospheric orders found; using first '{order_ids[0]}' (set METOFFICE_ATMOS_ORDER_ID to pin one)",
        )
    return order_ids[0]


def atmospheric_file_score(file_obj: Dict, target_date: str) -> int:
    score = 0
    params = file_obj.get("parameters", [])
    if isinstance(params, list):
        merged = " ".join(normalize_key(str(p)) for p in params)
        if any(t in merged for t in ("temperature", "2metretemperature", "2t", "t2m")):
            score += 3
        if any(t in merged for t in ("windspeed", "10metrewindspeed", "10u", "ucomponentofwind", "10v", "vcomponentofwind")):
            score += 3

    timesteps = file_obj.get("timesteps", [])
    if isinstance(timesteps, list):
        ts_text = " ".join(str(t) for t in timesteps)
        if target_date.replace("-", "") in ts_text:
            score += 1
    return score


def select_atmospheric_files(files: Sequence[Dict], target_date: str) -> List[Dict]:
    scored: List[Tuple[int, str, Dict]] = []
    for raw in files:
        if not isinstance(raw, dict):
            continue
        file_id = str(raw.get("fileId", "")).strip()
        if not file_id:
            continue
        scored.append((atmospheric_file_score(raw, target_date), str(raw.get("runDateTime", "") or ""), raw))

    if not scored:
        return []

    if any(s > 0 for s, _, _ in scored):
        scored = [t for t in scored if t[0] > 0]

    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return [obj for _, _, obj in scored[:METOFFICE_ATMOS_MAX_FILES]]


def delete_file_safely(path: Path) -> None:
    try:
        path.unlink()
    except Exception:
        pass


def download_atmospheric_grib(order_id: str, file_id: str, headers: Dict[str, str]) -> Optional[Path]:
    METOFFICE_ATMOS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    normalized_order_id = normalize_atmos_order_id(order_id)
    safe_name = f"{sanitize_filename_fragment(normalized_order_id)}_{sanitize_filename_fragment(file_id)}.grib2"
    out_path = METOFFICE_ATMOS_CACHE_DIR / safe_name
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    order_enc = quote(normalized_order_id, safe="")
    file_enc = quote(file_id, safe="")
    data_url = f"{METOFFICE_ATMOSPHERIC_BASE}/orders/{order_enc}/latest/{file_enc}/data"
    max_bytes = METOFFICE_ATMOS_MAX_FILE_MB * 1024 * 1024

    try:
        with requests.get(
            data_url,
            headers={**headers, "Accept": "application/x-grib"},
            timeout=REQUEST_TIMEOUT * 3,
            stream=True,
        ) as resp:
            if resp.status_code >= 400:
                set_runtime_note_once(
                    SOURCE_MET_OFFICE_ATMOSPHERIC,
                    f"Atmospheric GRIB download failed for file '{file_id}' (HTTP {resp.status_code})",
                )
                return None

            content_len = to_float(resp.headers.get("Content-Length"))
            if content_len is not None and content_len > max_bytes:
                set_runtime_note_once(
                    SOURCE_MET_OFFICE_ATMOSPHERIC,
                    f"Atmospheric GRIB file '{file_id}' too large ({int(content_len / (1024 * 1024))}MB > {METOFFICE_ATMOS_MAX_FILE_MB}MB limit)",
                )
                return None

            tmp_path = out_path.with_suffix(out_path.suffix + ".part")
            size = 0
            try:
                with tmp_path.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=128 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > max_bytes:
                            set_runtime_note_once(
                                SOURCE_MET_OFFICE_ATMOSPHERIC,
                                f"Atmospheric GRIB stream exceeded {METOFFICE_ATMOS_MAX_FILE_MB}MB limit; skipped '{file_id}'",
                            )
                            delete_file_safely(tmp_path)
                            return None
                        fh.write(chunk)

                if size <= 0:
                    delete_file_safely(tmp_path)
                    return None
                tmp_path.replace(out_path)
                return out_path
            except Exception:
                delete_file_safely(tmp_path)
                raise
    except Exception as exc:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric GRIB download request failed for file '{file_id}' ({exc})",
        )
        return None


def fetch_met_office_atmospheric_target(target_date: str) -> Dict[Tuple[float, float], Dict[str, Optional[float]]]:
    out: Dict[Tuple[float, float], Dict[str, Optional[float]]] = {}
    if not METOFFICE_ATMOS_API_KEY:
        return out

    if not token_has_api_context(METOFFICE_ATMOS_API_KEY, "/atmospheric-models/"):
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            "token is not subscribed to atmospheric-models API context",
        )
        return out

    headers = {"apikey": METOFFICE_ATMOS_API_KEY, "Accept": "application/json"}

    # Allow explicit pinning to bypass unreliable/empty /orders responses.
    order_id = normalize_atmos_order_id(METOFFICE_ATMOS_ORDER_ID)
    if not order_id:
        status = None
        payload = None
        message = ""

        # Some API keys reject detail=MINIMAL; prefer FULL first.
        for query in (
            {"detail": "FULL", "dataSpec": "1.1.0"},
            {"detail": "FULL"},
            {"detail": "MINIMAL", "dataSpec": "1.1.0"},
        ):
            orders_url = f"{METOFFICE_ATMOSPHERIC_BASE}/orders?{urlencode(query)}"
            trial_status, trial_payload, trial_message = request_json_with_meta(orders_url, headers=headers)
            if trial_status == 200:
                status, payload, message = trial_status, trial_payload, trial_message
                break
            if status is None:
                status, payload, message = trial_status, trial_payload, trial_message

        if status is None:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric Models request failed ({message or 'network error'})",
            )
            return out
        if status == 401:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric Models auth failed (HTTP 401: {message or 'missing/invalid apikey header'})",
            )
            return out
        if status == 403:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric Models auth failed (HTTP 403: {message or 'resource forbidden'})",
            )
            return out
        if status >= 400:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric Models HTTP {status} ({message or 'request failed'})",
            )
            return out
        if not payload:
            set_runtime_note_once(SOURCE_MET_OFFICE_ATMOSPHERIC, "Atmospheric Models payload unavailable from /orders")
            return out

        order_id = resolve_atmos_order_id(payload)
        if not order_id:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                "No atmospheric orders configured (create an order in Met Office Data Configuration Tool)",
            )
            return out

    latest_url = f"{METOFFICE_ATMOSPHERIC_BASE}/orders/{order_id}/latest?{urlencode({'detail': 'FULL', 'dataSpec': '1.1.0'})}"
    latest_status, latest_payload, latest_message = request_json_with_meta(latest_url, headers=headers)
    if latest_status is None:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric latest-file request failed ({latest_message or 'network error'})",
        )
        return out
    if latest_status == 401:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric latest-file auth failed (HTTP 401: {latest_message or 'invalid credentials for this order'})",
        )
        return out
    if latest_status == 404:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric order '{order_id}' not found or not ready yet (HTTP 404; check order status is Complete)",
        )
        return out
    if latest_status >= 400:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric latest-file HTTP {latest_status} ({latest_message or 'request failed'})",
        )
        return out

    files = (
        latest_payload.get("orderDetails", {}).get("files", [])
        if isinstance(latest_payload, dict)
        else []
    )
    if not isinstance(files, list) or not files:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric order '{order_id}' returned no latest files",
        )
        return out

    eccodes_module = get_eccodes_module()
    if eccodes_module is None:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            "GRIB parser unavailable (install python package 'eccodes' or provide wgrib2)",
        )
        return out

    selected_files = select_atmospheric_files(files, target_date)
    if not selected_files:
        set_runtime_note_once(
            SOURCE_MET_OFFICE_ATMOSPHERIC,
            f"Atmospheric order '{order_id}' has no candidate files for temperature/wind",
        )
        return out

    aggregated_samples = init_atmos_samples()
    processed_files = 0
    for file_obj in selected_files:
        file_id = str(file_obj.get("fileId", "")).strip()
        if not file_id:
            continue
        grib_path = download_atmospheric_grib(order_id, file_id, headers)
        if not grib_path:
            continue
        try:
            samples = parse_atmospheric_grib_file(grib_path, target_date, eccodes_module)
            merge_atmos_samples(aggregated_samples, samples)
            processed_files += 1
        except Exception as exc:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric GRIB parse failed for '{file_id}' ({exc})",
            )

    out = samples_to_metrics(aggregated_samples)
    if not out:
        if processed_files == 0:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"No atmospheric GRIB files could be processed for order '{order_id}'",
            )
        else:
            set_runtime_note_once(
                SOURCE_MET_OFFICE_ATMOSPHERIC,
                f"Atmospheric GRIB files parsed but no target-day point values found for {target_date}",
            )
    return out


def fetch_met_office_atmospheric_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not METOFFICE_ATMOS_API_KEY:
        return none_metrics()

    if target_date not in METOFFICE_ATMOS_CACHE:
        METOFFICE_ATMOS_CACHE[target_date] = fetch_met_office_atmospheric_target(target_date)

    coord = rounded_coord(lat, lon)
    return METOFFICE_ATMOS_CACHE[target_date].get(coord, none_metrics())


def fetch_openweather_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not OPENWEATHER_API_KEY:
        return none_metrics()

    def parse_onecall(payload: Dict) -> Dict[str, Optional[float]]:
        daily = payload.get("daily", []) if isinstance(payload, dict) else []
        if not isinstance(daily, list):
            return none_metrics()

        next_7_days: List[Dict[str, Optional[float]]] = []
        for day in daily:
            if not isinstance(day, dict):
                continue
            dt_val = day.get("dt")
            if not isinstance(dt_val, (int, float)):
                continue

            day_date = dt.datetime.fromtimestamp(float(dt_val), tz=dt.timezone.utc).astimezone(TZ).date().isoformat()
            temp_obj = day.get("temp", {}) if isinstance(day.get("temp"), dict) else {}
            tmax = to_float(temp_obj.get("max"))
            tmin = to_float(temp_obj.get("min"))
            wind = mps_to_kmh(to_float(day.get("wind_speed")) or to_float(day.get("wind_gust")))
            rain_chance = clamp_probability_percent(day.get("pop"))
            wind_dir = parse_wind_direction_degrees(day.get("wind_deg"))
            next_7_days.append(
                normalize_daily_forecast_item(
                    date_str=day_date,
                    temp_max=tmax,
                    temp_min=tmin,
                    wind_max=wind,
                    rain_chance=rain_chance,
                    wind_dir=wind_dir,
                )
            )

        next_7_days = next_7_days[:7]
        target = next((x for x in next_7_days if x.get("date") == target_date), None)
        if target:
            out = {
                "temp_max": target.get("temp_max"),
                "temp_min": target.get("temp_min"),
                "wind_max": target.get("wind_max"),
                "rain_chance": target.get("rain_chance"),
                "wind_dir": target.get("wind_dir"),
                "next_7_days": next_7_days,
            }
            return out
        out = none_metrics()
        out["next_7_days"] = next_7_days
        return out

    def parse_forecast_25(payload: Dict) -> Dict[str, Optional[float]]:
        entries = payload.get("list", []) if isinstance(payload, dict) else []
        if not isinstance(entries, list):
            return none_metrics()
        buckets: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: {"temps": [], "winds": [], "dirs": [], "rain_prob": []}
        )

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            ts = None
            dt_val = entry.get("dt")
            if isinstance(dt_val, (int, float)):
                ts = dt.datetime.fromtimestamp(float(dt_val), tz=dt.timezone.utc).astimezone(TZ)
            else:
                raw = entry.get("dt_txt")
                if isinstance(raw, str):
                    try:
                        ts = dt.datetime.fromisoformat(raw.replace(" ", "T")).replace(tzinfo=dt.timezone.utc).astimezone(TZ)
                    except ValueError:
                        ts = None

            if ts is None:
                continue

            main = entry.get("main", {}) if isinstance(entry.get("main"), dict) else {}
            wind = entry.get("wind", {}) if isinstance(entry.get("wind"), dict) else {}
            rain = entry.get("rain", {}) if isinstance(entry.get("rain"), dict) else {}

            t = to_float(main.get("temp"))
            w = to_float(wind.get("speed"))
            wd = parse_wind_direction_degrees(wind.get("deg"))
            pop = clamp_probability_percent(entry.get("pop"))
            rain_mm = to_float(rain.get("3h"))
            if pop is None and rain_mm is not None:
                pop = 100.0 if rain_mm > 0.05 else 0.0

            day_key = ts.date().isoformat()
            bucket = buckets[day_key]
            if t is not None:
                bucket["temps"].append(t)
            if w is not None:
                bucket["winds"].append(mps_to_kmh(w))
            if wd is not None:
                bucket["dirs"].append(wd)
            if pop is not None:
                bucket["rain_prob"].append(pop)

        next_7_days: List[Dict[str, Optional[float]]] = []
        for date_str in sorted(buckets.keys())[:7]:
            bucket = buckets[date_str]
            next_7_days.append(
                normalize_daily_forecast_item(
                    date_str=date_str,
                    temp_max=max(bucket["temps"]) if bucket["temps"] else None,
                    temp_min=min(bucket["temps"]) if bucket["temps"] else None,
                    wind_max=max(bucket["winds"]) if bucket["winds"] else None,
                    rain_chance=max(bucket["rain_prob"]) if bucket["rain_prob"] else None,
                    wind_dir=mean_direction_deg(bucket["dirs"]),
                )
            )

        target = next((x for x in next_7_days if x.get("date") == target_date), None)
        if target:
            out = {
                "temp_max": target.get("temp_max"),
                "temp_min": target.get("temp_min"),
                "wind_max": target.get("wind_max"),
                "rain_chance": target.get("rain_chance"),
                "wind_dir": target.get("wind_dir"),
                "next_7_days": next_7_days,
            }
            return out
        out = none_metrics()
        out["next_7_days"] = next_7_days
        return out

    base_params = {
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
    }

    if OPENWEATHER_MODE in ("auto", "onecall3", "onecall"):
        onecall_params = {**base_params, "exclude": "minutely,hourly,alerts"}
        onecall_url = f"{OPENWEATHER_ONECALL_BASE}?{urlencode(onecall_params)}"
        status, payload, message = request_json_with_meta(onecall_url)
        if status == 200 and payload:
            result = parse_onecall(payload)
            if has_any_metric(result):
                return result
        if status in (401, 403):
            msg = message or f"HTTP {status}"
            if "One Call 3.0 requires a separate subscription" in msg:
                RUNTIME_SOURCE_NOTES[SOURCE_OPENWEATHER] = "One Call 3.0 subscription not enabled"
            else:
                RUNTIME_SOURCE_NOTES[SOURCE_OPENWEATHER] = f"OpenWeather auth failed ({msg})"

    if OPENWEATHER_MODE in ("auto", "forecast2_5", "forecast"):
        forecast_url = f"{OPENWEATHER_FORECAST_BASE}?{urlencode(base_params)}"
        status, payload, message = request_json_with_meta(forecast_url)
        if status == 200 and payload:
            result = parse_forecast_25(payload)
            if has_any_metric(result):
                return result
        if status in (401, 403):
            msg = message or f"HTTP {status}"
            if SOURCE_OPENWEATHER not in RUNTIME_SOURCE_NOTES:
                RUNTIME_SOURCE_NOTES[SOURCE_OPENWEATHER] = f"Forecast API auth failed ({msg})"

    if SOURCE_OPENWEATHER not in RUNTIME_SOURCE_NOTES:
        RUNTIME_SOURCE_NOTES[SOURCE_OPENWEATHER] = "No usable OpenWeather forecast data"

    return none_metrics()


def google_display_date_to_iso(display_date) -> Optional[str]:
    if isinstance(display_date, dict):
        year = display_date.get("year")
        month = display_date.get("month")
        day = display_date.get("day")
        if isinstance(year, int) and isinstance(month, int) and isinstance(day, int):
            try:
                return dt.date(year, month, day).isoformat()
            except ValueError:
                return None
    if isinstance(display_date, str):
        text = display_date.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return text
    return None


def google_temperature_c(temp_obj) -> Optional[float]:
    if not isinstance(temp_obj, dict):
        return None
    value = to_float(temp_obj.get("degrees"))
    if value is None:
        value = to_float(temp_obj.get("value"))
    unit = str(temp_obj.get("unit", "")).upper()
    if "FAHRENHEIT" in unit:
        return fahrenheit_to_celsius(value)
    return value


def google_speed_kmh(speed_obj) -> Optional[float]:
    if not isinstance(speed_obj, dict):
        return None
    value = to_float(speed_obj.get("value"))
    if value is None:
        return None
    unit = str(speed_obj.get("unit", "")).upper()
    if "MILES_PER_HOUR" in unit:
        return mph_to_kmh(value)
    if "METERS_PER_SECOND" in unit:
        return mps_to_kmh(value)
    return value


def google_daypart_wind_kmh(daypart_obj) -> Optional[float]:
    if not isinstance(daypart_obj, dict):
        return None
    wind_obj = daypart_obj.get("wind", {})
    if not isinstance(wind_obj, dict):
        return None
    candidates: List[float] = []
    gust = google_speed_kmh(wind_obj.get("gust"))
    speed = google_speed_kmh(wind_obj.get("speed"))
    if gust is not None:
        candidates.append(gust)
    if speed is not None:
        candidates.append(speed)
    return max(candidates) if candidates else None


def google_daypart_wind_dir_deg(daypart_obj) -> Optional[float]:
    if not isinstance(daypart_obj, dict):
        return None
    wind_obj = daypart_obj.get("wind", {})
    if not isinstance(wind_obj, dict):
        return None
    for key in ("direction", "windDirection", "directionDegrees", "windDirectionDegrees"):
        val = parse_wind_direction_degrees(wind_obj.get(key))
        if val is not None:
            return val
    return None


def google_daypart_rain_chance(daypart_obj) -> Optional[float]:
    if not isinstance(daypart_obj, dict):
        return None
    val = pick_value_from_obj(
        daypart_obj,
        aliases=(
            "precipitation.probability",
            "rainProbability",
            "probabilityOfPrecipitation",
            "precipitationChance",
            "chanceOfRain",
            "pop",
        ),
        avoid_tokens=("amount", "mm"),
    )
    return clamp_probability_percent(val)


def fetch_google_weather_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not GOOGLE_WEATHER_API_KEY and not GOOGLE_WEATHER_ACCESS_TOKEN:
        return none_metrics()

    params = {
        "location.latitude": f"{lat:.6f}",
        "location.longitude": f"{lon:.6f}",
        "days": "7",
        "pageSize": "7",
        "unitsSystem": GOOGLE_WEATHER_UNITS_SYSTEM,
        "languageCode": GOOGLE_WEATHER_LANGUAGE_CODE,
    }
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if GOOGLE_WEATHER_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {GOOGLE_WEATHER_ACCESS_TOKEN}"
        if GOOGLE_WEATHER_QUOTA_PROJECT:
            headers["X-Goog-User-Project"] = GOOGLE_WEATHER_QUOTA_PROJECT
    else:
        params["key"] = GOOGLE_WEATHER_API_KEY
    url = f"{GOOGLE_WEATHER_BASE}?{urlencode(params)}"
    status, payload, message = request_json_with_meta(url, headers=headers if headers else None)
    if status in (401, 403):
        msg = message or f"HTTP {status}"
        if "API keys are not supported by this API" in msg:
            msg = "API key rejected; use GOOGLE_WEATHER_ACCESS_TOKEN (OAuth2 Bearer token)"
        if "requires a quota project" in msg.lower() and not GOOGLE_WEATHER_QUOTA_PROJECT:
            msg += "; set GOOGLE_WEATHER_QUOTA_PROJECT in env"
        RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = f"Google Weather auth failed ({msg})"
    elif status is not None and status >= 400:
        RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = f"Google Weather HTTP {status} ({message or 'request failed'})"

    if not payload:
        if SOURCE_GOOGLE_WEATHER not in RUNTIME_SOURCE_NOTES:
            RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = "Google Weather payload unavailable"
        return none_metrics()

    forecast_days = payload.get("forecastDays", [])
    if not isinstance(forecast_days, list):
        if SOURCE_GOOGLE_WEATHER not in RUNTIME_SOURCE_NOTES:
            RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = "Google Weather forecastDays missing"
        return none_metrics()

    next_7_days: List[Dict[str, Optional[float]]] = []
    for day in forecast_days:
        if not isinstance(day, dict):
            continue
        date_str = google_display_date_to_iso(day.get("displayDate"))
        if not date_str:
            continue

        temp_max = google_temperature_c(day.get("maxTemperature"))
        temp_min = google_temperature_c(day.get("minTemperature"))
        winds: List[float] = []
        dirs: List[float] = []
        rains: List[float] = []
        for part_key in ("daytimeForecast", "nighttimeForecast"):
            part_obj = day.get(part_key)
            w = google_daypart_wind_kmh(part_obj)
            if w is not None:
                winds.append(w)
            d = google_daypart_wind_dir_deg(part_obj)
            if d is not None:
                dirs.append(d)
            r = google_daypart_rain_chance(part_obj)
            if r is not None:
                rains.append(r)

        next_7_days.append(
            normalize_daily_forecast_item(
                date_str=date_str,
                temp_max=temp_max,
                temp_min=temp_min,
                wind_max=max(winds) if winds else None,
                rain_chance=max(rains) if rains else None,
                wind_dir=mean_direction_deg(dirs),
            )
        )

    next_7_days = next_7_days[:7]
    target = next((x for x in next_7_days if x.get("date") == target_date), None)
    if target:
        metrics = {
            "temp_max": target.get("temp_max"),
            "temp_min": target.get("temp_min"),
            "wind_max": target.get("wind_max"),
            "rain_chance": target.get("rain_chance"),
            "wind_dir": target.get("wind_dir"),
            "next_7_days": next_7_days,
        }
        if has_any_metric(metrics):
            return metrics

    if SOURCE_GOOGLE_WEATHER not in RUNTIME_SOURCE_NOTES:
        RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = "No target-day data in Google Weather response"
    out = none_metrics()
    out["next_7_days"] = next_7_days
    return out


def fetch_mwis_latest_pdf_links(limit: int = 5) -> List[str]:
    try:
        resp = requests.get("https://www.mwis.org.uk/forecasts", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        links: List[str] = []
        for match in re.finditer(r'href="([^"]+\.pdf)"', resp.text, flags=re.IGNORECASE):
            href = match.group(1)
            href_l = href.lower()
            if "mwi" not in href_l:
                continue
            if href.startswith("/"):
                href = f"https://www.mwis.org.uk{href}"
            if href not in links:
                links.append(href)
            if len(links) >= limit:
                break
        return links
    except Exception:
        return []


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
            run_date TEXT NOT NULL,
            target_date TEXT NOT NULL,
            source TEXT NOT NULL,
            location TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            temp_max REAL,
            temp_min REAL,
            wind_max REAL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_date, target_date, source, location)
        );

        CREATE INDEX IF NOT EXISTS idx_forecasts_target_source
            ON forecasts(target_date, source, location, run_date);

        CREATE TABLE IF NOT EXISTS actuals (
            date TEXT NOT NULL,
            location TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            temp_max REAL,
            temp_min REAL,
            wind_max REAL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, location)
        );

        CREATE TABLE IF NOT EXISTS source_scores (
            date TEXT NOT NULL,
            source TEXT NOT NULL,
            mae_temp_max REAL,
            mae_temp_min REAL,
            mae_wind_max REAL,
            composite_error REAL,
            confidence REAL,
            sample_count INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, source)
        );

        CREATE TABLE IF NOT EXISTS source_weights (
            date TEXT NOT NULL,
            source TEXT NOT NULL,
            weight REAL NOT NULL,
            rolling_confidence REAL NOT NULL,
            lookback_days INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, source)
        );
        """
    )


def purge_retired_sources(conn: sqlite3.Connection, sources: Sequence[str]) -> None:
    src = [str(s).strip() for s in sources if str(s).strip()]
    if not src:
        return
    placeholders = ",".join("?" for _ in src)
    params = tuple(src)
    conn.execute(f"DELETE FROM forecasts WHERE source IN ({placeholders})", params)
    conn.execute(f"DELETE FROM source_scores WHERE source IN ({placeholders})", params)
    conn.execute(f"DELETE FROM source_weights WHERE source IN ({placeholders})", params)


def upsert_forecast(
    conn: sqlite3.Connection,
    run_date: str,
    target_date: str,
    source: str,
    location: Dict,
    metrics: Dict[str, Optional[float]],
) -> None:
    conn.execute(
        """
        INSERT INTO forecasts (
            run_date, target_date, source, location, lat, lon, temp_max, temp_min, wind_max
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_date, target_date, source, location)
        DO UPDATE SET
            temp_max=excluded.temp_max,
            temp_min=excluded.temp_min,
            wind_max=excluded.wind_max,
            lat=excluded.lat,
            lon=excluded.lon
        """,
        (
            run_date,
            target_date,
            source,
            location["name"],
            location["lat"],
            location["lon"],
            metrics.get("temp_max"),
            metrics.get("temp_min"),
            metrics.get("wind_max"),
        ),
    )


def upsert_actual(conn: sqlite3.Connection, date_str: str, location: Dict, metrics: Dict[str, Optional[float]]) -> None:
    conn.execute(
        """
        INSERT INTO actuals (date, location, lat, lon, temp_max, temp_min, wind_max)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, location)
        DO UPDATE SET
            temp_max=excluded.temp_max,
            temp_min=excluded.temp_min,
            wind_max=excluded.wind_max,
            lat=excluded.lat,
            lon=excluded.lon
        """,
        (
            date_str,
            location["name"],
            location["lat"],
            location["lon"],
            metrics.get("temp_max"),
            metrics.get("temp_min"),
            metrics.get("wind_max"),
        ),
    )


def configured_sources() -> List[str]:
    sources = [SOURCE_OPEN_METEO, SOURCE_MET_NO]
    if METOFFICE_UI_ENABLED:
        sources.append(SOURCE_MET_OFFICE)
    if OPENWEATHER_API_KEY:
        sources.append(SOURCE_OPENWEATHER)
    if GOOGLE_WEATHER_API_KEY or GOOGLE_WEATHER_ACCESS_TOKEN:
        sources.append(SOURCE_GOOGLE_WEATHER)
    return sources


def missing_source_keys() -> List[str]:
    missing: List[str] = []
    if not OPENWEATHER_API_KEY:
        missing.append(SOURCE_OPENWEATHER)
    if not GOOGLE_WEATHER_API_KEY and not GOOGLE_WEATHER_ACCESS_TOKEN:
        missing.append(SOURCE_GOOGLE_WEATHER)
    return missing


def capture_forecasts(
    conn: sqlite3.Connection,
    run_date: str,
    target_date: str,
    sources: Sequence[str],
) -> Dict[str, Dict[str, Dict[str, object]]]:
    fetchers = {
        SOURCE_OPEN_METEO: fetch_open_meteo_forecast,
        SOURCE_MET_NO: fetch_met_no_forecast,
        SOURCE_MET_OFFICE: fetch_met_office_forecast,
        SOURCE_OPENWEATHER: fetch_openweather_forecast,
        SOURCE_GOOGLE_WEATHER: fetch_google_weather_forecast,
    }
    captured: Dict[str, Dict[str, Dict[str, object]]] = defaultdict(dict)

    for loc in LOCATIONS:
        for source in sources:
            fetcher = fetchers[source]
            try:
                metrics = fetcher(loc["lat"], loc["lon"], target_date)
            except Exception as exc:
                set_runtime_note_once(
                    source,
                    f"fetch exception ({exc.__class__.__name__}: {exc})",
                )
                continue
            if not isinstance(metrics, dict):
                set_runtime_note_once(source, "fetcher returned invalid payload")
                continue
            captured[loc["name"]][source] = dict(metrics)
            if has_any_metric(metrics):
                upsert_forecast(conn, run_date, target_date, source, loc, metrics)
    return captured


def capture_actuals(conn: sqlite3.Connection, date_str: str) -> None:
    for loc in LOCATIONS:
        metrics = fetch_open_meteo_actual(loc["lat"], loc["lon"], date_str)
        if has_any_metric(metrics):
            upsert_actual(conn, date_str, loc, metrics)


def available_sources_for_target(conn: sqlite3.Connection, target_date: str, sources: Sequence[str]) -> List[str]:
    available: List[str] = []
    for source in sources:
        count = conn.execute(
            """
            SELECT COUNT(1)
            FROM forecasts
            WHERE target_date = ? AND source = ?
              AND (temp_max IS NOT NULL OR temp_min IS NOT NULL OR wind_max IS NOT NULL)
            """,
            (target_date, source),
        ).fetchone()[0]
        if count and int(count) > 0:
            available.append(source)
    return available


def mean(values: Iterable[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return (sum(vals) / len(vals)) if vals else None


def weighted_composite_error(mae_temp_max: Optional[float], mae_temp_min: Optional[float], mae_wind_max: Optional[float]) -> Optional[float]:
    # Normalize each metric to a rough practical range before blending.
    parts: List[float] = []
    weights: List[float] = []

    if mae_temp_max is not None:
        parts.append(mae_temp_max / 6.0)
        weights.append(0.4)
    if mae_temp_min is not None:
        parts.append(mae_temp_min / 6.0)
        weights.append(0.3)
    if mae_wind_max is not None:
        parts.append(mae_wind_max / 25.0)
        weights.append(0.3)

    if not parts:
        return None

    total_w = sum(weights)
    return sum(p * w for p, w in zip(parts, weights)) / total_w


def confidence_from_error(composite_error: Optional[float]) -> float:
    if composite_error is None:
        return 50.0
    score = 100.0 * math.exp(-composite_error)
    return max(5.0, min(99.0, round(score, 1)))


def evaluate_and_store(conn: sqlite3.Connection, target_date: str, sources: Sequence[str]) -> Dict[str, Dict[str, Optional[float]]]:
    if not sources:
        return {}

    placeholders = ",".join("?" for _ in sources)
    params: List[str] = [target_date, target_date, *sources]

    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT source, location, target_date, MAX(run_date) AS run_date
            FROM forecasts
            WHERE target_date = ?
              AND run_date < ?
              AND source IN ({placeholders})
            GROUP BY source, location, target_date
        )
        SELECT
            f.source,
            f.location,
            f.temp_max,
            f.temp_min,
            f.wind_max,
            a.temp_max AS actual_temp_max,
            a.temp_min AS actual_temp_min,
            a.wind_max AS actual_wind_max
        FROM latest l
        JOIN forecasts f
          ON f.source = l.source
         AND f.location = l.location
         AND f.target_date = l.target_date
         AND f.run_date = l.run_date
        JOIN actuals a
          ON a.date = f.target_date
         AND a.location = f.location
        ORDER BY f.source, f.location
        """,
        params,
    ).fetchall()

    grouped: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

    for r in rows:
        if r["temp_max"] is not None and r["actual_temp_max"] is not None:
            grouped[r["source"]]["temp_max_err"].append(abs(r["temp_max"] - r["actual_temp_max"]))
        if r["temp_min"] is not None and r["actual_temp_min"] is not None:
            grouped[r["source"]]["temp_min_err"].append(abs(r["temp_min"] - r["actual_temp_min"]))
        if r["wind_max"] is not None and r["actual_wind_max"] is not None:
            grouped[r["source"]]["wind_max_err"].append(abs(r["wind_max"] - r["actual_wind_max"]))

    results: Dict[str, Dict[str, Optional[float]]] = {}
    for source in sources:
        temp_max_mae = mean(grouped[source].get("temp_max_err", []))
        temp_min_mae = mean(grouped[source].get("temp_min_err", []))
        wind_max_mae = mean(grouped[source].get("wind_max_err", []))

        composite = weighted_composite_error(temp_max_mae, temp_min_mae, wind_max_mae)
        confidence = confidence_from_error(composite)
        sample_count = max(
            len(grouped[source].get("temp_max_err", [])),
            len(grouped[source].get("temp_min_err", [])),
            len(grouped[source].get("wind_max_err", [])),
        )

        if sample_count == 0:
            continue

        conn.execute(
            """
            INSERT INTO source_scores (
                date, source, mae_temp_max, mae_temp_min, mae_wind_max,
                composite_error, confidence, sample_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, source)
            DO UPDATE SET
                mae_temp_max=excluded.mae_temp_max,
                mae_temp_min=excluded.mae_temp_min,
                mae_wind_max=excluded.mae_wind_max,
                composite_error=excluded.composite_error,
                confidence=excluded.confidence,
                sample_count=excluded.sample_count
            """,
            (
                target_date,
                source,
                temp_max_mae,
                temp_min_mae,
                wind_max_mae,
                composite,
                confidence,
                sample_count,
            ),
        )

        results[source] = {
            "mae_temp_max": temp_max_mae,
            "mae_temp_min": temp_min_mae,
            "mae_wind_max": wind_max_mae,
            "composite_error": composite,
            "confidence": confidence,
            "sample_count": float(sample_count),
        }

    return results


def rolling_confidence(conn: sqlite3.Connection, as_of_date: str, sources: Sequence[str], lookback_days: int) -> Dict[str, Dict[str, float]]:
    if not sources:
        return {}

    placeholders = ",".join("?" for _ in sources)
    rows = conn.execute(
        f"""
        SELECT source, date, confidence, composite_error
        FROM source_scores
        WHERE date <= ? AND source IN ({placeholders})
        ORDER BY date DESC
        """,
        (as_of_date, *sources),
    ).fetchall()

    bucket: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: {"conf": [], "err": []})
    for r in rows:
        source = r["source"]
        if len(bucket[source]["conf"]) >= lookback_days:
            continue
        if r["confidence"] is not None:
            bucket[source]["conf"].append(float(r["confidence"]))
        if r["composite_error"] is not None:
            bucket[source]["err"].append(float(r["composite_error"]))

    out: Dict[str, Dict[str, float]] = {}
    for source in sources:
        conf_vals = bucket[source]["conf"]
        err_vals = bucket[source]["err"]

        avg_conf = sum(conf_vals) / len(conf_vals) if conf_vals else 55.0
        avg_err = sum(err_vals) / len(err_vals) if err_vals else 1.0

        out[source] = {
            "rolling_confidence": round(avg_conf, 1),
            "rolling_error": avg_err,
            "samples": float(len(conf_vals)),
        }

    return out


def compute_weights(rolling: Dict[str, Dict[str, float]], sources: Sequence[str]) -> Dict[str, float]:
    if not sources:
        return {}

    raw: Dict[str, float] = {}
    for source in sources:
        conf = rolling[source]["rolling_confidence"]
        # Softmax-like transform for smoother adaptation.
        raw[source] = math.exp((conf - 50.0) / 20.0)

    total = sum(raw.values())
    if total <= 0:
        return {s: 1.0 / len(sources) for s in sources}

    return {s: raw[s] / total for s in sources}


def store_weights(
    conn: sqlite3.Connection,
    date_str: str,
    weights: Dict[str, float],
    rolling: Dict[str, Dict[str, float]],
    lookback_days: int,
) -> None:
    for source, weight in weights.items():
        conn.execute(
            """
            INSERT INTO source_weights (date, source, weight, rolling_confidence, lookback_days)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, source)
            DO UPDATE SET
                weight=excluded.weight,
                rolling_confidence=excluded.rolling_confidence,
                lookback_days=excluded.lookback_days
            """,
            (
                date_str,
                source,
                float(weight),
                float(rolling[source]["rolling_confidence"]),
                int(lookback_days),
            ),
        )


def latest_forecasts_by_location(
    conn: sqlite3.Connection,
    target_date: str,
    sources: Sequence[str],
) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
    if not sources:
        return {}

    placeholders = ",".join("?" for _ in sources)
    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT source, location, target_date, MAX(run_date) AS run_date
            FROM forecasts
            WHERE target_date = ?
              AND source IN ({placeholders})
            GROUP BY source, location, target_date
        )
        SELECT f.source, f.location, f.temp_max, f.temp_min, f.wind_max
        FROM latest l
        JOIN forecasts f
          ON f.source = l.source
         AND f.location = l.location
         AND f.target_date = l.target_date
         AND f.run_date = l.run_date
        ORDER BY f.location, f.source
        """,
        (target_date, *sources),
    ).fetchall()

    out: Dict[str, Dict[str, Dict[str, Optional[float]]]] = defaultdict(dict)
    for r in rows:
        out[r["location"]][r["source"]] = {
            "temp_max": to_float(r["temp_max"]),
            "temp_min": to_float(r["temp_min"]),
            "wind_max": to_float(r["wind_max"]),
            "rain_chance": None,
            "wind_dir": None,
            "next_7_days": [],
        }
    return out


def merge_live_forecast_extras(
    db_forecasts: Dict[str, Dict[str, Dict[str, object]]],
    live_forecasts: Dict[str, Dict[str, Dict[str, object]]],
) -> None:
    for location, by_source in live_forecasts.items():
        loc_bucket = db_forecasts.setdefault(location, {})
        for source, metrics in by_source.items():
            src_bucket = loc_bucket.setdefault(source, {})
            if "rain_chance" in metrics:
                src_bucket["rain_chance"] = clamp_probability_percent(metrics.get("rain_chance"))
            if "wind_dir" in metrics:
                src_bucket["wind_dir"] = parse_wind_direction_degrees(metrics.get("wind_dir"))

            daily_in = metrics.get("next_7_days")
            if isinstance(daily_in, list):
                daily_out: List[Dict[str, Optional[float]]] = []
                for item in daily_in[:7]:
                    if not isinstance(item, dict):
                        continue
                    date_str = str(item.get("date") or "").strip()
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
                        continue
                    daily_out.append(
                        normalize_daily_forecast_item(
                            date_str=date_str,
                            temp_max=to_float(item.get("temp_max")),
                            temp_min=to_float(item.get("temp_min")),
                            wind_max=to_float(item.get("wind_max")),
                            rain_chance=to_float(item.get("rain_chance")),
                            wind_dir=item.get("wind_dir"),
                        )
                    )
                src_bucket["next_7_days"] = daily_out


def aggregate_next_7_days(
    by_source: Dict[str, object],
    weights: Dict[str, float],
) -> List[Dict[str, Optional[float]]]:
    daily_by_date: Dict[str, Dict[str, Dict[str, Optional[float]]]] = defaultdict(dict)
    for source, raw_days in by_source.items():
        if not isinstance(raw_days, list):
            continue
        for item in raw_days[:7]:
            if not isinstance(item, dict):
                continue
            date_str = str(item.get("date") or "").strip()
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
                continue
            daily_by_date[date_str][source] = {
                "temp_max": to_float(item.get("temp_max")),
                "temp_min": to_float(item.get("temp_min")),
                "wind_max": to_float(item.get("wind_max")),
                "rain_chance": clamp_probability_percent(item.get("rain_chance")),
                "wind_dir": parse_wind_direction_degrees(item.get("wind_dir")),
            }

    out: List[Dict[str, Optional[float]]] = []
    for date_str in sorted(daily_by_date.keys())[:7]:
        values = daily_by_date[date_str]
        temp_max_by_source = {src: row.get("temp_max") for src, row in values.items()}
        temp_min_by_source = {src: row.get("temp_min") for src, row in values.items()}
        wind_max_by_source = {src: row.get("wind_max") for src, row in values.items()}
        rain_by_source = {src: row.get("rain_chance") for src, row in values.items()}
        wind_dir_by_source = {src: row.get("wind_dir") for src, row in values.items()}

        out.append(
            normalize_daily_forecast_item(
                date_str=date_str,
                temp_max=weighted_metric(temp_max_by_source, weights),
                temp_min=weighted_metric(temp_min_by_source, weights),
                wind_max=weighted_metric(wind_max_by_source, weights),
                rain_chance=weighted_metric(rain_by_source, weights),
                wind_dir=weighted_direction_deg(wind_dir_by_source, weights),
            )
        )
    return out


def weighted_metric(values: Dict[str, Optional[float]], weights: Dict[str, float]) -> Optional[float]:
    usable = [(src, val) for src, val in values.items() if val is not None]
    if not usable:
        return None

    total_w = sum(weights.get(src, 0.0) for src, _ in usable)
    if total_w <= 0:
        return sum(v for _, v in usable) / len(usable)

    return sum(weights.get(src, 0.0) * v for src, v in usable) / total_w


def spread(values: Dict[str, Optional[float]]) -> Optional[float]:
    usable = [v for v in values.values() if v is not None]
    if len(usable) < 2:
        return None
    return max(usable) - min(usable)


def fmt(val: Optional[float], ndigits: int = 1) -> str:
    if val is None:
        return "?"
    return f"{val:.{ndigits}f}"


def kmh_to_mph(kmh: Optional[float]) -> Optional[float]:
    if kmh is None:
        return None
    return kmh / 1.60934


def wind_band(kmh: Optional[float]) -> str:
    if kmh is None:
        return "unknown wind"
    if kmh < 15:
        return "light wind"
    if kmh < 30:
        return "moderate wind"
    if kmh < 45:
        return "strong wind"
    return "very strong wind"


def best_window_from_conditions(tmin: Optional[float], tmax: Optional[float], wind_kmh: Optional[float]) -> str:
    if wind_kmh is None:
        return "best window uncertain due limited wind data"
    if wind_kmh >= 45:
        return "best window is brief lower-level outings only"
    if wind_kmh >= 30:
        return "best window is late morning to early afternoon on sheltered routes"
    if tmin is not None and tmin <= -2:
        return "best window is late morning through afternoon after early cold"
    return "best window is mid-morning through mid-afternoon"


def concise_best_window(tmin: Optional[float], tmax: Optional[float], wind_kmh: Optional[float]) -> str:
    window = best_window_from_conditions(tmin, tmax, wind_kmh)
    prefix = "best window is "
    if window.lower().startswith(prefix):
        window = window[len(prefix):]
    return window.capitalize()


def freeze_condition_phrase(tmin: Optional[float], tmax: Optional[float], short: bool = False) -> str:
    long_note = ""
    short_note = ""
    if tmax is not None and tmax <= 1:
        long_note = "Temperatures stay near/below freezing on higher ground."
        short_note = "sub-freezing tops"
    elif tmin is not None and tmin <= 0:
        long_note = "Early frost/ice risk on exposed sections."
        short_note = "early ice risk"

    return short_note if short else long_note


def zone_briefing_line(
    name: str,
    tmin: Optional[float],
    tmax: Optional[float],
    wind_kmh: Optional[float],
    rain_chance: Optional[float],
    wind_dir: Optional[float],
    spread_temp: Optional[float],
    spread_wind: Optional[float],
) -> str:
    if tmin is None and tmax is None and wind_kmh is None and rain_chance is None:
        return f"- {name}: forecast unavailable from current source set."

    temp_part = f"{fmt(tmin)} -> {fmt(tmax)} C"
    wind_part = f"{fmt(wind_kmh)} km/h ({fmt(kmh_to_mph(wind_kmh))} mph)"
    wind_desc = wind_band(wind_kmh)
    wind_dir_part = ""
    if wind_dir is not None:
        wind_dir_part = f" from {direction_to_cardinal(wind_dir)} ({fmt(wind_dir, 0)}°)"
    rain_part = ""
    if rain_chance is not None:
        rain_part = f" Rain chance around {fmt(rain_chance, 0)}%."

    stability_notes: List[str] = []
    if spread_temp is not None and spread_temp >= 4:
        stability_notes.append("higher model spread on temperature")
    if spread_wind is not None and spread_wind >= 15:
        stability_notes.append("higher model spread on wind")
    stability_text = ""
    if stability_notes:
        stability_text = "; " + ", ".join(stability_notes)

    freezing_line = freeze_condition_phrase(tmin, tmax)
    freezing_suffix = f" {freezing_line}" if freezing_line else ""

    return (
        f"- {name} - {temp_part}. {wind_desc}{wind_dir_part}, peaking near {wind_part}{stability_text}. "
        f"{best_window_from_conditions(tmin, tmax, wind_kmh)}.{freezing_suffix}{rain_part}"
    )


def suitability_level(score: int) -> str:
    if score >= 2:
        return "Good"
    if score >= 0:
        return "Fair"
    return "Poor"


def activity_suitability(
    tmin: Optional[float],
    tmax: Optional[float],
    wind_kmh: Optional[float],
) -> Dict[str, str]:
    cycling = 1
    hiking = 1
    skiing = -1

    if wind_kmh is not None:
        if wind_kmh >= 40:
            cycling -= 3
            hiking -= 2
            skiing -= 1
        elif wind_kmh >= 30:
            cycling -= 2
            hiking -= 1
        elif wind_kmh <= 18:
            cycling += 1
            hiking += 1
            skiing += 0

    if tmin is not None and tmax is not None:
        if tmax >= 22:
            cycling -= 1
            hiking -= 1
            skiing -= 2
        if tmin <= -3:
            cycling -= 2
            hiking -= 1
            skiing += 1
        if tmax <= 3:
            skiing += 2
        elif tmax <= 6:
            skiing += 1
        elif tmax >= 10:
            skiing -= 2
        if 3 <= tmax <= 18 and tmin >= -1:
            cycling += 1
            hiking += 1

    return {
        "cycling": suitability_level(cycling),
        "hiking": suitability_level(hiking),
        "skiing": suitability_level(skiing),
    }


def suitability_go_line(
    tmin: Optional[float],
    tmax: Optional[float],
    wind_kmh: Optional[float],
    suitability: Dict[str, str],
) -> str:
    if wind_kmh is not None and wind_kmh >= 45:
        return "Go only if you are comfortable with very exposed, windy terrain."
    if tmax is not None and tmax <= 2:
        return "Go if you are equipped for wintry ground; skiing is favored over cycling."
    if suitability.get("cycling") == "Good" and suitability.get("hiking") == "Good":
        return "Go if you are fine with cool, potentially damp conditions; comfort is straightforward with layered kit."
    return "Go with standard hill caution; conditions are generally manageable on sheltered routes."


def suitability_cautions_line(tmin: Optional[float], tmax: Optional[float], wind_kmh: Optional[float]) -> str:
    cautions: List[str] = []
    if tmin is not None and tmin <= 0:
        cautions.append("freeze/thaw patches can make paths and roads slick early/late")
    if wind_kmh is not None and wind_kmh >= 30:
        cautions.append("exposed ridges and plateaus will feel significantly windier")
    if tmax is not None and tmax >= 18:
        cautions.append("unexpected warm spells can soften snowpack and increase slush")
    if not cautions:
        return "No major wind/temperature hazards indicated; still verify local rain and visibility before departure."
    return "; ".join(cautions).capitalize() + "."


def suitability_adjustments_line(
    tmin: Optional[float],
    wind_kmh: Optional[float],
    suitability: Dict[str, str],
) -> str:
    adjustments: List[str] = []
    if wind_kmh is not None and wind_kmh >= 30:
        adjustments.append("pack a windproof shell and full-finger gloves")
    if tmin is not None and tmin <= 0:
        adjustments.append("carry traction aid for icy sections")
    if suitability.get("cycling") != "Good":
        adjustments.append("reduce tyre pressure slightly and leave extra braking margin on descents")
    if suitability.get("skiing") == "Good":
        adjustments.append("bring goggles and cold-weather layers for exposed sections")
    if not adjustments:
        adjustments.append("carry a light shell and one dry spare layer for after activity")
    return "; ".join(adjustments).capitalize() + "."


def activity_suitability_block(
    name: str,
    tmin: Optional[float],
    tmax: Optional[float],
    wind_kmh: Optional[float],
) -> List[str]:
    suitability = activity_suitability(tmin, tmax, wind_kmh)
    lines: List[str] = []
    lines.append(f"- {name}")
    lines.append(
        f"  Go: {suitability_go_line(tmin, tmax, wind_kmh, suitability)}"
    )
    lines.append(
        f"  Cautions: {suitability_cautions_line(tmin, tmax, wind_kmh)}"
    )
    lines.append(
        f"  Nice-to-have adjustments: {suitability_adjustments_line(tmin, wind_kmh, suitability)}"
    )
    lines.append(
        f"  Ratings: Cycling {suitability['cycling']}, Hiking {suitability['hiking']}, Skiing {suitability['skiing']}"
    )
    return lines


def compact_condition_hint(tmin: Optional[float], tmax: Optional[float], wind_kmh: Optional[float]) -> str:
    hints: List[str] = []
    if tmin is not None and tmin <= 0:
        hints.append("icy AM")
    if wind_kmh is not None and wind_kmh >= 30:
        hints.append("gusty tops")
    if not hints:
        hints.append("settled")
    return ", ".join(hints)


def rating_initial(label: str) -> str:
    return (label[:1] if label else "?").upper()


def summarize_next_7_days(days_obj) -> str:
    if not isinstance(days_obj, list):
        return "7-day forecast unavailable."
    days = [d for d in days_obj if isinstance(d, dict) and isinstance(d.get("date"), str)]
    if not days:
        return "7-day forecast unavailable."

    temps_max = [to_float(d.get("temp_max")) for d in days if to_float(d.get("temp_max")) is not None]
    temps_min = [to_float(d.get("temp_min")) for d in days if to_float(d.get("temp_min")) is not None]
    winds = [to_float(d.get("wind_max")) for d in days if to_float(d.get("wind_max")) is not None]
    rains = [to_float(d.get("rain_chance")) for d in days if to_float(d.get("rain_chance")) is not None]
    dirs = [to_float(d.get("wind_dir")) for d in days if to_float(d.get("wind_dir")) is not None]

    start_date = str(days[0].get("date"))
    end_date = str(days[min(len(days), 7) - 1].get("date"))
    span = f"{start_date} -> {end_date}"
    temp_text = (
        f"Tmax {fmt(max(temps_max))}C, Tmin {fmt(min(temps_min))}C"
        if temps_max and temps_min
        else "temperature range unavailable"
    )
    wind_text = (
        f"peak wind {fmt(max(winds))} km/h from {direction_to_cardinal(mean_direction_deg(dirs))}"
        if winds
        else "wind trend unavailable"
    )
    rain_text = f"rain chance up to {fmt(max(rains), 0)}%" if rains else "rain chance unavailable"
    return f"{span} | {temp_text}, {wind_text}, {rain_text}."


def compute_zone_rows(
    available_sources: Sequence[str],
    forecasts: Dict[str, Dict[str, Dict[str, object]]],
    weights: Dict[str, float],
) -> Dict[str, Dict[str, Optional[float]]]:
    zone_rows: Dict[str, Dict[str, Optional[float]]] = {}
    source_list = list(available_sources)

    for loc in LOCATIONS:
        name = loc["name"]
        source_rows = forecasts.get(name, {})
        if not source_list:
            source_keys = list(source_rows.keys())
        else:
            source_keys = source_list

        tmax_by_source = {s: source_rows.get(s, {}).get("temp_max") for s in source_keys}
        tmin_by_source = {s: source_rows.get(s, {}).get("temp_min") for s in source_keys}
        wind_by_source = {s: source_rows.get(s, {}).get("wind_max") for s in source_keys}
        rain_by_source = {s: source_rows.get(s, {}).get("rain_chance") for s in source_keys}
        wind_dir_by_source = {s: source_rows.get(s, {}).get("wind_dir") for s in source_keys}
        next7_by_source = {s: source_rows.get(s, {}).get("next_7_days") for s in source_keys}

        tmax = weighted_metric(tmax_by_source, weights)
        tmin = weighted_metric(tmin_by_source, weights)
        wind = weighted_metric(wind_by_source, weights)
        rain = weighted_metric(rain_by_source, weights)
        wind_dir = weighted_direction_deg(wind_dir_by_source, weights)
        next_7_days = aggregate_next_7_days(next7_by_source, weights)

        zone_rows[name] = {
            "temp_max": tmax,
            "temp_min": tmin,
            "wind_max": wind,
            "rain_chance": rain,
            "wind_dir": wind_dir,
            "spread_temp": spread(tmax_by_source),
            "spread_wind": spread(wind_by_source),
            "next_7_days": next_7_days,
        }

    return zone_rows


def build_full_briefing(
    forecast_date: str,
    eval_date: str,
    configured_sources: Sequence[str],
    available_sources: Sequence[str],
    skipped_error_sources: Sequence[str],
    missing_sources: Sequence[str],
    zone_rows: Dict[str, Dict[str, Optional[float]]],
    rolling: Dict[str, Dict[str, float]],
    weights: Dict[str, float],
    eval_results: Dict[str, Dict[str, Optional[float]]],
    mwis_links: List[str],
) -> str:
    lines: List[str] = []
    lines.append(f"Scottish mountains forecast (adaptive) - {forecast_date} (UK)")
    lines.append("Sources benchmarked daily; ensemble weights auto-updated.")
    lines.append("")

    report_sources = [s for s in configured_sources if s in available_sources]

    lines.append("1) Latest forecast by zone (with briefing)")

    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        lines.append(
            zone_briefing_line(
                name,
                row.get("temp_min"),
                row.get("temp_max"),
                row.get("wind_max"),
                row.get("rain_chance"),
                row.get("wind_dir"),
                row.get("spread_temp"),
                row.get("spread_wind"),
            )
        )
        lines.append(f"  7-day: {summarize_next_7_days(row.get('next_7_days'))}")

    lines.append("")
    lines.append(f"2) Latest benchmark ({eval_date})")
    if eval_results:
        for source in report_sources:
            if source not in eval_results:
                continue
            r = eval_results[source]
            lines.append(
                f"- {SOURCE_LABELS.get(source, source)}: conf {fmt(r.get('confidence'))}%, "
                f"MAE Tmax {fmt(r.get('mae_temp_max'))}C, "
                f"Tmin {fmt(r.get('mae_temp_min'))}C, "
                f"Wind {fmt(r.get('mae_wind_max'))} km/h"
            )
    else:
        lines.append("- Not enough history yet (scores start filling after 1 full day).")

    lines.append("")
    lines.append("3) Suitability for Cycling/Hiking/Skiing")
    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        block_lines = activity_suitability_block(
            name,
            row.get("temp_min"),
            row.get("temp_max"),
            row.get("wind_max"),
        )
        lines.extend(block_lines)
        lines.append("")

    lines.append("")
    lines.append(f"4) Forecasting source with confidence % (last {LOOKBACK_DAYS} scored days)")
    for source in report_sources:
        conf = rolling[source]["rolling_confidence"]
        w = weights.get(source, 0.0) * 100.0
        samples = int(rolling[source]["samples"])
        lines.append(f"- {SOURCE_LABELS.get(source, source)}: {fmt(conf)}% confidence (weight {fmt(w)}%, samples {samples})")
    if not report_sources:
        lines.append("- No source produced usable metrics for this run.")

    if skipped_error_sources:
        skipped_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in skipped_error_sources)
        lines.append(f"- Skipped errored sources this run: {skipped_labels}")

    for source in missing_sources:
        env_name = {
            SOURCE_OPENWEATHER: "OPENWEATHER_API_KEY",
            SOURCE_GOOGLE_WEATHER: "GOOGLE_WEATHER_ACCESS_TOKEN",
        }.get(source, "API_KEY")
        lines.append(f"- {SOURCE_LABELS.get(source, source)}: not configured ({env_name} missing)")

    lines.append("")
    lines.append("5) Latest Full PDF links")
    if mwis_links:
        for link in mwis_links:
            lines.append(f"- {link}")
    else:
        lines.append("- No PDF links found in this run.")

    return "\n".join(lines)


def build_compact_briefing(
    forecast_date: str,
    eval_date: str,
    configured_sources: Sequence[str],
    available_sources: Sequence[str],
    skipped_error_sources: Sequence[str],
    missing_sources: Sequence[str],
    zone_rows: Dict[str, Dict[str, Optional[float]]],
    rolling: Dict[str, Dict[str, float]],
    weights: Dict[str, float],
    eval_results: Dict[str, Dict[str, Optional[float]]],
    mwis_links: List[str],
) -> str:
    report_sources = [s for s in configured_sources if s in available_sources]

    lines: List[str] = []
    lines.append(f"Scottish mountains quick briefing - {forecast_date} (UK)")
    lines.append("Compact daily snapshot; run `--mode full` for the detailed 5-section report.")
    lines.append("")

    lines.append("Zone snapshot (min→max °C, peak wind km/h, rain %, wind dir):")
    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        tmin = row.get("temp_min")
        tmax = row.get("temp_max")
        wind = row.get("wind_max")
        rain = row.get("rain_chance")
        wind_dir = row.get("wind_dir")
        window = concise_best_window(tmin, tmax, wind)
        freeze_short = freeze_condition_phrase(tmin, tmax, short=True)
        spread_flags: List[str] = []
        if row.get("spread_temp") is not None and row.get("spread_temp") >= 4:
            spread_flags.append("temp spread high")
        if row.get("spread_wind") is not None and row.get("spread_wind") >= 15:
            spread_flags.append("wind spread high")
        extras = "; ".join(flag for flag in [freeze_short, *spread_flags] if flag)
        extras_text = f" | {extras}" if extras else ""
        dir_text = direction_to_cardinal(wind_dir) if wind_dir is not None else "?"
        lines.append(
            f"- {name}: {fmt(tmin)}→{fmt(tmax)}°C, {fmt(wind)} km/h ({wind_band(wind)}), "
            f"rain {fmt(rain, 0)}%, dir {dir_text}. "
            f"Window {window}{extras_text}"
        )

    lines.append("")
    lines.append("7-day outlook (ensemble):")
    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        lines.append(f"- {name}: {summarize_next_7_days(row.get('next_7_days'))}")

    lines.append("")
    lines.append("Activities (Cycling/Hiking/Skiing ratings):")
    for loc in LOCATIONS:
        name = loc["name"]
        row = zone_rows.get(name, {})
        suitability = activity_suitability(row.get("temp_min"), row.get("temp_max"), row.get("wind_max"))
        ratings = "/".join(
            [
                rating_initial(suitability.get("cycling")),
                rating_initial(suitability.get("hiking")),
                rating_initial(suitability.get("skiing")),
            ]
        )
        hint = compact_condition_hint(row.get("temp_min"), row.get("temp_max"), row.get("wind_max"))
        lines.append(f"- {name}: {ratings} ({hint})")

    lines.append("")
    lines.append(f"Benchmarks ({eval_date}):")
    if eval_results:
        for source in report_sources:
            if source not in eval_results:
                continue
            r = eval_results[source]
            lines.append(
                f"- {SOURCE_LABELS.get(source, source)}: conf {fmt(r.get('confidence'))}% | "
                f"Tmax {fmt(r.get('mae_temp_max'))}C, Tmin {fmt(r.get('mae_temp_min'))}C, Wind {fmt(r.get('mae_wind_max'))} km/h"
            )
    else:
        lines.append("- Not enough scored history yet.")

    lines.append("")
    lines.append(f"Sources & weights (last {LOOKBACK_DAYS} days):")
    if report_sources:
        for source in report_sources:
            conf = rolling[source]["rolling_confidence"]
            samples = int(rolling[source]["samples"])
            weight_pct = weights.get(source, 0.0) * 100.0
            lines.append(
                f"- {SOURCE_LABELS.get(source, source)}: {fmt(conf)}% conf, weight {fmt(weight_pct)}%, samples {samples}"
            )
    else:
        lines.append("- No source produced usable metrics for this run.")

    if skipped_error_sources:
        skipped_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in skipped_error_sources)
        lines.append(f"Skipped due to fetch errors: {skipped_labels}")

    if missing_sources:
        missing_labels = ", ".join(SOURCE_LABELS.get(s, s) for s in missing_sources)
        lines.append(f"Missing API keys: {missing_labels}")

    lines.append("")
    lines.append("MWIS PDFs:")
    if mwis_links:
        for link in mwis_links[:3]:
            lines.append(f"- {link}")
        if len(mwis_links) > 3:
            lines.append(f"- +{len(mwis_links) - 3} more at mwis.org.uk/forecasts")
    else:
        lines.append("- No PDF links found in this run.")

    return "\n".join(lines)


def main() -> None:
    global LOCATIONS
    args = parse_args()
    mode = "compact" if args.compact else args.mode

    if str(args.add_city or "").strip():
        ok, message = add_city_to_watchlist_sheet(
            city_name=str(args.add_city).strip(),
            country_code=str(args.city_country or "").strip(),
        )
        print(message)
        update_heartbeat_state(
            "weather_watchlist_update",
            "ok" if ok else "failed",
            {
                "city": str(args.add_city).strip(),
                "country": str(args.city_country or "").strip().upper(),
                "ok": bool(ok),
                "message": message,
            },
        )
        if not ok:
            raise SystemExit(1)
        return

    LOCATIONS = load_locations_from_watchlist()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_SOURCE_NOTES.clear()

    today = london_today()
    run_date = iso(today)
    forecast_date = iso(today + dt.timedelta(days=1))
    eval_date = iso(today - dt.timedelta(days=1))

    active_sources = configured_sources()
    missing_sources = missing_source_keys()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        init_db(conn)
        purge_retired_sources(conn, RETIRED_SOURCES)

        live_forecasts = capture_forecasts(conn, run_date=run_date, target_date=forecast_date, sources=active_sources)
        capture_actuals(conn, date_str=eval_date)
        available_sources = available_sources_for_target(conn, target_date=forecast_date, sources=active_sources)
        skipped_error_sources = [s for s in active_sources if s not in available_sources and s in RUNTIME_SOURCE_NOTES]

        eval_results = evaluate_and_store(conn, target_date=eval_date, sources=active_sources)
        rolling = rolling_confidence(conn, as_of_date=eval_date, sources=active_sources, lookback_days=LOOKBACK_DAYS)
        weight_sources = available_sources if available_sources else active_sources
        weights = compute_weights(rolling, weight_sources)
        store_weights(conn, date_str=run_date, weights=weights, rolling=rolling, lookback_days=LOOKBACK_DAYS)

        forecasts = latest_forecasts_by_location(conn, target_date=forecast_date, sources=available_sources)
        merge_live_forecast_extras(forecasts, live_forecasts)
        zone_rows = compute_zone_rows(available_sources=available_sources, forecasts=forecasts, weights=weights)
        mwis_links = fetch_mwis_latest_pdf_links(limit=5)

        if mode == "compact":
            briefing = build_compact_briefing(
                forecast_date=forecast_date,
                eval_date=eval_date,
                configured_sources=active_sources,
                available_sources=available_sources,
                skipped_error_sources=skipped_error_sources,
                missing_sources=missing_sources,
                zone_rows=zone_rows,
                rolling=rolling,
                weights=weights,
                eval_results=eval_results,
                mwis_links=mwis_links,
            )
        else:
            briefing = build_full_briefing(
                forecast_date=forecast_date,
                eval_date=eval_date,
                configured_sources=active_sources,
                available_sources=available_sources,
                skipped_error_sources=skipped_error_sources,
                missing_sources=missing_sources,
                zone_rows=zone_rows,
                rolling=rolling,
                weights=weights,
                eval_results=eval_results,
                mwis_links=mwis_links,
            )

        try:
            weather_site_payloads = build_weather_site_payloads(
                conn=conn,
                mode=mode,
                run_date=run_date,
                forecast_date=forecast_date,
                eval_date=eval_date,
                configured_sources=active_sources,
                available_sources=available_sources,
                skipped_error_sources=skipped_error_sources,
                missing_sources=missing_sources,
                zone_rows=zone_rows,
                forecasts=forecasts,
                rolling=rolling,
                weights=weights,
                eval_results=eval_results,
                mwis_links=mwis_links,
            )
            weather_site_sync = publish_weather_site_json(
                payloads=weather_site_payloads,
                run_date=run_date,
                forecast_date=forecast_date,
            )
        except Exception as exc:
            weather_site_sync = {
                "enabled": bool(WEATHER_SITE_SYNC_ENABLED),
                "status": "failed_exception",
                "repo": str(resolve_weather_site_repo_dir() or ""),
                "files": [],
                "changed_files": [],
                "error": f"{exc.__class__.__name__}: {exc}",
            }

        persist_weather_memory_entry(
            run_date=run_date,
            forecast_date=forecast_date,
            eval_date=eval_date,
            active_sources=active_sources,
            available_sources=available_sources,
            missing_sources=missing_sources,
            skipped_error_sources=skipped_error_sources,
        )
        persist_weather_site_sync_note(run_date=run_date, sync_result=weather_site_sync)
        update_heartbeat_state(
            "weather_briefing",
            "ok",
            {
                "run_date": run_date,
                "forecast_date": forecast_date,
                "eval_date": eval_date,
                "active_source_count": len(active_sources),
                "available_source_count": len(available_sources),
                "missing_source_count": len(missing_sources),
                "skipped_error_source_count": len(skipped_error_sources),
                "db_path": str(DB_PATH),
                "weather_site_sync_status": weather_site_sync.get("status"),
                "weather_site_repo": weather_site_sync.get("repo"),
                "weather_site_changed_files": len(weather_site_sync.get("changed_files") or []),
                "weather_site_sync_error": weather_site_sync.get("error"),
            },
        )
        print(briefing)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        today = london_today()
        forecast_date = iso(today + dt.timedelta(days=1))
        update_heartbeat_state(
            "weather_briefing",
            "failed",
            {
                "forecast_date": forecast_date,
                "error": f"{exc.__class__.__name__}: {exc}",
                "db_path": str(DB_PATH),
            },
        )
        print(f"Scottish mountains forecast (adaptive) - {forecast_date} (UK)")
        print("Daily report generated with degraded mode due internal error.")
        print(f"Error: {exc.__class__.__name__}: {exc}")
