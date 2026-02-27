#!/usr/bin/env python3
"""Adaptive Scotland mountain weather briefing with daily source benchmarking.

Daily run behavior:
1) Store one-day-ahead forecasts from multiple sources.
2) Store yesterday actuals (reference: Open-Meteo archive daily values).
3) Benchmark each source against actuals and update confidence.
4) Recompute source weights for ensemble forecast logic.
5) Print concise Telegram-ready briefing for tomorrow.

Optional API keys:
- METOFFICE_API_KEY   (Met Office DataHub site-specific endpoint)
- METOFFICE_ATMOS_API_KEY (Met Office DataHub atmospheric-models endpoint; falls back to METOFFICE_API_KEY)
- METOFFICE_ATMOS_ORDER_ID (optional: pin a specific atmospheric orderId)
- OPENWEATHER_API_KEY (OpenWeather One Call 3.0 / Forecast endpoints)
- GOOGLE_WEATHER_API_KEY (Google Weather API forecast.days.lookup endpoint)
- GOOGLE_WEATHER_ACCESS_TOKEN (preferred for Google Weather OAuth2)

Atmospheric GRIB extraction requires Python package `eccodes`.
"""

from __future__ import annotations

import datetime as dt
import base64
import json
import math
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import requests

LOCATIONS = [
    {"name": "Glencoe", "lat": 56.68, "lon": -5.10},
    {"name": "Ben Nevis", "lat": 56.7969, "lon": -5.0036},
    {"name": "Glenshee", "lat": 56.8526, "lon": -3.4258},
    {"name": "Cairngorms", "lat": 57.1, "lon": -3.7},
]

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

OPENMETEO_FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1/archive"
MET_NO_BASE = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
MET_NO_USER_AGENT = "AIBot-WeatherBenchmark/1.0 (felixlee@example.com)"

ENV_FALLBACK_FILES = [
    Path(__file__).resolve().parent.parent / ".env",
    Path("/home/felixlee/.openclaw/.env"),
    Path("/home/felixlee/Desktop/aibot/.env"),
    Path.home() / ".openclaw/.env",
    Path.home() / "Desktop/aibot/.env",
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


# Met Office DataHub site-specific (from DataHub docs and examples).
METOFFICE_BASE = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/three-hourly"
METOFFICE_API_KEY = read_env_value("METOFFICE_API_KEY", "")
METOFFICE_DATASOURCE = read_env_value("METOFFICE_DATASOURCE", "BD1").strip() or "BD1"
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

RUNTIME_SOURCE_NOTES: Dict[str, str] = {}

LOOKBACK_DAYS = 14
REQUEST_TIMEOUT = 20
DEFAULT_NONE_METRICS = {"temp_max": None, "temp_min": None, "wind_max": None}
METOFFICE_ATMOS_CACHE: Dict[str, Dict[Tuple[float, float], Dict[str, Optional[float]]]] = {}
METOFFICE_ATMOS_CACHE_DIR = DATA_DIR / "metoffice_atmos_grib"


def london_today() -> dt.date:
    return dt.datetime.now(TZ).date()


def iso(d: dt.date) -> str:
    return d.isoformat()


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
    return dict(DEFAULT_NONE_METRICS)


def has_any_metric(metrics: Dict[str, Optional[float]]) -> bool:
    return any(metrics.get(k) is not None for k in ("temp_max", "temp_min", "wind_max"))


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
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    idx = times.index(target_date)
    return {
        "temp_max": value_at(daily.get("temperature_2m_max", []), idx),
        "temp_min": value_at(daily.get("temperature_2m_min", []), idx),
        "wind_max": value_at(daily.get("wind_speed_10m_max", []), idx),
    }


def fetch_open_meteo_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,wind_speed_10m_max",
        "forecast_days": 3,
        "timezone": "Europe/London",
    }
    url = f"{OPENMETEO_FORECAST_BASE}?{urlencode(params)}"
    payload = request_json(url)
    if not payload:
        return {"temp_max": None, "temp_min": None, "wind_max": None}
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
        return {"temp_max": None, "temp_min": None, "wind_max": None}
    return extract_open_meteo_daily(payload, date_str)


def fetch_met_no_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    url = f"{MET_NO_BASE}?lat={lat}&lon={lon}"
    payload = request_json(url, headers={"User-Agent": MET_NO_USER_AGENT})
    if not payload:
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    timeseries = (
        payload.get("properties", {})
        .get("timeseries", [])
        if isinstance(payload, dict)
        else []
    )
    temps: List[float] = []
    winds_mps: List[float] = []

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

        if ts.date().isoformat() != target_date:
            continue

        details = item.get("data", {}).get("instant", {}).get("details", {})
        if not isinstance(details, dict):
            continue

        t = to_float(details.get("air_temperature"))
        w = to_float(details.get("wind_speed"))
        if t is not None:
            temps.append(t)
        if w is not None:
            winds_mps.append(w)

    return {
        "temp_max": max(temps) if temps else None,
        "temp_min": min(temps) if temps else None,
        "wind_max": mps_to_kmh(max(winds_mps) if winds_mps else None),
    }


def fetch_met_office_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not METOFFICE_API_KEY:
        return none_metrics()

    params = {
        "datasource": METOFFICE_DATASOURCE,
        "includeLocationName": "true",
        "excludeParameterMetadata": "true",
        "latitude": f"{lat:.6f}",
        "longitude": f"{lon:.6f}",
    }
    url = f"{METOFFICE_BASE}?{urlencode(params)}"
    status, payload, message = request_json_with_meta(url, headers={"apikey": METOFFICE_API_KEY})

    if status is None:
        set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office request failed ({message or 'network error'})")
        return none_metrics()

    if status == 401:
        set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office auth failed (HTTP 401: {message or 'missing/invalid apikey header'})")
        return none_metrics()

    if status == 403:
        hint = metoffice_subscription_hint(METOFFICE_API_KEY)
        if hint:
            set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office auth failed (HTTP 403: {hint})")
        else:
            set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office auth failed (HTTP 403: {message or 'resource forbidden'})")
        return none_metrics()

    if status >= 400:
        set_runtime_note_once(SOURCE_MET_OFFICE, f"Met Office HTTP {status} ({message or 'request failed'})")
        return none_metrics()

    if not payload:
        set_runtime_note_once(SOURCE_MET_OFFICE, "Met Office payload unavailable")
        return none_metrics()

    features = payload.get("features", [])
    if not isinstance(features, list) or not features:
        set_runtime_note_once(SOURCE_MET_OFFICE, "Met Office response missing features")
        return none_metrics()

    props = features[0].get("properties", {}) if isinstance(features[0], dict) else {}
    series = props.get("timeSeries") or props.get("timeseries") or []
    if not isinstance(series, list):
        set_runtime_note_once(SOURCE_MET_OFFICE, "Met Office response missing timeSeries array")
        return none_metrics()

    temps: List[float] = []
    winds_mps: List[float] = []

    temp_aliases = [
        "screenTemperature",
        "airTemperature",
        "temperature",
        "temp",
        "dayMaxScreenTemperature",
        "nightMinScreenTemperature",
    ]
    wind_aliases = [
        "windSpeed10m",
        "windSpeed",
        "max10mWindSpeed",
        "windGustSpeed10m",
        "windGustSpeed",
        "midday10MWindSpeed",
    ]

    for item in series:
        if not isinstance(item, dict):
            continue

        raw_time = item.get("time")
        if isinstance(raw_time, str):
            try:
                ts = dt.datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(TZ)
            except ValueError:
                continue
            if ts.date().isoformat() != target_date:
                continue
        else:
            continue

        candidate_objs = [
            item,
            item.get("data", {}).get("instant", {}).get("details", {}),
            item.get("parameters", {}),
            item.get("details", {}),
        ]

        t_val = None
        w_val = None
        for obj in candidate_objs:
            if t_val is None:
                t_val = pick_value_from_obj(obj, temp_aliases, avoid_tokens=("feels", "apparent"))
            if w_val is None:
                w_val = pick_value_from_obj(obj, wind_aliases, avoid_tokens=("direction",))

        if t_val is not None:
            temps.append(t_val)
        if w_val is not None:
            winds_mps.append(w_val)

    metrics = {
        "temp_max": max(temps) if temps else None,
        "temp_min": min(temps) if temps else None,
        "wind_max": mps_to_kmh(max(winds_mps) if winds_mps else None),
    }
    if not has_any_metric(metrics):
        set_runtime_note_once(SOURCE_MET_OFFICE, "No target-day Met Office metrics found in timeSeries")
    return metrics


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
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    def parse_onecall(payload: Dict) -> Dict[str, Optional[float]]:
        daily = payload.get("daily", []) if isinstance(payload, dict) else []
        if not isinstance(daily, list):
            return {"temp_max": None, "temp_min": None, "wind_max": None}

        for day in daily:
            if not isinstance(day, dict):
                continue
            dt_val = day.get("dt")
            if not isinstance(dt_val, (int, float)):
                continue

            day_date = dt.datetime.fromtimestamp(float(dt_val), tz=dt.timezone.utc).astimezone(TZ).date().isoformat()
            if day_date != target_date:
                continue

            temp_obj = day.get("temp", {}) if isinstance(day.get("temp"), dict) else {}
            tmax = to_float(temp_obj.get("max"))
            tmin = to_float(temp_obj.get("min"))
            wind = mps_to_kmh(to_float(day.get("wind_speed")))
            return {"temp_max": tmax, "temp_min": tmin, "wind_max": wind}

        return {"temp_max": None, "temp_min": None, "wind_max": None}

    def parse_forecast_25(payload: Dict) -> Dict[str, Optional[float]]:
        entries = payload.get("list", []) if isinstance(payload, dict) else []
        if not isinstance(entries, list):
            return {"temp_max": None, "temp_min": None, "wind_max": None}

        temps: List[float] = []
        winds_mps: List[float] = []

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

            if ts is None or ts.date().isoformat() != target_date:
                continue

            main = entry.get("main", {}) if isinstance(entry.get("main"), dict) else {}
            wind = entry.get("wind", {}) if isinstance(entry.get("wind"), dict) else {}

            t = to_float(main.get("temp"))
            w = to_float(wind.get("speed"))
            if t is not None:
                temps.append(t)
            if w is not None:
                winds_mps.append(w)

        return {
            "temp_max": max(temps) if temps else None,
            "temp_min": min(temps) if temps else None,
            "wind_max": mps_to_kmh(max(winds_mps) if winds_mps else None),
        }

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

    return {"temp_max": None, "temp_min": None, "wind_max": None}


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


def fetch_google_weather_forecast(lat: float, lon: float, target_date: str) -> Dict[str, Optional[float]]:
    if not GOOGLE_WEATHER_API_KEY and not GOOGLE_WEATHER_ACCESS_TOKEN:
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    params = {
        "location.latitude": f"{lat:.6f}",
        "location.longitude": f"{lon:.6f}",
        "days": "3",
        "pageSize": "3",
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
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    forecast_days = payload.get("forecastDays", [])
    if not isinstance(forecast_days, list):
        if SOURCE_GOOGLE_WEATHER not in RUNTIME_SOURCE_NOTES:
            RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = "Google Weather forecastDays missing"
        return {"temp_max": None, "temp_min": None, "wind_max": None}

    for day in forecast_days:
        if not isinstance(day, dict):
            continue
        if google_display_date_to_iso(day.get("displayDate")) != target_date:
            continue

        temp_max = google_temperature_c(day.get("maxTemperature"))
        temp_min = google_temperature_c(day.get("minTemperature"))
        winds: List[float] = []
        for part_key in ("daytimeForecast", "nighttimeForecast"):
            w = google_daypart_wind_kmh(day.get(part_key))
            if w is not None:
                winds.append(w)

        metrics = {
            "temp_max": temp_max,
            "temp_min": temp_min,
            "wind_max": max(winds) if winds else None,
        }
        if has_any_metric(metrics):
            return metrics

    if SOURCE_GOOGLE_WEATHER not in RUNTIME_SOURCE_NOTES:
        RUNTIME_SOURCE_NOTES[SOURCE_GOOGLE_WEATHER] = "No target-day data in Google Weather response"
    return {"temp_max": None, "temp_min": None, "wind_max": None}


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
    if METOFFICE_API_KEY:
        sources.append(SOURCE_MET_OFFICE)
    if METOFFICE_ATMOS_API_KEY:
        sources.append(SOURCE_MET_OFFICE_ATMOSPHERIC)
    if OPENWEATHER_API_KEY:
        sources.append(SOURCE_OPENWEATHER)
    if GOOGLE_WEATHER_API_KEY or GOOGLE_WEATHER_ACCESS_TOKEN:
        sources.append(SOURCE_GOOGLE_WEATHER)
    return sources


def missing_source_keys() -> List[str]:
    missing: List[str] = []
    if not METOFFICE_API_KEY:
        missing.append(SOURCE_MET_OFFICE)
    if not METOFFICE_ATMOS_API_KEY:
        missing.append(SOURCE_MET_OFFICE_ATMOSPHERIC)
    if not OPENWEATHER_API_KEY:
        missing.append(SOURCE_OPENWEATHER)
    if not GOOGLE_WEATHER_API_KEY and not GOOGLE_WEATHER_ACCESS_TOKEN:
        missing.append(SOURCE_GOOGLE_WEATHER)
    return missing


def capture_forecasts(conn: sqlite3.Connection, run_date: str, target_date: str, sources: Sequence[str]) -> None:
    fetchers = {
        SOURCE_OPEN_METEO: fetch_open_meteo_forecast,
        SOURCE_MET_NO: fetch_met_no_forecast,
        SOURCE_MET_OFFICE: fetch_met_office_forecast,
        SOURCE_MET_OFFICE_ATMOSPHERIC: fetch_met_office_atmospheric_forecast,
        SOURCE_OPENWEATHER: fetch_openweather_forecast,
        SOURCE_GOOGLE_WEATHER: fetch_google_weather_forecast,
    }

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
            if has_any_metric(metrics):
                upsert_forecast(conn, run_date, target_date, source, loc, metrics)


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
        }
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


def zone_briefing_line(
    name: str,
    tmin: Optional[float],
    tmax: Optional[float],
    wind_kmh: Optional[float],
    spread_temp: Optional[float],
    spread_wind: Optional[float],
) -> str:
    if tmin is None and tmax is None and wind_kmh is None:
        return f"- {name}: forecast unavailable from current source set."

    temp_part = f"{fmt(tmin)} -> {fmt(tmax)} C"
    wind_part = f"{fmt(wind_kmh)} km/h ({fmt(kmh_to_mph(wind_kmh))} mph)"
    wind_desc = wind_band(wind_kmh)

    stability_notes: List[str] = []
    if spread_temp is not None and spread_temp >= 4:
        stability_notes.append("higher model spread on temperature")
    if spread_wind is not None and spread_wind >= 15:
        stability_notes.append("higher model spread on wind")
    stability_text = ""
    if stability_notes:
        stability_text = "; " + ", ".join(stability_notes)

    freezing_line = ""
    if tmax is not None and tmax <= 1:
        freezing_line = " Temperatures stay near/below freezing on higher ground."
    elif tmin is not None and tmin <= 0:
        freezing_line = " Early frost/ice risk on exposed sections."

    return (
        f"- {name} - {temp_part}. {wind_desc}, peaking near {wind_part}{stability_text}. "
        f"{best_window_from_conditions(tmin, tmax, wind_kmh)}.{freezing_line}"
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


def build_briefing(
    forecast_date: str,
    eval_date: str,
    configured_sources: Sequence[str],
    available_sources: Sequence[str],
    skipped_error_sources: Sequence[str],
    missing_sources: Sequence[str],
    forecasts: Dict[str, Dict[str, Dict[str, Optional[float]]]],
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
    zone_rows: Dict[str, Dict[str, Optional[float]]] = {}

    for loc in LOCATIONS:
        name = loc["name"]
        source_rows = forecasts.get(name, {})

        tmax_by_source = {s: source_rows.get(s, {}).get("temp_max") for s in available_sources}
        tmin_by_source = {s: source_rows.get(s, {}).get("temp_min") for s in available_sources}
        wind_by_source = {s: source_rows.get(s, {}).get("wind_max") for s in available_sources}

        tmax = weighted_metric(tmax_by_source, weights)
        tmin = weighted_metric(tmin_by_source, weights)
        wind = weighted_metric(wind_by_source, weights)

        spread_temp = spread(tmax_by_source)
        spread_wind = spread(wind_by_source)

        spread_note = ""
        if spread_temp is not None or spread_wind is not None:
            spread_note = f" (spread Tmax {fmt(spread_temp)}C, Wind {fmt(spread_wind)} km/h)"

        lines.append(zone_briefing_line(name, tmin, tmax, wind, spread_temp, spread_wind))
        zone_rows[name] = {
            "temp_max": tmax,
            "temp_min": tmin,
            "wind_max": wind,
            "spread_note": None if not spread_note else 1.0,
        }

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
            SOURCE_MET_OFFICE: "METOFFICE_API_KEY",
            SOURCE_MET_OFFICE_ATMOSPHERIC: "METOFFICE_ATMOS_API_KEY",
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


def main() -> None:
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

        capture_forecasts(conn, run_date=run_date, target_date=forecast_date, sources=active_sources)
        capture_actuals(conn, date_str=eval_date)
        available_sources = available_sources_for_target(conn, target_date=forecast_date, sources=active_sources)
        skipped_error_sources = [s for s in active_sources if s not in available_sources and s in RUNTIME_SOURCE_NOTES]

        eval_results = evaluate_and_store(conn, target_date=eval_date, sources=active_sources)
        rolling = rolling_confidence(conn, as_of_date=eval_date, sources=active_sources, lookback_days=LOOKBACK_DAYS)
        weight_sources = available_sources if available_sources else active_sources
        weights = compute_weights(rolling, weight_sources)
        store_weights(conn, date_str=run_date, weights=weights, rolling=rolling, lookback_days=LOOKBACK_DAYS)

        forecasts = latest_forecasts_by_location(conn, target_date=forecast_date, sources=available_sources)
        mwis_links = fetch_mwis_latest_pdf_links(limit=5)

        briefing = build_briefing(
            forecast_date=forecast_date,
            eval_date=eval_date,
            configured_sources=active_sources,
            available_sources=available_sources,
            skipped_error_sources=skipped_error_sources,
            missing_sources=missing_sources,
            forecasts=forecasts,
            rolling=rolling,
            weights=weights,
            eval_results=eval_results,
            mwis_links=mwis_links,
        )

        print(briefing)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        today = london_today()
        forecast_date = iso(today + dt.timedelta(days=1))
        print(f"Scottish mountains forecast (adaptive) - {forecast_date} (UK)")
        print("Daily report generated with degraded mode due internal error.")
        print(f"Error: {exc.__class__.__name__}: {exc}")
