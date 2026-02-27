# AIBot

Automation scripts for two assistant workflows:

- **Yuen Yuen Weather Agent**: Scotland mountain forecast + source benchmarking + Telegram delivery.
- **Chief Fafa Bot**: recipe/video URL ingestion + content pack generation + Google Docs note creation.

## Repository Layout

```text
scripts/
  weather_mountains_briefing.py     # Forecast + benchmark engine
  send_weather_telegram.py          # Chunked Telegram sender for weather output
  chief_fafa_recipe_pipeline.py     # Recipe/video extraction + content formats + Google Doc
  google_keep_oauth_setup.py        # OAuth helper for Google Docs/Keep tokens
```

## Prerequisites

- Python 3.10+
- `requests` Python package
- Optional:
  - `yt-dlp` for richer video metadata/transcript extraction in Chief Fafa
  - `eccodes` Python package for Met Office atmospheric GRIB extraction

Install minimum dependency:

```bash
python3 -m pip install --upgrade requests
```

## Environment Variables

Create a local `.env` (or use `~/.openclaw/.env` for OpenClaw runtime).  
Do **not** commit secrets.

### Weather Agent

- `TELEGRAM_BOT_TOKEN` (required for Telegram send)
- `WEATHER_TELEGRAM_CHAT_ID` (optional; default currently `6683969437`)
- `WEATHER_BENCHMARK_DATA_DIR` (optional; default auto-resolved)
- `METOFFICE_API_KEY` (optional)
- `METOFFICE_ATMOS_API_KEY` (optional)
- `METOFFICE_ATMOS_ORDER_ID` (optional)
- `OPENWEATHER_API_KEY` (optional)
- `GOOGLE_WEATHER_ACCESS_TOKEN` (recommended for Google Weather)
- `GOOGLE_WEATHER_API_KEY` (fallback for Google Weather)
- `GOOGLE_WEATHER_QUOTA_PROJECT` / `GOOGLE_CLOUD_PROJECT` (for Google billing header)

### Chief Fafa (Google Docs)

- `OPENAI_API_KEY`
- `GOOGLE_DOCS_CLIENT_ID`
- `GOOGLE_DOCS_CLIENT_SECRET`
- `GOOGLE_DOCS_REFRESH_TOKEN`
- `GOOGLE_DOCS_ACCESS_TOKEN` (optional; refresh token flow preferred)
- `GOOGLE_DOCS_REDIRECT_URI` (optional; default `http://127.0.0.1:8788/callback`)
- `CHIEF_FAFA_OUTPUT_DIR` (optional; default `/home/felixlee/Desktop/chief-fafa/notes`)
- `CHIEF_FAFA_OPENAI_TIMEOUT_SEC` (optional; timeout tuning)
- `CHIEF_FAFA_MEMORY_ROOT` (optional; default `/home/felixlee/Desktop/chief-fafa`)
- `CHIEF_FAFA_OPENCLAW_SESSIONS_DIR` (optional; default `/home/felixlee/.openclaw/agents/chief-fafa/sessions`)
- `CHIEF_FAFA_AUTO_REVIEW_ENABLED` (optional; default `1`)
- `CHIEF_FAFA_AUTO_REVIEW_SCRIPT` (optional; default `/home/felixlee/Desktop/chief-fafa/scripts/chief_fafa_auto_review.py`)
- `CHIEF_FAFA_SEEN_HOSTS_FILE` (optional; default `/home/felixlee/Desktop/chief-fafa/.pi/seen_url_hosts.json`)

## Yuen Yuen Weather Agent

### 1) Generate briefing locally

```bash
python3 scripts/weather_mountains_briefing.py
```

Output format includes:

1. Latest forecast by zone (with briefing)
2. Latest benchmark
3. Suitability for Cycling/Hiking/Skiing
4. Forecasting source with confidence %
5. Latest Full PDF links

### 2) Send briefing to Telegram (auto-splitting long messages)

```bash
python3 scripts/send_weather_telegram.py
```

This sender:

- Splits by section (`1)`, `2)`, ...)
- Further chunks overlong sections
- Sends plain text (avoids Telegram markdown parse errors)

### 3) OpenClaw cron example

```bash
openclaw cron add \
  --name "Scottish Mountains 08:00" \
  --cron "0 8 * * *" \
  --tz "Europe/London" \
  --session isolated \
  --message "/bash /usr/bin/python3 /home/felixlee/Desktop/aibot/scripts/send_weather_telegram.py" \
  --no-deliver \
  --best-effort-deliver
```

## Chief Fafa Recipe Pipeline

### URL mode

```bash
python3 scripts/chief_fafa_recipe_pipeline.py "https://example.com/recipe" --json --json-brief
```

### Full-text recipe mode (multiline / Telegram text)

```bash
cat recipe.txt | python3 scripts/chief_fafa_recipe_pipeline.py --stdin --json --json-brief
```

### Saved-recipe enquiry mode

```bash
python3 scripts/chief_fafa_recipe_pipeline.py "find my saved black sesame recipe" --json --json-brief
```

Lookup order:

1. `memory/` + `MEMORY.md` (long-term notes)
2. Local recipe run reports in `CHIEF_FAFA_OUTPUT_DIR`
3. OpenClaw conversation history sessions
4. Google Docs search (Drive API)

### Behavior summary

- Extracts title, ingredients, methods, media URL from source URL or free text
- Supports multilingual content handling
- Generates multi-format content pack (web/Facebook/Instagram/YouTube style)
- Creates Google Doc note (when Docs credentials are configured)
- Prevents duplicate recipe docs by checking prior URL runs and existing Google Docs matches before creating a new doc
- Supports enquiry of previously saved recipes and returns the best matching Google Doc URL when found
- Automatically triggers source-code auto-review on new URL host formats or URL-flow runtime errors (best-effort)

## Google OAuth Helper (Docs/Keep)

1) Generate consent URL:

```bash
python3 scripts/google_keep_oauth_setup.py --client-secret-file /path/client_secret.json
```

2) Exchange auth code:

```bash
python3 scripts/google_keep_oauth_setup.py \
  --client-secret-file /path/client_secret.json \
  --code "<AUTH_CODE>"
```

It prints env lines to place in your `.env`.

## Security Notes

- Rotate/revoke any token that was shared in chat or logs.
- Keep `.env`, `credentials/`, `.secrets/`, and `data/` out of git history.
- Use fine-grained GitHub PATs with minimal scope (`Contents: Read and write` only when needed).
