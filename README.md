# Chief Fafa Bot

Chief Fafa is a Telegram/OpenClaw recipe agent that accepts URL or text input, extracts recipe data, and creates a Google Doc note.

## Features

- URL intake for recipe pages and video/social links
- Full-text recipe intake (multi-language)
- Saved recipe enquiry (memory + local notes + conversation history + Google Docs)
- Duplicate URL detection before creating a new document
- Google Docs generation with structured recipe format and image embedding (best-effort)
- Stable chat payload via `--json --json-brief` including `reply_message` for direct Telegram reply

## Repository Layout

```text
scripts/
  chief_fafa_recipe_pipeline.py   # Core extraction + formatting + Google Docs write
  chief_fafa_auto_review.py       # Auto-review helper (best-effort)
  google_keep_oauth_setup.py      # OAuth helper for Google credentials bootstrap
```

## Prerequisites

- Python 3.10+
- `requests`
- Optional: `yt-dlp` for richer video metadata/captions

```bash
python3 -m pip install --upgrade requests
```

## Environment Variables

Set in `.env` or `~/.openclaw/.env` (do not commit secrets).

Required for normal operation:

- `OPENAI_API_KEY`
- `GOOGLE_DOCS_CLIENT_ID`
- `GOOGLE_DOCS_CLIENT_SECRET`
- `GOOGLE_DOCS_REFRESH_TOKEN`

Common optional settings:

- `GOOGLE_DOCS_ACCESS_TOKEN` (fallback only; refresh token preferred)
- `GOOGLE_DOCS_REDIRECT_URI` (default `http://127.0.0.1:8788/callback`)
- `GOOGLE_DOCS_QUOTA_PROJECT` (recommended for Google billing headers)
- `CHIEF_FAFA_OUTPUT_DIR` (default `/home/felixlee/Desktop/chief-fafa/notes`)
- `CHIEF_FAFA_FAST_MODE` (`1` to reduce heavy enrichment for faster Telegram replies)
- `CHIEF_FAFA_OPENAI_TIMEOUT_SEC` (timeout tuning)
- `CHIEF_FAFA_MEMORY_ROOT` (memory root override)
- `CHIEF_FAFA_OPENCLAW_SESSIONS_DIR` (OpenClaw sessions path override)
- `CHIEF_FAFA_AUTO_REVIEW_ENABLED` (default `1`)
- `CHIEF_FAFA_AUTO_REVIEW_SCRIPT` (auto-review script path)
- `CHIEF_FAFA_SEEN_HOSTS_FILE` (tracked hosts file path)

## Pipeline Usage

URL mode:

```bash
python3 scripts/chief_fafa_recipe_pipeline.py "https://example.com/recipe" --json --json-brief
```

Text mode:

```bash
cat recipe.txt | python3 scripts/chief_fafa_recipe_pipeline.py --stdin --json --json-brief
```

Enquiry mode:

```bash
python3 scripts/chief_fafa_recipe_pipeline.py "find my black sesame recipe" --json --json-brief
```

## JSON Brief Contract

`--json --json-brief` returns:

- `summary`
- `google_doc_status`
- `google_doc_url`
- `error_message`
- `reply_message`

`reply_message` is canonical for Telegram:

- 3 lines when no error (`Error:` omitted)
- 4 lines only when `error_message` is non-empty

## OpenClaw Integration Notes

- Bind Telegram account `chieffafa` to agent `chief-fafa`.
- Keep command execution in `CHIEF_FAFA_FAST_MODE=1` for responsiveness.
- Agent instructions should send `brief.reply_message` verbatim.
- `process` tool is intentionally denied for `chief-fafa` to avoid interim placeholder replies.

## Troubleshooting

- Google Doc not created:
  - Verify Docs OAuth vars are set and refresh token is valid.
- URL processed but no structured ingredients:
  - Enable/install `yt-dlp`; retry with fast mode off for deeper extraction.
- Duplicate URL returns existing doc:
  - Expected behavior; pipeline reuses prior document URL.
- Telegram response includes unexpected formatting:
  - Ensure agent prompt/instructions still enforce `reply_message` passthrough.

## Security

- Rotate/revoke tokens exposed in chat/logs.
- Keep `.env`, `credentials/`, `.secrets/`, and generated data out of git.
- Use fine-grained GitHub PAT scopes only as needed.
