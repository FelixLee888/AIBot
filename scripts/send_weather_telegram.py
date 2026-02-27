#!/usr/bin/env python3
"""Send mountain weather summary to Telegram in safe chunks.

This script runs `weather_mountains_briefing.py`, splits output by numbered
sections, then sends each section as plain text to Telegram. It avoids
Markdown parse failures and Telegram message size limits.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import requests

MAX_MSG_CHARS = 3500
REQUEST_TIMEOUT = 20

SCRIPT_DIR = Path(__file__).resolve().parent
WEATHER_SCRIPT = SCRIPT_DIR / "weather_mountains_briefing.py"
OPENCLAW_ENV_PATHS = [
    Path("/home/felixlee/.openclaw/.env"),
    Path.home() / ".openclaw/.env",
    SCRIPT_DIR.parent / ".env",
]
OPENCLAW_CONFIG_PATHS = [
    Path("/home/felixlee/.openclaw/openclaw.json"),
    Path.home() / ".openclaw/openclaw.json",
]


def read_env_file_value(path: Path, key: str) -> str:
    try:
        if not path.exists():
            return ""
    except OSError:
        return ""

    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() != key:
                continue
            return v.strip().strip("'\"")
    except Exception:
        return ""
    return ""


def resolve_telegram_token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if token:
        return token

    for env_path in OPENCLAW_ENV_PATHS:
        token = read_env_file_value(env_path, "TELEGRAM_BOT_TOKEN")
        if token:
            return token

    for cfg_path in OPENCLAW_CONFIG_PATHS:
        try:
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            token = (
                data.get("channels", {})
                .get("telegram", {})
                .get("botToken", "")
                .strip()
            )
            if token:
                return token
        except Exception:
            continue
    return ""


def resolve_chat_id() -> str:
    chat_id = os.getenv("WEATHER_TELEGRAM_CHAT_ID", "").strip()
    if chat_id:
        return chat_id
    # Default weather-recipient chat id used by existing cron job.
    return "6683969437"


def run_weather_script() -> str:
    proc = subprocess.run(
        ["/usr/bin/python3", str(WEATHER_SCRIPT)],
        cwd=str(SCRIPT_DIR.parent),
        check=False,
        capture_output=True,
        text=True,
    )
    output = (proc.stdout or "").strip()
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        raise RuntimeError(f"weather script failed ({proc.returncode}): {err}")
    if not output:
        raise RuntimeError("weather script returned empty output")
    return output


def split_sections(report: str) -> List[str]:
    lines = [ln.rstrip() for ln in report.strip().splitlines()]
    if not lines:
        return []

    header: List[str] = []
    sections: List[List[str]] = []
    current: Optional[List[str]] = None

    for line in lines:
        if re.match(r"^\d\)\s+", line):
            if current:
                sections.append(current)
            current = [line]
            continue
        if current is None:
            header.append(line)
        else:
            current.append(line)

    if current:
        sections.append(current)

    out: List[str] = []
    if header:
        out.append("\n".join(header).strip())
    out.extend("\n".join(sec).strip() for sec in sections if sec)
    return [part for part in out if part]


def split_long_text(text: str, max_chars: int = MAX_MSG_CHARS) -> List[str]:
    clean = text.strip()
    if len(clean) <= max_chars:
        return [clean]

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for raw_line in clean.splitlines():
        line = raw_line.rstrip()
        add_len = len(line) + (1 if current else 0)
        if current and current_len + add_len > max_chars:
            chunks.append("\n".join(current).strip())
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += add_len

    if current:
        chunks.append("\n".join(current).strip())

    # Hard fallback for a single overlong line.
    safe_chunks: List[str] = []
    for chunk in chunks:
        if len(chunk) <= max_chars:
            safe_chunks.append(chunk)
            continue
        start = 0
        while start < len(chunk):
            safe_chunks.append(chunk[start:start + max_chars])
            start += max_chars
    return safe_chunks


def build_message_chunks(report: str) -> List[str]:
    parts = split_sections(report)
    chunks: List[str] = []
    for idx, part in enumerate(parts, 1):
        section_chunks = split_long_text(part, MAX_MSG_CHARS)
        if len(section_chunks) == 1:
            chunks.append(section_chunks[0])
            continue
        total = len(section_chunks)
        for n, piece in enumerate(section_chunks, 1):
            chunks.append(f"[Part {idx}.{n}/{total}]\n{piece}")
    return chunks


def send_telegram_message(token: str, chat_id: str, text: str) -> Tuple[bool, str]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        data = resp.json() if resp.content else {}
    except Exception as exc:
        return False, str(exc)

    if resp.status_code != 200 or not data.get("ok"):
        return False, str(data.get("description") or f"HTTP {resp.status_code}")
    return True, ""


def main() -> int:
    token = resolve_telegram_token()
    chat_id = resolve_chat_id()
    if not token:
        print("error: TELEGRAM_BOT_TOKEN not configured")
        return 2
    if not chat_id:
        print("error: WEATHER_TELEGRAM_CHAT_ID not configured")
        return 2

    try:
        report = run_weather_script()
    except Exception as exc:
        print(f"error: {exc}")
        return 3

    chunks = build_message_chunks(report)
    if not chunks:
        print("error: generated report was empty after chunking")
        return 4

    failures: List[str] = []
    sent = 0
    for idx, chunk in enumerate(chunks, 1):
        ok, err = send_telegram_message(token, chat_id, chunk)
        if ok:
            sent += 1
        else:
            failures.append(f"chunk {idx}: {err}")
        time.sleep(0.35)

    if failures:
        print(f"error: sent {sent}/{len(chunks)} chunks; failures: {' | '.join(failures)}")
        return 5

    print(f"ok: sent {sent} weather chunks to chat {chat_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
