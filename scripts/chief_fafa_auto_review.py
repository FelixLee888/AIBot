#!/usr/bin/env python3
"""Chief Fafa auto-review worker.

Triggered by the recipe pipeline when:
1) a new URL host format is seen, or
2) a runtime error occurs for a URL flow.

This worker records incidents and applies a conservative self-improvement:
- For `new_url_host`, if the URL looks video-like, add host token to VIDEO_URL_HINTS.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse


def normalize_host(url: str) -> str:
    try:
        host = urlparse(str(url or "").strip()).netloc.lower()
    except Exception:
        return ""
    for pref in ("www.", "m.", "mbasic.", "mobile."):
        if host.startswith(pref):
            return host[len(pref) :]
    return host


def root_host(host: str) -> str:
    parts = [x for x in host.split(".") if x]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def url_looks_video_like(url: str) -> bool:
    low = str(url or "").strip().lower()
    return any(tag in low for tag in ["/reel/", "/video", "/videos/", "/shorts/", "/watch", "v=", "/clip/"])


def append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def add_video_host_hint(pipeline_path: Path, host_token: str) -> bool:
    if not host_token:
        return False
    content = read_text(pipeline_path)
    marker = "VIDEO_URL_HINTS = ["
    start = content.find(marker)
    if start < 0:
        return False
    end = content.find("]\n", start)
    if end < 0:
        return False
    block = content[start : end + 2]
    if f"\"{host_token}\"" in block:
        return False
    insert_at = end
    new_content = content[:insert_at] + f'    "{host_token}",\n' + content[insert_at:]
    write_text(pipeline_path, new_content)
    return True


def run_py_compile(py_file: Path) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["/usr/bin/python3", "-m", "py_compile", str(py_file)],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:
        return False, f"py_compile failed ({exc.__class__.__name__}: {exc})"
    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        return False, msg[:1200]
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Chief Fafa auto-review/self-improvement worker.")
    parser.add_argument("--reason", default="", help="Trigger reason: new_url_host | pipeline_error")
    parser.add_argument("--source-url", default="", help="URL being processed")
    parser.add_argument("--error", default="", help="Error message (if any)")
    parser.add_argument("--input-snippet", default="", help="Input snippet for incident diagnostics")
    parser.add_argument("--workspace", default="/home/felixlee/Desktop/chief-fafa", help="Chief Fafa workspace root")
    parser.add_argument("--dry-run", action="store_true", help="Record incident only, no file patch")
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    pipeline_path = workspace / "scripts" / "chief_fafa_recipe_pipeline.py"
    incident_path = workspace / ".pi" / "auto_review_incidents.jsonl"
    sync_repo = Path("/home/felixlee/Desktop/chief-fafa-sync")

    host = normalize_host(args.source_url)
    host_token = root_host(host)
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    action = "logged_only"
    changed = False
    compile_ok = True
    compile_error = ""
    sync_copied = False

    if not args.dry_run and args.reason == "new_url_host" and host_token and url_looks_video_like(args.source_url):
        if pipeline_path.exists():
            changed = add_video_host_hint(pipeline_path, host_token)
            action = "video_host_hint_added" if changed else "no_change"
            if changed:
                compile_ok, compile_error = run_py_compile(pipeline_path)
                if not compile_ok:
                    action = "compile_failed_after_patch"
                elif (sync_repo / ".git").exists():
                    try:
                        target = sync_repo / "scripts" / "chief_fafa_recipe_pipeline.py"
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(pipeline_path, target)
                        sync_copied = True
                    except Exception:
                        sync_copied = False

    payload = {
        "ts_utc": timestamp,
        "reason": args.reason,
        "source_url": args.source_url,
        "host": host,
        "host_token": host_token,
        "error": args.error[:1200],
        "input_snippet": args.input_snippet[:1200],
        "action": action,
        "changed": changed,
        "compile_ok": compile_ok,
        "compile_error": compile_error,
        "sync_copied": sync_copied,
        "dry_run": bool(args.dry_run),
    }
    append_jsonl(incident_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

