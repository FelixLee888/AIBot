#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import re
import string
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_APPROVER_IDS = ["6683969437", "8713835432"]


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def read_json(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        data = json.loads(path.read_text(encoding="utf-8"))
        return data
    except Exception:
        return fallback


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_ids(value: str) -> List[str]:
    raw = [x.strip() for x in re.split(r"[,\s]+", str(value or "")) if x.strip()]
    out: List[str] = []
    seen = set()
    for item in raw:
        if not re.fullmatch(r"\d{6,16}", item):
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out or list(DEFAULT_APPROVER_IDS)


def generate_code(n: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def load_openclaw_config(path: Path) -> Dict[str, Any]:
    data = read_json(path, {})
    return data if isinstance(data, dict) else {}


def resolve_bot_token(openclaw_cfg: Dict[str, Any], account: str = "default") -> str:
    channels = openclaw_cfg.get("channels", {})
    tg = channels.get("telegram", {}) if isinstance(channels, dict) else {}
    if account and account != "default":
        accounts = tg.get("accounts", {}) if isinstance(tg, dict) else {}
        acc = accounts.get(account, {}) if isinstance(accounts, dict) else {}
        token = str(acc.get("botToken", "")).strip()
        if token:
            return token
    return str(tg.get("botToken", "")).strip()


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    keyboard_buttons: Optional[List[str]] = None,
) -> str:
    if not bot_token or not chat_id:
        return ""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if keyboard_buttons:
        payload["reply_markup"] = {
            "keyboard": [[{"text": btn} for btn in keyboard_buttons[:2]]],
            "resize_keyboard": True,
            "one_time_keyboard": True,
            "selective": True,
        }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code >= 400:
            return f"HTTP {resp.status_code}: {(resp.text or '')[:300]}"
    except Exception as exc:
        return f"{exc.__class__.__name__}: {exc}"
    return ""


def load_store(path: Path) -> Dict[str, Any]:
    data = read_json(path, {})
    if not isinstance(data, dict):
        data = {}
    reqs = data.get("requests", [])
    if not isinstance(reqs, list):
        reqs = []
    data["version"] = 1
    data["requests"] = reqs
    return data


def find_by_code(store: Dict[str, Any], code: str) -> Optional[Dict[str, Any]]:
    for req in store.get("requests", []):
        if str(req.get("code", "")).upper() == code.upper():
            return req
    return None


def find_by_fingerprint(store: Dict[str, Any], fingerprint: str) -> Optional[Dict[str, Any]]:
    for req in reversed(store.get("requests", [])):
        if str(req.get("fingerprint", "")) == fingerprint and str(req.get("status", "")) in {
            "pending",
            "approved",
        }:
            return req
    return None


def build_message(workspace: str, code: str, reason: str, changes: List[str]) -> str:
    ch = "\n".join(f"- {c}" for c in changes[:12]) if changes else "- (no file list)"
    return (
        "AIBot self-improve approval required\n"
        f"Workspace: {workspace}\n"
        f"Code: {code}\n"
        f"Reason: {reason}\n"
        "Proposed changes:\n"
        f"{ch}\n"
        "Approve by replying to bot with: approve self-improve " + code + "\n"
        "Reject by replying to bot with: reject self-improve " + code
    )


def notify(bot_token: str, approver_ids: List[str], text: str, keyboard_buttons: Optional[List[str]] = None) -> List[str]:
    errs: List[str] = []
    for uid in approver_ids:
        err = send_telegram_message(bot_token, uid, text, keyboard_buttons=keyboard_buttons)
        if err:
            errs.append(f"{uid}: {err}")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser(description="AIBot self-improve approval gate")
    ap.add_argument("action", choices=["create", "approve", "reject", "status", "consume"])
    ap.add_argument("--workspace", default="/home/felixlee/Desktop/aibot")
    ap.add_argument("--reason", default="self-improve")
    ap.add_argument("--fingerprint", default="")
    ap.add_argument("--changes", default="")
    ap.add_argument("--code", default="")
    ap.add_argument("--approver-id", default="")
    ap.add_argument("--approver-ids", default=",".join(DEFAULT_APPROVER_IDS))
    ap.add_argument("--openclaw-config", default="/home/felixlee/.openclaw/openclaw.json")
    ap.add_argument("--bot-account", default="default")
    args = ap.parse_args()

    workspace = Path(args.workspace).resolve()
    state_path = workspace / ".pi" / "self_improve" / "approvals.json"
    store = load_store(state_path)
    approver_ids = normalize_ids(args.approver_ids)

    if args.action == "create":
        fp = str(args.fingerprint or "").strip()
        if not fp:
            print(json.dumps({"ok": False, "status": "failed", "message": "missing fingerprint"}, ensure_ascii=True))
            return 2
        req = find_by_fingerprint(store, fp)
        changes = [x for x in [c.strip() for c in str(args.changes or "").split(",")] if x]
        created = False
        if not req:
            code = generate_code(8)
            while find_by_code(store, code):
                code = generate_code(8)
            req = {
                "code": code,
                "workspace": str(workspace),
                "reason": str(args.reason or "self-improve"),
                "fingerprint": fp,
                "changes": changes,
                "status": "pending",
                "created_at_utc": now_utc_iso(),
                "updated_at_utc": now_utc_iso(),
                "approvals": [],
                "notifications": [],
            }
            store["requests"].append(req)
            created = True

        if created or str(req.get("status", "")) == "pending":
            cfg = load_openclaw_config(Path(args.openclaw_config))
            token = resolve_bot_token(cfg, account=args.bot_account)
            message = build_message(workspace.name, str(req.get("code", "")), str(req.get("reason", "self-improve")), req.get("changes", []))
            buttons = [f"approve self-improve {req.get('code', '')}", f"reject self-improve {req.get('code', '')}"]
            errs = notify(token, approver_ids, message, keyboard_buttons=buttons) if token else ["bot token missing"]
            req.setdefault("notifications", []).append(
                {
                    "at_utc": now_utc_iso(),
                    "to": approver_ids,
                    "errors": errs,
                }
            )
            req["updated_at_utc"] = now_utc_iso()

        write_json(state_path, store)
        print(
            json.dumps(
                {
                    "ok": True,
                    "status": str(req.get("status", "pending")),
                    "code": str(req.get("code", "")),
                    "reason": str(req.get("reason", "")),
                    "changes": req.get("changes", []),
                    "created": created,
                },
                ensure_ascii=True,
            )
        )
        return 0

    code = str(args.code or "").strip().upper()
    if not code:
        print(json.dumps({"ok": False, "status": "failed", "message": "missing code"}, ensure_ascii=True))
        return 2
    req = find_by_code(store, code)
    if not req:
        print(json.dumps({"ok": False, "status": "not_found", "code": code}, ensure_ascii=True))
        return 3

    if args.action in {"approve", "reject"}:
        approver_id = str(args.approver_id or "").strip()
        if approver_id and approver_id not in approver_ids:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "status": "forbidden",
                        "code": code,
                        "message": f"approver_id {approver_id} not allowed",
                    },
                    ensure_ascii=True,
                )
            )
            return 4
        new_status = "approved" if args.action == "approve" else "rejected"
        req["status"] = new_status
        req["updated_at_utc"] = now_utc_iso()
        req.setdefault("approvals", []).append(
            {
                "action": new_status,
                "approver_id": approver_id or "unknown",
                "at_utc": now_utc_iso(),
            }
        )
        write_json(state_path, store)

        cfg = load_openclaw_config(Path(args.openclaw_config))
        token = resolve_bot_token(cfg, account=args.bot_account)
        if token:
            notify(
                token,
                approver_ids,
                f"AIBot self-improve request {code} marked {new_status} by {approver_id or 'unknown'}.",
            )

        print(
            json.dumps(
                {
                    "ok": True,
                    "status": new_status,
                    "code": code,
                    "approver_id": approver_id or "unknown",
                },
                ensure_ascii=True,
            )
        )
        return 0

    if args.action == "consume":
        req["status"] = "consumed"
        req["updated_at_utc"] = now_utc_iso()
        write_json(state_path, store)
        print(json.dumps({"ok": True, "status": "consumed", "code": code}, ensure_ascii=True))
        return 0

    print(
        json.dumps(
            {
                "ok": True,
                "status": str(req.get("status", "unknown")),
                "code": code,
                "reason": str(req.get("reason", "")),
                "changes": req.get("changes", []),
                "updated_at_utc": str(req.get("updated_at_utc", "")),
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
