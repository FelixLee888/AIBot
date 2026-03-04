#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/felixlee/Desktop/aibot"
OUT_DIR="$ROOT/data/self_improve"
MEMORY_DIR="$ROOT/memory"
HEARTBEAT_JSON="$MEMORY_DIR/heartbeat-state.json"
TS="$(date +%Y%m%d-%H%M%S)"
LOG="$OUT_DIR/check-$TS.log"
JSON="$OUT_DIR/check-latest.json"

mkdir -p "$OUT_DIR" "$MEMORY_DIR"

echo "[self-improve-check] start $(date -Is)" | tee "$LOG"

ok=true
failures=()

if ! command -v /usr/bin/python3 >/dev/null 2>&1; then
  ok=false
  failures+=("python3_missing")
fi

if [ "$ok" = true ]; then
  echo "[check] py_compile scripts/*.py" | tee -a "$LOG"
  if ! /usr/bin/python3 -m py_compile "$ROOT"/scripts/*.py >>"$LOG" 2>&1; then
    ok=false
    failures+=("py_compile_failed")
  fi
fi

if [ "$ok" = true ]; then
  echo "[check] weather script smoke run" | tee -a "$LOG"
  if ! WEATHER_BENCHMARK_DATA_DIR="$ROOT/data" /usr/bin/python3 "$ROOT/scripts/weather_mountains_briefing.py" >/tmp/weather_smoke.out 2>>"$LOG"; then
    ok=false
    failures+=("weather_smoke_failed")
  fi
fi

status="ok"
if [ "$ok" != true ]; then
  status="failed"
fi

fail_csv=""
if [ ${#failures[@]} -gt 0 ]; then
  fail_csv="$(IFS=,; echo "${failures[*]}")"
fi

/usr/bin/python3 - <<PY2
import json
import datetime as dt
from pathlib import Path
status = "$status"
ts = "$TS"
log = "$LOG"
csv = "$fail_csv"
heartbeat_path = Path("$HEARTBEAT_JSON")
fails = [x for x in csv.split(',') if x]
p = Path("$JSON")
payload = {
  "status": status,
  "timestamp": ts,
  "failures": fails,
  "log_path": log,
}
p.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

# Maintain heartbeat state for automated checks.
heartbeat = {}
if heartbeat_path.exists():
    try:
        heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        if not isinstance(heartbeat, dict):
            heartbeat = {}
    except Exception:
        heartbeat = {}
checks = heartbeat.get("checks", {})
if not isinstance(checks, dict):
    checks = {}
now_iso = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
entry = checks.get("self_improve_check", {})
if not isinstance(entry, dict):
    entry = {}
entry["status"] = status
entry["last_run_utc"] = now_iso
entry["last_log_path"] = log
entry["last_failures"] = fails
if status == "ok":
    entry["last_success_utc"] = now_iso
checks["self_improve_check"] = entry
heartbeat["version"] = 1
heartbeat["updated_at_utc"] = now_iso
heartbeat["checks"] = checks
heartbeat_path.write_text(json.dumps(heartbeat, ensure_ascii=True, indent=2), encoding="utf-8")

print(json.dumps(payload, ensure_ascii=True))
PY2

if [ "$status" = "failed" ]; then
  echo "[self-improve-check] failed" | tee -a "$LOG"
  exit 1
fi

echo "[self-improve-check] ok" | tee -a "$LOG"
exit 0
