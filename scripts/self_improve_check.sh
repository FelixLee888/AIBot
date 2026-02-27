#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/felixlee/Desktop/aibot"
OUT_DIR="$ROOT/data/self_improve"
TS="$(date +%Y%m%d-%H%M%S)"
LOG="$OUT_DIR/check-$TS.log"
JSON="$OUT_DIR/check-latest.json"

mkdir -p "$OUT_DIR"

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
from pathlib import Path
status = "$status"
ts = "$TS"
log = "$LOG"
csv = "$fail_csv"
fails = [x for x in csv.split(',') if x]
p = Path("$JSON")
payload = {
  "status": status,
  "timestamp": ts,
  "failures": fails,
  "log_path": log,
}
p.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
print(json.dumps(payload, ensure_ascii=True))
PY2

if [ "$status" = "failed" ]; then
  echo "[self-improve-check] failed" | tee -a "$LOG"
  exit 1
fi

echo "[self-improve-check] ok" | tee -a "$LOG"
exit 0
