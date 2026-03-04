#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/felixlee/Desktop/aibot"
cd "$ROOT"

TARGET_BRANCH="${1:-main}"
REASON="${2:-self-improve}"
APPROVAL_SCRIPT="$ROOT/scripts/self_improve_approval_gate.py"
APPROVER_IDS="${SELF_IMPROVE_APPROVER_IDS:-6683969437,8713835432}"
BOT_ACCOUNT="${SELF_IMPROVE_BOT_ACCOUNT:-default}"

current_branch="$(git branch --show-current)"
if [ -z "$current_branch" ]; then
  echo "error: unable to detect current git branch"
  exit 2
fi

if [ "$current_branch" != "$TARGET_BRANCH" ]; then
  git checkout "$TARGET_BRANCH"
fi

candidates=(
  AGENTS.md TOOLS.md HEARTBEAT.md BOOTSTRAP.md IDENTITY.md SOUL.md USER.md README.md
  scripts apps
)

to_add=()
for p in "${candidates[@]}"; do
  if [ -e "$p" ]; then
    to_add+=("$p")
  fi
done

if [ ${#to_add[@]} -eq 0 ]; then
  echo "no_paths"
  exit 0
fi

# Stage only source/docs policy surfaces (never secrets/runtime state).
git add -A -- "${to_add[@]}"

if git diff --cached --quiet; then
  echo "no_changes"
  exit 0
fi

if [ ! -x "$APPROVAL_SCRIPT" ]; then
  echo "error: missing approval gate script at $APPROVAL_SCRIPT"
  exit 2
fi

changes_csv="$(git diff --cached --name-only | paste -sd, -)"
if command -v sha256sum >/dev/null 2>&1; then
  diff_hash="$(git diff --cached --binary | sha256sum | awk '{print $1}')"
else
  diff_hash="$(git diff --cached --binary | shasum -a 256 | awk '{print $1}')"
fi

approval_json="$(/usr/bin/python3 "$APPROVAL_SCRIPT" create \
  --workspace "$ROOT" \
  --reason "$REASON" \
  --fingerprint "$diff_hash" \
  --changes "$changes_csv" \
  --approver-ids "$APPROVER_IDS" \
  --bot-account "$BOT_ACCOUNT" 2>/tmp/aibot_self_improve_approval.err || true)"

approval_status="$(printf '%s' "$approval_json" | /usr/bin/python3 -c 'import json,sys
raw=sys.stdin.read().strip()
try:
 d=json.loads(raw)
 print(str(d.get("status","")))
except Exception:
 print("")')"
approval_code="$(printf '%s' "$approval_json" | /usr/bin/python3 -c 'import json,sys
raw=sys.stdin.read().strip()
try:
 d=json.loads(raw)
 print(str(d.get("code","")))
except Exception:
 print("")')"

if [ -z "$approval_status" ] || [ -z "$approval_code" ]; then
  echo "error: approval gate returned invalid payload"
  if [ -s /tmp/aibot_self_improve_approval.err ]; then
    echo "approval_stderr=$(tr '\n' ' ' </tmp/aibot_self_improve_approval.err | cut -c1-400)"
  fi
  echo "approval_raw=$(printf '%s' "$approval_json" | tr '\n' ' ' | cut -c1-500)"
  exit 2
fi

if [ "$approval_status" != "approved" ]; then
  echo "approval_required code=$approval_code status=$approval_status"
  echo "approve_hint=send 'approve self-improve $approval_code' to Telegram bot"
  exit 3
fi

stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
msg="auto(self-improve): ${REASON} ${stamp}"

git commit -m "$msg"

git pull --rebase origin "$TARGET_BRANCH"

git push origin "$TARGET_BRANCH"

/usr/bin/python3 "$APPROVAL_SCRIPT" consume --workspace "$ROOT" --code "$approval_code" >/dev/null 2>&1 || true

echo "pushed_commit=$(git rev-parse --short HEAD)"
echo "approval_code=$approval_code"
