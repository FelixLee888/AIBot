#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/felixlee/Desktop/aibot"
cd "$ROOT"

TARGET_BRANCH="${1:-$(git branch --show-current)}"
REASON="${2:-self-improve}"

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

stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
msg="auto(self-improve): ${REASON} ${stamp}"

git commit -m "$msg"

git pull --rebase origin "$TARGET_BRANCH"

git push origin "$TARGET_BRANCH"

echo "pushed_commit=$(git rev-parse --short HEAD)"
