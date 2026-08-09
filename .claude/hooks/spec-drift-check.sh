#!/usr/bin/env bash
# Stop hook: warn when project files changed more recently than any spec file.
#
# Non-blocking by design — it surfaces possible drift, it does not gate the turn.
# Judging whether a change actually needs a spec update is the model's job.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPECS="$ROOT/specs"

[[ -d "$SPECS" ]] || exit 0

newest() {
  # newest mtime (epoch seconds) among matching files, or 0 if none
  find "$@" -type f -printf '%T@\n' 2>/dev/null \
    | sort -rn | head -1 | cut -d. -f1
}

spec_mtime="$(newest "$SPECS" -name '*.md')"
spec_mtime="${spec_mtime:-0}"

src_mtime="$(newest "$ROOT" \
  \( -path "$SPECS" -o -path "$ROOT/.git" -o -path "$ROOT/.claude" \) -prune -o \
  \( -name '*.py' -o -name '*.yml' -o -name '*.yaml' -o -name '*.sh' -o -name '*.toml' \) -print)"
src_mtime="${src_mtime:-0}"

# Grace period: edits within the same turn as a spec edit are not drift.
if (( src_mtime > spec_mtime + 60 )); then
  mapfile -t stale < <(find "$ROOT" \
    \( -path "$SPECS" -o -path "$ROOT/.git" -o -path "$ROOT/.claude" \) -prune -o \
    \( -name '*.py' -o -name '*.yml' -o -name '*.yaml' -o -name '*.sh' -o -name '*.toml' \) \
    -newermt "@$spec_mtime" -print 2>/dev/null | sed "s|^$ROOT/||" | head -10)

  ((${#stale[@]})) || exit 0

  printf '%s' "$(cat <<EOF
{"systemMessage": "Spec drift check: these files changed after the most recent spec edit — $(printf '%s, ' "${stale[@]}" | sed 's/, $//'). Per CLAUDE.md: tick completed tasks and amend design.md automatically; if the change contradicts or exceeds requirements.md, stop and ask instead."}
EOF
)"
fi

exit 0
