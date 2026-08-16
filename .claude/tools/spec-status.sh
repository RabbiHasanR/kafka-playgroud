#!/usr/bin/env bash
# Traceability check across specs/: every requirement needs an implementing task,
# every task needs a requirement citation.
#
# Usage: .claude/tools/spec-status.sh [feature-slug ...]
# Exit:  0 = fully traced, 1 = gaps found

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPECS="$ROOT/specs"

[[ -d "$SPECS" ]] || { echo "no specs/ directory at $SPECS"; exit 1; }

if (($#)); then
  features=()
  for slug in "$@"; do features+=("$SPECS/$slug"); done
else
  mapfile -t features < <(find "$SPECS" -mindepth 1 -maxdepth 1 -type d | sort)
fi

((${#features[@]})) || { echo "no feature directories under specs/"; exit 1; }

# Requirement IDs look like R0.14 / R12.3 — matched wherever they appear.
ID_RE='R[0-9]+\.[0-9]+'
gaps=0

for dir in "${features[@]}"; do
  slug="$(basename "$dir")"
  req="$dir/requirements.md"
  tasks="$dir/tasks.md"
  design="$dir/design.md"

  echo "── $slug"

  missing_docs=()
  for f in "$req" "$design" "$tasks"; do
    [[ -f "$f" ]] || missing_docs+=("$(basename "$f")")
  done
  if ((${#missing_docs[@]})); then
    echo "   missing: ${missing_docs[*]}"
    gaps=1
    [[ -f "$req" && -f "$tasks" ]] || { echo; continue; }
  fi

  # Declared IDs: only those defined as bullets in requirements.md, e.g. "- **R0.1** —"
  mapfile -t declared < <(grep -oE "^[[:space:]]*[-*][[:space:]]+\*\*$ID_RE\*\*" "$req" \
    | grep -oE "$ID_RE" | sort -u -V)
  mapfile -t cited < <(grep -oE "$ID_RE" "$tasks" | sort -u -V)

  if ((${#declared[@]} == 0)); then
    echo "   no requirement IDs declared — expected '- **R<n>.<n>** — …' bullets"
    gaps=1
    echo
    continue
  fi

  # comm requires its inputs in the collating order of plain `sort`, not the version
  # order above: -V puts R2.2 before R2.10, plain sort does the reverse. Feeding it
  # -V-sorted lists makes it warn and, worse, silently miss differences past the first
  # disorder — so the set arithmetic sorts plainly and only the display re-sorts by -V.
  mapfile -t uncovered < <(comm -23 \
    <(printf '%s\n' "${declared[@]}" | sort) \
    <(printf '%s\n' "${cited[@]:-}" | sort) | sort -V)
  mapfile -t unknown < <(comm -13 \
    <(printf '%s\n' "${declared[@]}" | sort) \
    <(printf '%s\n' "${cited[@]:-}" | sort) | sort -V)

  # A task is "- [ ] **T<n>** …" plus its wrapped continuation lines, so the
  # citation is looked for across the whole block, not just the first line.
  mapfile -t untraced < <(
    awk -v re='R[0-9]+[.][0-9]+' '
      function flush() {
        if (start && body !~ re) {
          gsub(/^[[:space:]]*[-*][[:space:]]+\[[ xX]\][[:space:]]*/, "", first)
          printf "     tasks.md:%d  %.80s\n", start, first
        }
        start = 0; body = ""; first = ""
      }
      /^[[:space:]]*[-*][[:space:]]+\[[ xX]\]/ { flush(); start = NR; first = $0 }
      start { body = body " " $0 }
      /^[[:space:]]*$/ { flush() }
      END { flush() }
    ' "$tasks"
  )

  total=$(grep -cE '^[[:space:]]*[-*][[:space:]]+\[[ xX]\]' "$tasks")
  done_n=$(grep -cE '^[[:space:]]*[-*][[:space:]]+\[[xX]\]' "$tasks")

  echo "   requirements: ${#declared[@]} declared, $(( ${#declared[@]} - ${#uncovered[@]} )) covered"
  echo "   tasks:        $done_n/$total complete"

  if ((${#uncovered[@]})); then
    echo "   ✗ requirements with no task: ${uncovered[*]}"
    gaps=1
  fi
  if ((${#unknown[@]})); then
    echo "   ✗ tasks cite undeclared IDs: ${unknown[*]}"
    gaps=1
  fi
  if ((${#untraced[@]})); then
    echo "   ✗ tasks citing no requirement:"
    printf '%s\n' "${untraced[@]}"
    gaps=1
  fi
  ((gaps)) || echo "   ✓ fully traced"
  echo
done

((gaps)) && echo "traceability gaps found" || echo "all specs fully traced"
exit $gaps
