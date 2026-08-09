# kafka-playground — Project Rules

This project uses **spec-driven development**. These rules override default behaviour.

## The gate sequence

Every feature moves through four gates. Each needs explicit approval before the next.

```
requirements.md  →  approve  →  design.md  →  approve  →  tasks.md  →  approve  →  implement
```

**No code is written before `tasks.md` is approved.** Not a scaffold, not a "quick
draft", not an example. If implementation reveals the spec is wrong, stop and amend
the spec — do not patch the code and move on.

## Layout

```
specs/<NNN>-<feature-slug>/
├── requirements.md   # what & why — user stories + EARS criteria, no implementation
├── design.md         # how — architecture, decisions, rejected alternatives
└── tasks.md          # order — numbered checklist, each task cites requirement IDs
```

Feature directories are zero-padded and sequential (`000-foundations`, `001-…`).

## Requirements format

Acceptance criteria use **EARS**. Every criterion gets a stable ID (`R<feature>.<n>`)
that design sections and tasks cite.

| Pattern | Shape |
|---|---|
| Ubiquitous | THE SYSTEM SHALL `<behaviour>` |
| Event-driven | WHEN `<trigger>` THE SYSTEM SHALL `<behaviour>` |
| State-driven | WHILE `<state>` THE SYSTEM SHALL `<behaviour>` |
| Conditional | IF `<condition>` THEN THE SYSTEM SHALL `<behaviour>` |
| Optional | WHERE `<feature is included>` THE SYSTEM SHALL `<behaviour>` |

Criteria must be testable. "Fast", "reliable", "user-friendly" are not criteria —
give a number or a observable behaviour.

## Keeping specs current

| Situation | Action |
|---|---|
| A task is completed | Tick it in `tasks.md` immediately. Automatic, no approval needed. |
| Implementation diverges from `design.md` | Amend `design.md` to match reality, and say so in the response. Automatic. |
| Implementation would contradict or exceed `requirements.md` | **Stop.** Surface the conflict and ask. Never auto-amend requirements. |
| A new requirement surfaces mid-build | Propose it as a new ID and wait for approval before building it. |

`design.md` describes reality and may track it. `requirements.md` is an approved
contract and changes only with the user's say-so.

## Traceability

- Every requirement has at least one task implementing it.
- Every task cites at least one requirement ID.
- Run `.claude/tools/spec-status.sh` to check both directions.

Anything built that no requirement asked for is scope creep — flag it rather than
quietly keeping it.

## Stack conventions

- Kafka client: decided per feature in `design.md`, not assumed.
- Type hints on all function signatures; Google-style docstrings on public functions.
- snake_case; UPPER_CASE constants.
- No hardcoded secrets or connection strings — environment variables only.
- Conventional commits (`feat/fix/refactor/test/chore/docs/ci`).

## Infrastructure

The broker is defined in `docker-compose.yml` (single-node KRaft, Kafka 4.3.1).
CLI reference lives in `README.md`. Feature code must not redefine broker config.
