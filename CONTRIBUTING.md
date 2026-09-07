# Contributing

This is a learning repository built with spec-driven development. The method is not
overhead around the work — it *is* the work, and the specs are the artifact most
worth reading. So contributions follow the same gates the existing nine features did.

## The gate sequence

Every feature moves through four gates, each approved before the next:

```
requirements.md  →  approve  →  design.md  →  approve  →  tasks.md  →  approve  →  implement
```

**No code before `tasks.md` is approved** — not a scaffold, not a quick draft, not an
illustrative example. A pull request that arrives as code with the specs written
afterwards will be asked to back up and start at requirements. That is not
bureaucracy; the point of the repo is that the reasoning came first and is visible.

If implementation reveals the spec was wrong, stop and amend the spec rather than
patching the code and moving on.

## Adding a feature

1. **Claim the next number.** Directories are zero-padded and sequential:
   `specs/009-<feature-slug>/`. Nine features exist (`000`–`008`); `009` is open.
2. **Write `requirements.md`** — what and why, in user stories plus EARS acceptance
   criteria. No implementation detail. Declare `**Status:**` and `**Depends on:**`
   at the top, as the existing specs do.
3. **Write `design.md`** — architecture, the decisions taken, and the alternatives
   rejected with the reason. Design sections cite requirement IDs.
4. **Write `tasks.md`** — a numbered checklist, ordered so each task can actually be
   done when reached.
5. **Implement**, ticking tasks as they land.

### Requirement IDs and EARS

Every acceptance criterion gets a stable ID (`R<feature>.<n>`, e.g. `R9.3`) that
design sections and tasks cite by name.

| Pattern | Shape |
|---|---|
| Ubiquitous | THE SYSTEM SHALL `<behaviour>` |
| Event-driven | WHEN `<trigger>` THE SYSTEM SHALL `<behaviour>` |
| State-driven | WHILE `<state>` THE SYSTEM SHALL `<behaviour>` |
| Conditional | IF `<condition>` THEN THE SYSTEM SHALL `<behaviour>` |
| Optional | WHERE `<feature is included>` THE SYSTEM SHALL `<behaviour>` |

Criteria must be testable by observation. "Fast", "reliable" and "user-friendly" are
not criteria — give a number or a behaviour someone can watch happen.

### Task rules

- Each task is a change to code, config, or docs that a requirement asked for.
- Each task cites at least one requirement ID (`— *R9.1, R9.4*`).
- Tick a task the moment it is done — no separate approval needed.
- **No test tasks**, and **no verification-experiment tasks**. Nothing whose
  deliverable is "run it and observe" belongs in `tasks.md`. If you think something
  needs checking, say so in the pull request description.

## Before you commit

```bash
.claude/tools/spec-status.sh          # must exit 0
```

It checks traceability in both directions: every requirement has a task, and every
task cites a declared requirement. Anything implemented that no requirement asked
for is scope creep — flag it rather than quietly keeping it.

## Testing

**This project has no test suite, and one should not be added.** No `tests/`
directory, no pytest. Verification is manual: run the feature against the real
broker, kill things, and judge whether it behaves. Describe what you ran in the pull
request; do not record observed results in the specs.

## Code conventions

- Type hints on all function signatures; Google-style docstrings on public functions.
- `snake_case` for names, `UPPER_CASE` for constants.
- No hardcoded secrets or connection strings — environment variables only, declared
  in `config.py` and documented in `.env.example`.
- Feature code must not redefine broker config; the cluster lives in
  `docker-compose.yml` alone.
- The Kafka client is chosen per feature in `design.md`, not assumed.
- Keep functions small and single-purpose; extract shared logic rather than
  duplicating it.

## Git

- Conventional commits with the feature number as the scope: `feat(009): …`,
  `fix(007): …`, `docs(006): …`, `refactor: …`, `chore: …`.
- Commit to `master`. Branches are for pull requests from forks; the upstream history
  is linear on purpose, since the spec gates already structure it.

## Questions

Open an issue. A question that turns out to be a real gap usually becomes a new
requirement ID rather than a patch.
