---
name: pre-push-checks
description: >
  Pick and run the right checks for a change in this repository, and recover from Docker network exhaustion.
  Trigger: before a commit, a push or reporting work done; after a rebase or conflict fix;
  Compose fails with "all predefined address pools have been fully subnetted".
---

Rules: [checks](../../rules/checks.md).

## 1. Classify the change

```sh
/usr/bin/git -C <worktree> diff --name-only origin/main...HEAD
```

For a stacked branch, diff against its parent branch instead of `origin/main`. After a rebase or conflict fix, look only at the files the resolution changed.

## 2. Run what the change needs

| Changed files | Run |
| --- | --- |
| Docs, README, `.agents/`, docs-only conflict fixes | The hooks only |
| Python under `analytics/` | `make lint`, `make typecheck`, `make test` |
| Jobs, `docker-compose.yaml`, Dockerfiles, SQL/DDL, `.avsc` schemas, Spark or Iceberg config | The above plus only the affected `tests/infra` modules (`uv run --frozen pytest tests/infra/<file> ... --no-cov`); the full `make test-infra` only when the user asks |
| Anything that changes what lands in the data | The above plus `make data-quality` on the shared stack, as evidence. Never inside `make test-infra` |

- Commit normally so the hooks run. Never `--no-verify`.
- `make data-quality` and other shared-stack targets touch `deel_home_task`; as orchestrator, run them one at a time.

## 3. Docker network exhaustion

1. List your own networks: `docker network ls --filter name=deel-checks-<worktree folder> --format '{{.Name}}'` (and `deel-infra-tests-<worktree folder>` for infra runs).
2. For each, `docker network inspect <name> --format '{{len .Containers}}'`; remove with `docker network rm <name>` only when it prints `0`.
3. Never prune, and never touch other folders' networks or the shared stack's.

## 4. Report

List the checks run with their result and the checks skipped with why (for example "infra skipped: only README changed"). Then add the hand-off test instructions ([collaboration](../../rules/collaboration.md#handing-off-finished-work)).
