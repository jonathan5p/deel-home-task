# Checks

Which checks to run, and how to keep them from interfering with other worktrees. The [pre-push-checks skill](../skills/pre-push-checks/SKILL.md) walks through them.

## Never bypass hooks

Never use `--no-verify`, on commit or push. If a hook fails, fix the cause. The pre-commit hooks run `uv lock --check`, ruff check, ruff format, `ty` and the unit tests.

## Per change type

| Change | Run |
| --- | --- |
| Docs, README, agent files, conflict fixes that only touch docs | The hooks only |
| Python code | `make lint`, `make typecheck`, `make test` (the hooks run them) |
| Jobs, Compose services, SQL/DDL, schemas, Spark or Iceberg behaviour | The above plus the `tests/infra` modules the change affects |
| Changes to what lands in the data | The above plus `make data-quality` on the shared stack, as evidence |

## Infra tests only when needed

Run infra tests only when the change can affect what they exercise: jobs, Compose services, SQL/DDL, Spark or Iceberg behaviour. Run only the modules the change affects, chosen by reading what each module exercises (the jobs and `tests/infra/apps/` it submits, the services it starts, the SQL it applies), not the full `make test-infra`; run the full suite only when the user asks for it. Name the modules you ran and why the others were skipped. For docs, README, comment edits, renames already covered by unit tests, and other small edits, run only lint, ty and unit tests, and say that the infra tests were skipped and why. After a rebase or conflict fix, check whether non-doc files changed in the resolution; if only docs changed, skip infra.

Infra tests run locally only, never in CI. CI covers lint, typecheck and unit tests. Do not write tests of the Makefile targets or the Compose file themselves; platform behaviour is tested in `tests/infra`.

## Data quality is not a test

Data quality checks (for example landing versus Kafka reconciliation) are production code in `analytics/pipelines/src/pipelines/quality/`. Run them with `make data-quality` against the running shared stack and report the output as evidence. Never add them to `make test-infra`. Their logic is unit-tested on small DataFrames.

## Docker network exhaustion

If Compose fails with "all predefined address pools have been fully subnetted", remove only your own idle networks:

```sh
docker network ls --filter name=deel-checks-<your worktree folder> --format '{{.Name}}'
docker network inspect <name> --format '{{len .Containers}}'   # remove only when 0
docker network rm <name>
```

Never run `docker network prune` or `docker system prune`, and never remove another worktree's networks or the shared `deel_home_task` stack's.
