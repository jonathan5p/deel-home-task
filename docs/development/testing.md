# Testing and CI

Five kinds of checks cover the project, from the code to the data on the running stack.

| Layer | What it covers | Where it runs | Command | Typical time |
| --- | --- | --- | --- | --- |
| [Unit tests](#unit-tests-lint-and-types) | The code of the pipelines, the API and `common`, on small synthetic data; the Grafana dashboard JSON | Containers with the cluster's Spark and Python; CI on every pull request | `make test` | A few minutes, with 4 workers |
| [Lint and types](#unit-tests-lint-and-types) | Formatting, lint rules, annotations and types | Host (ruff) and containers (ty); CI | `make lint`, `make typecheck` | Under a minute |
| [Docs build](#docs-build) | Every page, link and anchor of this site, and the table reference rendered from the schemas | Host; CI when the docs change | `make docs` | Seconds |
| [Infra tests](#infra-tests) | The platform: Compose services, Spark and Iceberg behaviour, the jobs end to end on fixture data, the serving logins | Isolated copies of the stack, locally only | `make test-infra` | About 12 minutes with 2 workers (measured in #192) |
| [Data quality checks](#data-quality-checks) | The data itself: each layer against the one before it, from Kafka to gold | The running stack, on demand or on a schedule | `make data-quality` | One Spark application; grows with the data |

- Unit tests, lint, types and the docs build run on every pull request; the infra tests and the data quality checks need Docker and run locally.
- The pre-commit hooks run the code checks before every commit that touches code ([pre-commit hooks](#pre-commit-hooks)).
- Coverage stays at or above 90% (branch coverage), or `make test` fails.

## Unit tests, lint and types

```sh
make test       # unit tests with branch coverage; fails below 90%
make lint       # ruff lint and format check
make format     # format the code and apply safe lint fixes (files keep your ownership)
make typecheck  # check types with ty; any diagnostic fails
make schemas    # render every lake table and view as a flat Avro schema, for reading
```

- **Unit tests** live next to each project and mirror its source tree (`analytics/<project>/tests/test_<module>.py`). The Grafana dashboard has its own tests in `tests/grafana/` ([Grafana](../serving/grafana.md)).
- **Parallel runs:** `make test` spreads the test files over 4 [pytest-xdist](https://pytest-xdist.readthedocs.io/) workers in each image (in the app image each worker has its own local Spark session); set `PYTEST_WORKERS` to change that (`PYTEST_WORKERS=auto` for one per CPU, as CI does, or `0` to run serially).
- **Coverage** is measured with branch coverage in each image's run: `pipelines` and `common` in the app image, `api` and `common` in the API dev image. The 90% floor (`[tool.coverage.report] fail_under` in `pyproject.toml`) applies to each run, and `make test` fails if either run is below it, even when every test passes.
- **Lint and format** use [ruff](https://docs.astral.sh/ruff/), configured in `pyproject.toml` (`[tool.ruff]`).
- **Typing** is required for all code: ruff's `ANN` rules fail `make lint` when an annotation is missing, and [ty](https://docs.astral.sh/ty/) checks that the annotations are correct. ty is in beta, so its version is pinned in `uv.lock`; it only runs in the containers.

??? info "Where the checks run"

    The checks run in throwaway containers with `analytics/` and `tests/` mounted, so they pick up edits without a rebuild:

    - the pipelines and `common` in the Spark `app` image, with the cluster's exact Spark and Python versions;
    - the API and `common` in the API's own dev image (`docker/api/Dockerfile`, target `dev`), which has the API's dependencies and no Spark.

    They use their own Compose project, named after the checkout folder (`deel-checks-<folder>`, lowercased, with every character other than letters, digits, `_` and `-` replaced by `-`), with one app image (`<project>-spark-app`) and one API dev image (`<project>-api-dev`), so checks in different checkouts or worktrees never share an image and can run in parallel.

    ty checks calls into PySpark, whose types it reads from the Spark image (`make typecheck` passes `--extra-search-path /opt/spark/python`), and into FastAPI, which only the API dev image has, so the API code is checked there.

??? info "Combined coverage and the coverage badge"

    `make test` keeps each run's coverage data in the git-ignored `target/coverage/` (`.coverage.app` and `.coverage.api`). The `test` job in CI combines both into one total, with `common` counted once, and writes it as a [shields.io endpoint](https://shields.io/badges/endpoint-badge) JSON. After a green run on a push to `main`, the `coverage badge` job commits that JSON as `coverage.json` to the orphan `badges` branch, which the README's coverage badge reads.

## Docs build

`make docs` builds this site in strict mode, so any warning fails it, a broken link or anchor included; `make docs-serve` serves it on <http://localhost:8001> and rebuilds on every change. The [table reference](../reference/index.md) is rendered from the Avro schemas on every build, and the [latest latency results](../operations/latency-results.md) and [latest data quality results](../lakehouse/data-quality-results.md) from the reports committed under `docs/results/`. `make docs-results` refreshes those reports from the lake: it copies the newest latency report (JSON and HTML) and the newest data quality report, so a fresh measurement is one `make latency-probe` or `make data-quality`, one `make docs-results` and a commit away from the site.

## Infra tests

Infra tests check the platform itself rather than the pipeline code. They need Docker and [uv](https://docs.astral.sh/uv/) on the host, and run locally only (not in CI):

```sh
make test-infra                   # in 2 parallel workers
make test-infra INFRA_WORKERS=3   # more workers, if Docker has the memory; 0 runs serially
```

- Each worker starts its own isolated copy of the stack, so the tests never touch your running stack or its data, and runs from different worktrees can run in parallel.
- Every worker needs the Docker memory of a full stack (about 2 GB plus the Spark drivers).
- The tests submit the apps in `tests/infra/apps/` with `make spark-submit` and check the results.

??? info "How the test stacks are isolated"

    - Each [pytest-xdist](https://pytest-xdist.readthedocs.io/) worker starts its own Compose project named after the checkout folder and the worker (`deel-infra-tests-<folder>-gw0`, `-gw1`, ...; `deel-infra-tests-<folder>` in a serial run), sanitised like the code-check project, with no host ports.
    - The tests of one module always run in the same worker, and the modules that share the bronze, silver and landing scenarios run together, so every scenario runs once per run.
    - Each project builds one Spark image (`<project>-spark`, shared by the master and the workers), one app image (`<project>-spark-app`), the API dev image (`<project>-api-dev`) and the API image (`<project>-api`); at the end the project is removed with its volumes and those four images.
    - The images are built once when a worker's stack starts, so the test runs of `make spark-submit` and `make spark-sql` skip their build check (`DRIVER_BUILD=`).
    - To launch fewer Spark drivers, the bronze, silver and replay scenarios read their tables back with DuckDB (`tests/infra/apps/lake_read.py`, with the extensions installed in the app image) through the REST catalog, which starts on first use, or straight from the table files for the replay's own catalog, in one app container kept running for the whole worker, and read the table and column descriptions from the REST catalog; only the silver views, which DuckDB cannot read, are read back by a Spark driver.
    - Records staged for the landing job's file sink are landed in the driver of the job that runs next (`tests/infra/apps/land_staged.py` runs the job's script after the landing).

??? info "What the infra tests check: the platform"

    - the Iceberg smoke job writes catalog metadata to PostgreSQL and table files to SeaweedFS
    - executors write and read Avro files on `s3a://lake/` and write and read a Kafka topic with the bundled connectors
    - drivers and executors run the same Spark (4.1.4) and Python (3.14) versions
    - each application run gets its own driver container, which is removed when the application ends
    - two applications capped with `spark.cores.max=2` run at the same time, both with executors, in two separate driver containers

??? info "What the infra tests check: landing, bronze and silver"

    - the landing example queries (`make spark-sql`) return the expected records from neutral, landing-shaped fixture data (topics `topic-a` and `topic-b`), including an update, a delete, a tombstone and a snapshot read, and inspect only the chosen topic
    - the bronze job, run with `--available-now` on records landed through the landing job's file sink (topics `test.operations.<table>`), writes typed rows with their change metadata to each entity's Iceberg table, partitioned by publish day; quarantines invalid records while valid ones flow, each with the path of the landing file it was read from, and drops tombstones; after new records land, a second run appends only those, and a third run with nothing new appends nothing, so every Kafka offset is written exactly once; and ignores an Avro file in a landing folder that the commit log does not record
    - the silver job, run with `--available-now` on the bronze tables those runs leave, writes one version row per bronze change to each entity's silver table, with the bronze values and change metadata and `_is_deleted` true only for the delete, which keeps the last known values; partitions the table by change day and describes every table and column; after bronze appends another change, that change delivered again and an already versioned change delivered again, a second run appends only the new change once, and a third run with nothing new appends nothing; keeps its deduplication state in RocksDB and commits it as a changelog (the checkpoint's state folder holds RocksDB changelog files); and exits with an error when a bronze table is missing
    - the views job, run after each silver run, creates each entity's history and current views in the JDBC catalog, where the history view folds the unchanged update but keeps the delete and the change after it, and the current view holds the live row with the source values; describes every view and column from its template; leaves the views' metadata files untouched when it runs again with nothing changed; serves the views to `make spark-sql`; and fails before creating any view when a silver table is missing
    - the same job skips the gold views in `lake.gold` while the gold tables in `lake.gold_internal` do not exist, so it creates only the silver views on the silver scenario

??? info "What the infra tests check: gold"

    - the gold DDL applies again to an existing database, describes every schema, table, column and index, creates the staging tables unlogged, keeps only views without the pipeline-only columns (`_commit_sequence`, `_content_hash`, `_is_deleted`, `_is_folded`) in `gold` and hides deleted rows from its current-state views, routes history rows to the default partition, rejects replayed history rows, resolves the customer version valid at each order change in `gold.fact_order_history` (the first version for an earlier change, none for an unknown customer), creates the business question views with the columns and types of their schemas and the partial indexes on the open orders behind them, whose plans read the open orders through those indexes, and the four business queries return the expected rows, computed when read from the current-state tables, on a fixture with a completed order, a deleted order, an unknown status and ties for third place (each test rolls its rows back)
    - gold retention drops only the monthly history partitions that end before the window (orders and order items alike), first gives every month the default partition holds a partition of its own, creates the months ahead, keeps the current-state row of an open order whose history it dropped, and drops nothing more when it runs again (each test rolls its rows back); `make gold-retention` and the first run of the `gold-retention-job` service drop a committed out-of-window month
    - the rebuild job, run on the gold facts' fixture silver tables, recreates the `gold_internal` Iceberg tables and the lake gold views over them, and ends with the same dimensions, history and current state in PostgreSQL as the gold job streaming the same changes, and with a cutoff the same as the gold job streaming only the changes up to it, late ones included (surrogate keys compared by the version they point at); `make gold-rebuild` refuses without `CONFIRM=1`
    - `make gold-reset` refuses without `CONFIRM=1`, recreates `gold` and `gold_internal` empty in PostgreSQL, drops the gold Iceberg tables in `lake.gold_internal`, and the lake gold views, can run again, deletes the gold checkpoints and keeps the silver checkpoints and the Iceberg catalog, and the service logins can still read the gold views after it
    - the gold job logs one line per batch and load with the time and Spark jobs of each step; on a realistic sample (a snapshot, then four source commits) every batch runs a few Spark jobs and writes only its own load's tables
    - the gold job, run with `--available-now` on silver-shaped fixture tables in a catalog of their own (`gold_fixture`, with its own warehouse), keeps the SCD2 dimensions in PostgreSQL, checked through the versions and public views: a price change closes the previous version and opens a new one, each key has exactly one current version, replaying every batch from a fresh checkpoint changes nothing, a late older version goes into history without becoming current, a re-snapshot or audit-only update is folded and a late differing version between two equal ones unfolds the later one, and a delete leaves no current version that replays or late versions could revive (the test removes its rows afterwards)
    - the gold job, run on the silver-shaped fixture tables of the fact scenario in a catalog of their own (`gold_lake_fixture`), creates the 14 gold views in `lake.gold` over its `lake.gold_internal` tables, which a refresh then leaves untouched; each holds exactly the rows of PostgreSQL's `gold` view of the same name for the fixture's keys, surrogate keys included, the calendar and status views every row, and `data_freshness` has a time for every entity

??? info "What the infra tests check: serving"

    - the API's repository, run as `api_reader` in the API dev image on the business-question fixture, returns exactly the rows of each query in `analytics/sql/gold/`, and its freshness matches `gold.data_freshness` for every entity, with a lag for each one that has data
    - the `api` service starts healthy; `/health` reports the database reachable; each metric endpoint returns exactly the rows of its query in `analytics/sql/gold/` on the business-question fixture; `/freshness` lists every entity; `/openapi.json` and `/docs` are served; and while `analytics-db` is stopped `/health` answers 503, then the API reconnects once the database is back
    - the API's service login, connecting with its password, reads every `gold` view, is refused `gold_internal`, starts read-only with a 5 s statement timeout, cannot insert into a view or create a table in `gold` or `public` even after turning read-only off, and `make db-logins` restores a broken login and can run again
    - Grafana starts healthy with the Gold datasource provisioned read only and connected, and anonymous visitors can browse; its service login reads every `gold` view, is refused `gold_internal`, starts read-only with a 30 s statement timeout, and `make db-logins` restores it; the Gold metrics dashboard is provisioned in the Gold folder as the home page, each business-question panel returns through Grafana the same rows as its `gold` view on the current-state fixture, and the freshness panel returns one lag per entity; the Spark and Iceberg tables dashboards are provisioned in the Spark and lake folder, and a search by the links bar's `spark-and-lake` tag finds them all
    - with the `metrics` profile, Prometheus scrapes a running `make spark-submit` streaming driver under `service="spark-app"` and stores its query's rates as `spark_streaming_*{query=...}` and its executors' metrics per application, scrapes the master, the applications page and both workers, keeps the samples when its container is replaced, and the alert rules pass their `promtool` tests; Grafana's Prometheus datasource connects; the stage exporter, started before a batch application, serves that application's completed stages, which Prometheus stores under its name; and it measures the Iceberg smoke table through the REST catalog: its data files, bytes, records and small files

??? info "What the infra tests check: operations"

    - the bronze replay, in its own Iceberg catalog on the stack, appends nothing for a range the bronze stream already wrote, appends each change of a range the stream never saw exactly once (including a re-sent copy inside the range), and quarantines a failing record once however many times it is replayed
    - `make lake-maintenance` on a test table built from six one-row commits leaves one data file with the same rows, rewrites the position delete files (none on that table) and adds only a `replace` snapshot, skips a namespace that does not exist, and the `maintenance-job` service runs the same maintenance on its first loop

## Data quality checks

Data quality checks are not tests: they verify the data on the running stack, not the code or the platform. Each check compares one layer with the one before it, from Kafka to the PostgreSQL copy of gold (the gold check also checks the gold model's invariants on the Iceberg tables), writes a JSON report to the lake and exits with code 1 if anything is missing, duplicated or different. They run with `make data-quality`, or on a schedule with the `data-quality` profile, and never inside `make test-infra`; their logic is unit-tested on small DataFrames ([data quality checks](../lakehouse/data-quality.md)).

## Pre-commit hooks

Install the hooks once per clone (needs [uv](https://docs.astral.sh/uv/) and Docker):

```sh
make hooks
```

Every `git commit` then runs, in order, stopping at the first failure:

| Hook | Runs when the commit touches | Where |
| --- | --- | --- |
| `uv.lock` matches `pyproject.toml` | any `pyproject.toml`, `uv.lock` | host |
| ruff check, ruff format check | Python files, `pyproject.toml` | host, with the ruff version from `uv.lock` |
| ty | `analytics/`, `tests/`, `pyproject.toml`, `uv.lock` | app and API dev containers (`make typecheck`) |
| unit tests with the 90% coverage floor | `analytics/`, `tests/`, `pyproject.toml`, `uv.lock` | app and API dev containers (`make test`) |

A commit that only changes documentation or configuration skips all of them. Commits that touch code take about 20–30 seconds longer, mostly for the container checks. Run `make format` to fix formatting before committing again. `git commit --no-verify` skips the hooks, which is why CI runs the same checks.

## Continuous integration

[`.github/workflows/ci.yml`](https://github.com/jonathan5p/deel-home-task/blob/main/.github/workflows/ci.yml) runs the code checks on every pull request to `main` (before merging) and on every push to `main` (after merging):

| Job | What it runs |
| --- | --- |
| `lint` | `uv lock --check`, `ruff check`, `ruff format --check` |
| `typecheck` | `make typecheck` in the app image and the API dev image |
| `test` | `make test` in the app image and the API dev image, each with the 90% coverage floor, then the combined coverage |
| `coverage badge` | On pushes to `main` only, after `lint`, `typecheck` and `test` pass: commits the combined coverage to the `badges` branch |

The two images are built once per job with Docker's layer cache stored in GitHub Actions, and `make` reuses it (`APP_BUILD=`), so CI runs exactly the commands used locally. Infra tests are not part of CI; run them locally with `make test-infra`.

[`.github/workflows/docs.yml`](https://github.com/jonathan5p/deel-home-task/blob/main/.github/workflows/docs.yml) builds the docs site with `make docs` (strict, so any warning, a broken link or anchor included, fails it) whenever a pull request to `main` or a push to `main` changes the docs, `mkdocs.yml`, `tools/docs/`, the schemas or the Python dependencies; on a push to `main` it also deploys the site to GitHub Pages.
