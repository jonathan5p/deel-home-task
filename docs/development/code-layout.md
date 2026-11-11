# Code layout

The Python code is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): one repository-wide lock file and toolchain, with one small project per component under `analytics/`. Each image installs only the project it runs and what that project depends on.

```text
pyproject.toml              workspace root: members, development tools and their settings
uv.lock                     locked versions for the whole workspace (install with uv sync --frozen)
.python-version             Python 3.14.10, the same version the Spark image runs
analytics/
  api/                      project "api": the FastAPI analytics API over the gold views, depends on common
    src/api/
      settings.py           API settings (analytics database, connection pool)
      models.py             response models
      repository.py         the repository interface the endpoints read through
      postgres.py           the repository over the gold views (psycopg connection pool)
      metrics.py, status.py the metric endpoints, and /health and /freshness
      app.py, main.py       the app built around a repository, and its entry point with the pool
    tests/
  common/                   project "common": code shared by the components
    src/common/settings.py  shared settings (analytics database) and environment helpers
    tests/
  pipelines/                project "pipelines": the Spark pipelines, depends on common
    src/pipelines/
      settings.py           pipeline settings (Kafka, lake, analytics database)
      schemas/              the Avro table and column schemas, their loader, and the landing record schema
      quality/              data quality: checks.py (the check framework), job.py (runs the checks named in CHECK), report.py (the JSON report)
      landing/              landing layer: job.py (streaming job), quality.py (data quality checks)
      lake/                 code the lake layers share: metadata column names, Spark types, Iceberg table creation and description sync, Iceberg view creation and sync, checkpoint and snapshot bounds, starting and awaiting streaming queries
      bronze/               bronze layer: source entities, the transforms from landing records to bronze rows, job.py (streaming job) and quality.py (data quality checks)
      silver/               silver layer: the transform from bronze rows to version rows, the history and current views, and job.py (streaming job)
      gold/                 gold layer: ddl.py and ddl.sql.j2 (the gold DDL from the gold Avro schemas), scd2.py (the SCD2 SQL),
                            facts.py (the history fact and current-state SQL), dimensions.py (the dimensions and facts
                            and their lookups), sink.py, postgres.py, job.py (the gold streaming job) and
                            retention.py and rebuild.py (the gold retention and rebuild jobs)
      observability/        the Prometheus exporter: exporter.py (its entry point), stage_exporter.py (completed Spark stages, read from the History Server), table_stats.py (Iceberg table statistics, read with PyIceberg) and schedule.py (their periodic tasks)
    tests/
  sql/gold/                 the four business questions as SQL on the gold views in PostgreSQL
monitoring/grafana/         Grafana's provisioning (datasources, dashboard providers) and dashboards, as code
monitoring/prometheus/      Prometheus's scrape configuration and alert rules
tests/grafana/              static checks of the Grafana dashboards
tests/prometheus/           checks of the scrape configuration and alert rules on captured Spark metric pages
tests/infra/                infra tests that drive an isolated copy of the stack
  apps/                     Spark applications they submit (UI check, Iceberg smoke check, version and hold apps, fixture helpers)
```

Pipeline code is grouped by layer, `pipelines/<layer>/` (for example `pipelines/landing/job.py` for the landing job). Tests mirror the source tree of their project: `analytics/<project>/src/<project>/<path>/<module>.py` is tested in `analytics/<project>/tests/<path>/test_<module>.py`.

PySpark is not a dependency: it comes from the Spark image, so the pipelines always run the cluster's exact Spark version.

To work on the code outside Docker, install uv and run:

```sh
uv sync --frozen          # create .venv with Python 3.14.10, every project and the dev tools
uv run pytest --no-cov    # run the tests that do not need Spark
```

Outside Docker the Spark tests are skipped, so coverage is only meaningful with `make test`, which runs every test in the image.
