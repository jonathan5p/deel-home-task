# Engineering rules

These apply to every agent and tool working in this repository.

## Principles

1. **Every change is tested (TDD and XP, pragmatically).** Prefer writing the test first; small jumps are fine, such as writing a few lines of code before their test, as long as no change ships untested. Work in small steps with small commits, keep the design simple (build nothing that is not needed yet), refactor as you go, and keep `main` green through CI. Never bypass hooks with `--no-verify` (see [checks](checks.md)).
2. **Typed code.** All new Python is fully annotated and passes the type checker (`ty`) and ruff's annotation rules with no errors, unless the user says otherwise. Use `Any` only with a comment explaining why.
3. **Test coverage above 90%.** Unit-test coverage (branch coverage, measured by `make test`) must stay at or above 90%; CI and the pre-commit hook fail below it. Do not lower the threshold or add coverage exclusions without the user's agreement.
4. **Plan before building big features.** See [collaboration](collaboration.md#plan-before-big-features).
5. **Describe every table and column.** Every table and column created in the lake or in PostgreSQL gets a short description of what it holds, updated in the same change whenever the table or column changes. Descriptions are defined once, as the `doc` of the Avro schemas (`.avsc`) in `analytics/pipelines/src/pipelines/schemas/` (source tables in `sources/`, shared audit columns in `audit.avsc`, per-layer table templates in `tables/` (`<layer>_template.avsc`, which define the layer's own columns and `include` the source and audit columns; `make schemas` renders the flat result to the git-ignored `target/schemas/` for reading, never committed; a metadata (`_`) column used in several files must have the same definition everywhere; business columns are exempt), gold tables in `gold/`, where a column points at the description it reuses with `"from"`), and every layer reads them from there through `pipelines.schemas.definitions` and `pipelines.schemas.gold_definitions`. Iceberg: each column's `doc` and the table `comment`, set from field metadata when the table is created with `writeTo` and kept current on every start with `ALTER ... COMMENT` for the descriptions that changed. PostgreSQL: `COMMENT ON TABLE` / `COMMENT ON COLUMN`, never hand-written: `make up` renders them into the git-ignored gold DDL from the gold schemas and the template `pipelines/gold/ddl.sql.j2`.
6. **Prefix metadata columns with `_`.** On lake tables, every column the pipeline adds that is not source data (Kafka coordinates, change metadata such as `_commit_sequence`, `_change_op`, `_changed_at`, hashes, validity ranges, load times) starts with `_`, so readers can tell metadata from source and business columns at a glance. Source and business columns never start with `_`.
7. **Diagrams in Mermaid.** Every diagram in the repository is Mermaid inside Markdown, unless an image file is produced on purpose.

## Code structure

- **Tests mirror the source tree.** `src/<package>/<module>.py` is tested in `tests/<package>/test_<module>.py`. No generic `tests/unit/` bucket; platform checks live in `tests/infra/`.
- **Dependency injection, not monkeypatching.** Pass dependencies through parameters and constructors (for example `Settings.from_env(env: Mapping)` with no hidden `os.environ` fallback; only entry points pass `os.environ`).
- **Deployment-agnostic code.** No hosts, topics, bucket paths, catalog names or URLs in Python, not even as defaults. They are required environment variables; local values live in `.env` and Compose passes them in. Defaults only for tuning knobs. Tests use neutral values (`broker:9092`, `topic-a`). Moving to another deployment must be a configuration change, never a code change.
- **SOLID where it makes sense.** Small single-purpose classes composed together (for example `KafkaSettings`, `LakeSettings` and `AnalyticsDbSettings` inside `PipelineSettings`).
- **uv workspace monorepo.** The root `pyproject.toml` is the virtual workspace root (members, dev tools, tool config). Members live in `analytics/<project>/` with `src/<project>/` and `tests/`. Import names have no `deel_` prefix. Do not create folders for future components ahead of time.
- **Spark DataFrame API first.** Use the DataFrame and Catalog APIs (`writeTo(...)`, transforms, reads, writes) instead of building SQL strings. Use SQL only where it is genuinely clearer or the API lacks the operation, for example `ALTER TABLE ... ALTER COLUMN ... COMMENT` or readable ad-hoc queries.
- **Test setup in helper modules.** Test files hold only tests. Constants, sample rows, payload builders and small helpers go in a sibling module that is not prefixed `test_` (for example `parse_samples.py`), or in conftest fixtures.

## Comments

Write a comment only where a competent reader would otherwise misunderstand the code or make a wrong change: a non-obvious constraint, a surprising design choice, intent that the code cannot show. Applies to Python, SQL, Compose, Makefile, YAML, shell and config alike.

- Never narrate what the code does, restate the next line, or tell the story of how it came to be (ticket numbers, past incidents, "added because…").
- No module docstrings by default; docstrings only on functions and classes whose behaviour is not obvious.
- Compose, Makefile and job scripts stay almost comment-free; setup explanations belong in the tracked README.
- Always keep: `COMMENT ON` and Avro `doc` descriptions, the comment that justifies an `Any`, functional markers (`# noqa`, `# type: ignore`, shebangs), and reader warnings, which the user likes in uppercase (`LOCAL DEVELOPMENT CREDENTIALS ONLY`).
- When you remove a comment, do not move its text to `.agents/knowledge/m7-docs-notes.md` on your own. Whether an explanation goes to the M7 docs is the user's call, case by case; add a note there only when the user asks.
