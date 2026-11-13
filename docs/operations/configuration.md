# Configuration

Settings are read from environment variables into small, immutable settings objects, one per concern. The environment is always passed in explicitly, so code and tests never read or patch the process environment:

```python
import os

from pipelines.settings import PipelineSettings

settings = PipelineSettings.from_env(os.environ)  # only a job's entry point reads os.environ
```

The code holds no deployment details: brokers, topics, the lake, the catalog and the databases are required variables with no default, and only tuning settings have defaults. The local values live in `.env`, and Compose passes them to the jobs. The source database login goes only to the [latency probe](latency.md)'s container. Override a tuning setting for one run in the shell, for example `LANDING_TRIGGER_SECONDS=30 make spark-submit ...`.

| Variable | Settings | Default | Local value in `.env` | Purpose |
| --- | --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `pipelines.settings.KafkaSettings` | required | `kafka:29092` | Kafka brokers to read CDC topics from |
| `KAFKA_TOPICS` | `pipelines.settings.KafkaSettings` | required | the four `finance_db.operations.*` topics | Comma-separated CDC topics to read |
| `LAKE_ROOT` | `pipelines.settings.LakeSettings` | required | `s3a://lake` | Root of the data lake for plain files (S3A) |
| `ICEBERG_CATALOG` | `pipelines.settings.LakeSettings` | required | `lake` | Spark catalog name for Iceberg tables |
| `LAKE_CATALOG_URI` | `pipelines.latency.settings.LakeReadSettings`, `pipelines.settings.TableStatsSettings` | required | `http://iceberg-rest:8181` | Iceberg REST catalog the [latency probe](latency.md) reads the lake tables through and the exporter's table statistics read |
| `LAKE_S3_ENDPOINT` | `pipelines.latency.settings.LakeReadSettings`, `pipelines.settings.TableStatsSettings` | required | `http://seaweedfs:8333` | URL of the lake's S3 endpoint for readers outside Spark, such as the latency probe and the exporter's table statistics, which read the table manifests (`https://` turns TLS on) |
| `LAKE_S3_REGION` | `pipelines.latency.settings.LakeReadSettings`, `pipelines.settings.TableStatsSettings` | required | `us-east-1` | Region of the lake's S3 endpoint |
| `LAKE_S3_ACCESS_KEY` | `pipelines.latency.settings.LakeReadSettings` | required | `lakeadmin` | Access key of the lake's S3 endpoint; Spark reads it from the same variable |
| `LAKE_S3_SECRET_KEY` | `pipelines.latency.settings.LakeReadSettings` | required | `lakeadmin-secret` | Secret key of the lake's S3 endpoint |
| `CHECKPOINT_ROOT` | `pipelines.settings.LakeSettings` | `<LAKE_ROOT>/checkpoints` | not set | Root for Structured Streaming checkpoints |
| `LANDING_TRIGGER_SECONDS` | `pipelines.settings.LandingSettings` | `10` | `10` | Micro-batch interval of the landing job (a positive integer) |
| `BRONZE_TRIGGER_SECONDS` | `pipelines.settings.BronzeSettings` | `10` | `2` | Micro-batch interval of the bronze job (a positive integer) |
| `SILVER_TRIGGER_SECONDS` | `pipelines.settings.SilverSettings` | `10` | `2` | Micro-batch interval of the silver job (a positive integer) |
| `GOLD_TRIGGER_SECONDS` | `pipelines.settings.GoldSettings` | `10` | `5` | Micro-batch interval of the gold job (a positive integer) |
| `GOLD_RETENTION_MONTHS` | `pipelines.settings.GoldRetentionSettings`, `pipelines.settings.GoldSettings` | `36` (3 years) | not set | Months of gold history kept in PostgreSQL; a monthly partition that ends before this window is dropped, and the gold job never copies history older than it (a positive integer) |
| `GOLD_PARTITION_MONTHS_AHEAD` | `pipelines.settings.GoldRetentionSettings` | `2` | not set | Months after the current one that the retention job creates history partitions for (a positive integer) |
| `GOLD_RETENTION_INTERVAL_HOURS` | `gold-retention-job` service | `24` | not set | Hours between two runs of the scheduled gold retention service |
| `LANDING_CHECK_LAST_OFFSETS` | `pipelines.settings.LandingQualitySettings` | all landed offsets | not set | Limit the landing data quality checks to the latest offsets of each partition (a positive integer) |
| `MAINTENANCE_NAMESPACES` | `pipelines.settings.MaintenanceSettings` | `bronze,silver,gold_internal` | not set | Comma-separated Iceberg namespaces whose tables lake maintenance covers; a namespace that does not exist yet is skipped |
| `MAINTENANCE_SNAPSHOT_RETENTION_HOURS` | `pipelines.settings.MaintenanceSettings` | `168` (7 days) | `24` | Snapshots older than this are expired (a positive integer) |
| `MAINTENANCE_RETAIN_LAST` | `pipelines.settings.MaintenanceSettings` | `10` | not set | The newest snapshots kept whatever their age (a positive integer) |
| `MAINTENANCE_ORPHAN_AGE_HOURS` | `pipelines.settings.MaintenanceSettings` | `72` (3 days) | not set | Unreferenced files younger than this are kept (at least 24, Iceberg's own floor) |
| `MAINTENANCE_DOWNSTREAM_CHECKPOINTS` | `pipelines.settings.MaintenanceSettings` | `bronze.<entity>=<CHECKPOINT_ROOT>/silver/<entity>` for each source entity, and `silver.<entity>=<CHECKPOINT_ROOT>/gold/<gold table>` for each gold stream | not set | Checkpoints of the streams reading each table, as `<namespace>.<table>=<checkpoint>[\|<checkpoint>...]` entries separated by commas; replaces the default when set |
| `MAINTENANCE_INTERVAL_HOURS` | `maintenance-job` service | `24` | `6` | Hours between two runs of the scheduled maintenance service |
| `DATA_QUALITY_INTERVAL_HOURS` | `data-quality-job` service | `24` | not set | Hours between two runs of the scheduled data quality service |
| `HISTORY_SERVER_URL` | `pipelines.settings.StageExporterSettings` | required | `http://spark-history:18080` | Spark History Server whose REST API the stage exporter reads |
| `STAGE_EXPORTER_PORT` | `pipelines.settings.StageExporterSettings` | required | `9101` (set in `docker-compose.yaml`) | Port the stage exporter serves its Prometheus metrics on |
| `STAGE_EXPORTER_POLL_SECONDS` | `pipelines.settings.StageExporterSettings` | `30` | not set | Seconds between two reads of the History Server (a positive integer) |
| `TABLE_STATS_NAMESPACES` | `pipelines.settings.TableStatsSettings` | `bronze,silver,gold_internal` | not set | Comma-separated Iceberg namespaces whose tables the exporter measures |
| `SMALL_FILE_BYTES` | `pipelines.settings.TableStatsSettings` | `33554432` (32 MiB) | not set | Data files below this size count as small (a positive integer) |
| `TABLE_STATS_INTERVAL_SECONDS` | `pipelines.settings.TableStatsSettings` | `300` | not set | Seconds between two measurements of the tables (a positive integer) |
| `PARTITION_STATS_INTERVAL_SECONDS` | `pipelines.settings.TableStatsSettings` | `3600` | not set | Seconds between two measurements of the small files per partition (a positive integer) |
| `ANALYTICS_DB_URL` | `common.settings.AnalyticsDbSettings` | required | `jdbc:postgresql://analytics-db:5432/analytics` | JDBC URL of the analytics database (gold layer) |
| `ANALYTICS_DB_USER` | `common.settings.AnalyticsDbSettings` | required | `analytics` | Analytics database user |
| `ANALYTICS_DB_PASSWORD` | `common.settings.AnalyticsDbSettings` | required | `analytics` | Analytics database password |
| `SOURCE_DB_URL` | `pipelines.latency.settings.SourceDbSettings` | required | `jdbc:postgresql://transactions-db:5432/finance_db` | JDBC URL of the source database the [latency probe](latency.md) marks rows in |
| `SOURCE_DB_USER` | `pipelines.latency.settings.SourceDbSettings` | required | `finance_db_user` | Source database user, allowed to update the source tables |
| `SOURCE_DB_PASSWORD` | `pipelines.latency.settings.SourceDbSettings` | required | `1234` | Source database password |
| `DUCKDB_EXTENSION_DIRECTORY` | `pipelines.latency.settings.LatencySettings` | required | not set: the `spark-app` image sets it to `/opt/duckdb/extensions` | Directory of the DuckDB extensions the image installs at build time; the probe loads them from there and never downloads one |
| `API_DB_POOL_MIN_SIZE` | `api.settings.PoolSettings` | `1` | not set | Connections the API keeps open to the analytics database (a positive integer, at most `API_DB_POOL_MAX_SIZE`) |
| `API_DB_POOL_MAX_SIZE` | `api.settings.PoolSettings` | `5` | not set | Most connections the API opens to the analytics database (a positive integer) |
| `API_DB_POOL_TIMEOUT_SECONDS` | `api.settings.PoolSettings` | `5` | not set | How long a request waits for a free connection before the API answers 503 (a positive integer) |

Each job builds only the settings it needs; for example `BronzeSettings` reads `KAFKA_TOPICS` and the lake settings but not Kafka itself, and `ApiSettings` uses the read-only `api_reader` login ([read-only service logins](../architecture/infrastructure.md#read-only-service-logins)). A missing required variable raises `MissingSettingsError`, listing every missing name at once; a malformed value raises `InvalidSettingError`.
