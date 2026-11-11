# Analytics API

The analytics API (`analytics/api`, [FastAPI](https://fastapi.tiangolo.com/)) serves the four business questions and the freshness of gold over HTTP. It starts with `make up` as the `api` service, on http://localhost:8000, with its interactive documentation on http://localhost:8000/docs and the OpenAPI document on http://localhost:8000/openapi.json. It reads only the `gold` views, as the read-only login `api_reader` (see [read-only service logins](../architecture/infrastructure.md#read-only-service-logins)), through a connection pool (`API_DB_POOL_*` in [configuration](../operations/configuration.md)).

| Endpoint | Answer |
| --- | --- |
| `GET /metrics/open-orders-by-delivery-date-and-status` | `[{"delivery_date", "status_code", "open_orders"}]`, ordered by delivery date and status |
| `GET /metrics/top-delivery-dates-by-open-orders` | `[{"delivery_date", "open_orders"}]`, most open orders first; ties for third place included |
| `GET /metrics/pending-items-by-product` | `[{"product_id", "pending_items", "pending_quantity"}]`, ordered by product |
| `GET /metrics/top-customers-by-pending-orders` | `[{"customer_id", "customer_name", "pending_orders"}]`, most pending orders first; ties for third place included |
| `GET /health` | `200 {"status": "ok", "database": "reachable"}`, or `503 {"status": "unavailable", "database": "unreachable"}` |
| `GET /freshness` | `{"checked_at", "entities": [{"entity", "last_changed_at", "lag_seconds"}]}`, from `gold.data_freshness` at the database's clock, in UTC |

Each metric returns the rows of its query in `analytics/sql/gold/`, in the same order; dates are ISO 8601 and unknown values `null`. While the database is unreachable, slower than the pool timeout or without readable gold views, the metric and freshness endpoints answer `503` with a `detail`; any other failure answers `500` without its details. The API starts even while the database is down, reports itself unhealthy (`docker compose ps api`) and reconnects when the database is back.

The rows are read with plain SQL into small hand-written response models (`api/models.py`), which is enough for a read-only API over a handful of views; a larger project would use an ORM or SQL toolkit such as [SQLAlchemy](https://www.sqlalchemy.org/) for the queries, the mapping and the connections.

```sh
curl -s localhost:8000/health
curl -s localhost:8000/metrics/top-customers-by-pending-orders
curl -s localhost:8000/freshness
docker compose logs -f api   # Access log and errors
```

The image (`docker/api/Dockerfile`, `<project>-api`) is a slim Python 3.14 image with only the `api` and `common` projects and their dependencies, run by an unprivileged user. To run the API on the host instead, against the analytics database's host port:

```sh
ANALYTICS_DB_URL=postgresql://localhost:5433/analytics ANALYTICS_DB_USER=api_reader ANALYTICS_DB_PASSWORD=api_reader \
  uv run --frozen uvicorn --factory api.main:create_app_from_env --port 8001
```
