# Analytics API

The analytics API ([FastAPI](https://fastapi.tiangolo.com/)) serves the four business questions and gold freshness on http://localhost:8000, with interactive docs on `/docs`. It starts with `make up` and reads only the `gold` views, as the read-only login `api_reader` ([service logins](../architecture/infrastructure.md#read-only-service-logins)), through a connection pool ([configuration](../operations/configuration.md)).

| Endpoint | Answer |
| --- | --- |
| `GET /metrics/open-orders-by-delivery-date-and-status` | `[{"delivery_date", "status_code", "open_orders"}]`, ordered by delivery date and status |
| `GET /metrics/top-delivery-dates-by-open-orders` | `[{"delivery_date", "open_orders"}]`, most open orders first; ties for third place included |
| `GET /metrics/pending-items-by-product` | `[{"product_id", "pending_items", "pending_quantity"}]`, ordered by product |
| `GET /metrics/top-customers-by-pending-orders` | `[{"customer_id", "customer_name", "pending_orders"}]`, most pending orders first; ties for third place included |
| `GET /health` | `200 {"status": "ok", "database": "reachable"}`, or `503 {"status": "unavailable", "database": "unreachable"}` |
| `GET /freshness` | `{"checked_at", "entities": [{"entity", "last_changed_at", "lag_seconds"}]}`, from `gold.data_freshness` at the database's clock, in UTC |

Each metric returns the rows of its query in `analytics/sql/gold/`, in the same order. When the database is unreachable or too slow, the endpoints answer `503`; other failures answer `500` without details. The API starts while the database is down and reconnects when it is back. Plain SQL into small response models is enough for a read-only API over a few views.

```sh
curl -s localhost:8000/health
curl -s localhost:8000/metrics/top-customers-by-pending-orders
curl -s localhost:8000/freshness
docker compose logs -f api   # Access log and errors
```

The image is slim Python 3.14 with only the `api` and `common` projects, run by an unprivileged user. To run the API on the host instead:

```sh
ANALYTICS_DB_URL=postgresql://localhost:5433/analytics ANALYTICS_DB_USER=api_reader ANALYTICS_DB_PASSWORD=api_reader \
  uv run --frozen uvicorn --factory api.main:create_app_from_env --port 8001
```
