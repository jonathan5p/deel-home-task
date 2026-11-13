from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.gold.dimensions import STORED

SEEDS = STORED["dim_order_status"]
HISTORY = STORED["fact_order_history"]
ITEMS = STORED["current_order_items"]
SEED_SCHEMA = "status_key int, status_code string, is_open boolean"


def history_rows(spark: SparkSession, changes: Iterable[tuple[int, int, datetime]]) -> DataFrame:
    """Order history rows with the given key, `_commit_sequence` and `_changed_at`."""
    rows = [
        {
            "order_id": key,
            "_commit_sequence": sequence,
            "_changed_at": changed_at,
            "_change_op": "update",
            "status_key": 1,
            "customer_id": None,
            "order_date_key": None,
            "delivery_date_key": None,
            "_content_hash": "hash",
            "_is_deleted": False,
            "_is_folded": False,
            "_version_number": 1,
            "_valid_to": None,
            "_is_current": True,
        }
        for key, sequence, changed_at in changes
    ]
    schema = (
        "order_id long, _commit_sequence long, _changed_at timestamp, _change_op string,"
        " status_key int, customer_id long, order_date_key int,"
        " delivery_date_key int, _content_hash string, _is_deleted boolean, _is_folded boolean,"
        " _version_number long, _valid_to timestamp, _is_current boolean"
    )
    return spark.createDataFrame([tuple(row.values()) for row in rows], schema)


def item_rows(spark: SparkSession, rows: Iterable[tuple[int, int]]) -> DataFrame:
    """Current order item rows, each an (order_item_id, quantity) pair."""
    changed_at = datetime(2026, 11, 1, 10, 0)
    return spark.createDataFrame(
        [(key, 1, 1, quantity, key, changed_at, False) for key, quantity in rows],
        "order_item_id long, order_id long, product_id long, quantity int,"
        " _commit_sequence long, _changed_at timestamp, _is_deleted boolean",
    )
