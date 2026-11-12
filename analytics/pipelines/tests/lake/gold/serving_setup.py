from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from pipelines.lake.gold.dimensions import STORED

SEEDS = STORED["dim_order_status"]
HISTORY = STORED["fact_order_history"]
BY_PRODUCT = STORED["pending_items_by_product"]
TOP = STORED["top_delivery_dates_by_open_orders"]
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
            "customer_key": None,
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
        " status_key int, customer_id long, customer_key long, order_date_key int,"
        " delivery_date_key int, _content_hash string, _is_deleted boolean, _is_folded boolean,"
        " _version_number long, _valid_to timestamp, _is_current boolean"
    )
    return spark.createDataFrame([tuple(row.values()) for row in rows], schema)


def report_rows(spark: SparkSession, rows: Iterable[tuple[int, int, int]]) -> DataFrame:
    return spark.createDataFrame(
        list(rows), "product_id long, pending_items long, pending_quantity long"
    )
