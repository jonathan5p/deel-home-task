import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from tests.infra.stack import Stack

APP = "tests/infra/apps/gold_from_silver.py"
ENTITIES = ("customers", "products", "orders", "order_items")
FIRST_CHANGE = datetime(2026, 11, 1, 10, 0)

type Json = dict[str, object]
type Batches = Mapping[str, Mapping[str, Sequence[Json]]]


def changed_at(sequence: int) -> datetime:
    """Commit time of the change at `sequence`: one minute per position, in commit order."""
    return FIRST_CHANGE + timedelta(minutes=sequence)


def at(sequence: int) -> str:
    """`changed_at(sequence)` as psql prints a timestamp."""
    return changed_at(sequence).strftime("%Y-%m-%d %H:%M:%S")


def version(
    content: Json, sequence: int, op: str, *, deleted: bool = False, updated_by: int = -1
) -> Json:
    """The silver columns of a version but its key; the hash covers only `content`, as
    silver's hash leaves the key and the audit columns out."""
    digest = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
    return {
        **content,
        "updated_at": changed_at(sequence).isoformat(),
        "updated_by": updated_by,
        "created_at": FIRST_CHANGE.isoformat(),
        "created_by": -1,
        "_commit_sequence": sequence,
        "_change_op": op,
        "_changed_at": changed_at(sequence).isoformat(),
        "_content_hash": digest,
        "_is_deleted": deleted,
    }


@dataclass(frozen=True, slots=True)
class SilverFixture:
    """Silver-shaped fixture tables of one gold scenario, in their own lake folder and Iceberg
    catalog, so scenarios never read each other's silver rows."""

    lake_key: str
    catalog: str
    batches: Batches

    def upload(self, stack: Stack, batch: str) -> None:
        """Upload one JSON-lines file per entity, empty when the batch has no rows for it."""
        for entity in ENTITIES:
            rows = self.batches[batch].get(entity, ())
            lines = "".join(json.dumps(row) + "\n" for row in rows)
            stack.put_object(f"{self.lake_key}/silver/{batch}/{entity}.json", lines.encode())

    def env(self, checkpoints: str) -> dict[str, str]:
        return {
            "ICEBERG_CATALOG": self.catalog,
            "LAKE_ROOT": f"s3a://lake/{self.lake_key}",
            "CHECKPOINT_ROOT": f"s3a://lake/{self.lake_key}/checkpoints/{checkpoints}",
        }

    def run_gold(self, stack: Stack, *batches: str, checkpoints: str = "main") -> None:
        """Append the batches to the fixture silver tables, then run the gold job once."""
        for batch in batches:
            self.upload(stack, batch)
        returncode, output = stack.run_app(APP, self.lake_key, *batches, env=self.env(checkpoints))
        assert returncode == 0, output

    def rebuild_gold(self, stack: Stack, *args: str) -> str:
        """Run the rebuild job on the fixture silver tables as they are; returns its output."""
        returncode, output = stack.run_app(
            APP, self.lake_key, "--rebuild", *args, env=self.env("rebuild")
        )
        assert returncode == 0, output
        return output


def keys(values: Sequence[int]) -> str:
    return ", ".join(map(str, values))
