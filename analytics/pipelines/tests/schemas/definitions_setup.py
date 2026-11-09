import json
import shutil
from collections.abc import Mapping
from pathlib import Path

from pipelines.schemas.definitions import SCHEMAS

type Schema = dict[str, object]
type Location = tuple[str | int, ...]

AUDIT_COLUMNS = ("updated_at", "updated_by", "created_at", "created_by")
DELETE = object()
THING: Schema = {
    "type": "record",
    "name": "things",
    "namespace": "operations",
    "doc": "Things",
    "key": "thing_id",
    "include": ["audit"],
    "fields": [
        {"name": "thing_id", "type": "long", "doc": "Thing identifier"},
        {
            "name": "thing_nmae",
            "type": ["null", "string"],
            "default": None,
            "doc": "Name of the thing",
            "renamed_to": "thing_name",
        },
    ],
}
SILVER: Schema = {
    "type": "record",
    "name": "silver",
    "namespace": "lake",
    "doc": "Versions of {entity}: {entity_description}",
    "metadata": ["_valid_from", "_valid_to", "_is_current", "_is_deleted"],
    "fields": [],
}


def edited(schema: Mapping[str, object], path: Location, value: object) -> Schema:
    """A copy of `schema` with the value at `path` replaced, or removed when it is `DELETE`."""
    result = json.loads(json.dumps(schema))
    *parents, last = path
    target = result
    for step in parents:
        target = target[step]
    if value is DELETE:
        del target[last]
    else:
        target[last] = value
    return result


def schema_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.avsc"))


class SchemasTree:
    """A copy of the shipped schemas in a scratch folder, for tests that change one file."""

    def __init__(self, root: Path) -> None:
        shutil.copytree(Path(str(SCHEMAS)), root, ignore=shutil.ignore_patterns("*.py*"))
        self.root = root

    def write(self, relative: str, schema: Mapping[str, object] | str) -> None:
        text = schema if isinstance(schema, str) else json.dumps(schema)
        (self.root / relative).write_text(text, encoding="utf-8")

    def write_thing(self, path: Location = (), value: object = None) -> None:
        """Write the valid `things` source table, with the value at `path` changed if given."""
        self.write("sources/things.avsc", edited(THING, path, value) if path else THING)

    def write_silver(self, path: Location = (), value: object = None) -> None:
        """Write the valid `silver` layer table, with the value at `path` changed if given."""
        self.write("tables/silver.avsc", edited(SILVER, path, value) if path else SILVER)
