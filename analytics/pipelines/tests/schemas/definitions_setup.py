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
    "name": "silver_template",
    "namespace": "lake",
    "doc": "Versions of {entity}: {entity_description}",
    "include": ["source", "audit"],
    "fields": [
        {
            "name": "_valid_to",
            "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
            "default": None,
            "doc": "End of the version's validity",
        },
        {"name": "_is_current", "type": "boolean", "doc": "Whether the version is current"},
    ],
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


def column_definitions(root: Path, skip: str = "sources") -> dict[str, dict[str, list[str]]]:
    """Every field definition (type, default and doc) under `root`, by field name, with the
    files that define the field that way.

    The source tables in `skip` are left out: each describes its own columns, so a foreign key
    and the primary key it points to share a name but not a description or nullability.
    """
    definitions: dict[str, dict[str, list[str]]] = {}
    for file in schema_files(root):
        relative = file.relative_to(root)
        if relative.parts[0] == skip:
            continue
        for field in json.loads(file.read_text(encoding="utf-8"))["fields"]:
            definition = json.dumps({key: field.get(key) for key in ("type", "default", "doc")})
            by_definition = definitions.setdefault(field["name"], {})
            by_definition.setdefault(definition, []).append(relative.as_posix())
    return definitions


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
        """Write the valid `silver` table template, with the value at `path` changed if given."""
        self.write("tables/silver_template.avsc", edited(SILVER, path, value) if path else SILVER)
