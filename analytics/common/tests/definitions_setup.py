import shutil
from pathlib import Path

from common.definitions import DEFINITIONS

AUDIT_COLUMNS = ("updated_at", "updated_by", "created_at", "created_by")
THING = """
name = "things"
key = "thing_id"
description = "Things"
audit_columns = true

[columns.thing_id]
type = "bigint"
not_null = true
description = "Thing identifier"

[columns.thing_nmae]
renamed_to = "thing_name"
type = "varchar"
description = "Name of the thing"
"""


class DefinitionsTree:
    """A copy of the shipped definitions in a scratch folder, for tests that break one file."""

    def __init__(self, root: Path) -> None:
        shutil.copytree(Path(str(DEFINITIONS)), root, ignore=shutil.ignore_patterns("*.py*"))
        self.root = root

    def write(self, relative: str, text: str) -> None:
        (self.root / relative).write_text(text, encoding="utf-8")

    def write_entity(self, text: str, name: str = "things") -> None:
        self.write(f"entities/{name}.toml", text)

    def write_thing(self, *, replace: tuple[str, str] = ("", ""), extra: str = "") -> None:
        """Write the valid `things` entity with one text replaced and `extra` appended."""
        old, new = replace
        self.write_entity(THING.replace(old, new) + extra)
