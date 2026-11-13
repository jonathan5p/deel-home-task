from collections.abc import Iterable
from enum import StrEnum


class VersionColumn(StrEnum):
    IS_FOLDED = "_is_folded"
    VERSION_NUMBER = "_version_number"
    VALID_TO = "_valid_to"
    IS_CURRENT = "_is_current"


VERSION_COLUMNS = tuple(str(column) for column in VersionColumn)


def unversioned(columns: Iterable[str]) -> list[str]:
    """The columns that are not version columns, in order: those a change is loaded with."""
    return [column for column in columns if column not in VERSION_COLUMNS]
