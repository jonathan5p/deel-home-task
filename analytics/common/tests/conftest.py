from pathlib import Path

import pytest

from analytics.common.tests.definitions_setup import DefinitionsTree


@pytest.fixture
def definitions(tmp_path: Path) -> DefinitionsTree:
    return DefinitionsTree(tmp_path / "definitions")
