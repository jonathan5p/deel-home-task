from pathlib import Path

import pytest

from analytics.pipelines.tests.schemas.definitions_setup import SchemasTree


@pytest.fixture
def schemas(tmp_path: Path) -> SchemasTree:
    return SchemasTree(tmp_path / "schemas")
