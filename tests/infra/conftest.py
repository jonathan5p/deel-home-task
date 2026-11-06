from collections.abc import Iterator

import pytest

from tests.infra.stack import Stack


@pytest.fixture(scope="session")
def stack() -> Iterator[Stack]:
    platform = Stack.isolated()
    platform.down()
    platform.up()
    try:
        yield platform
    finally:
        platform.down()
