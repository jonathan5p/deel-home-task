import pytest

import pipelines.schemas


def test_the_landing_schema_is_exported_by_the_package() -> None:
    landing = pytest.importorskip("pipelines.schemas.landing")

    assert pipelines.schemas.KAFKA_RECORD_SCHEMA is landing.KAFKA_RECORD_SCHEMA


def test_an_unknown_name_is_not_an_attribute_of_the_package() -> None:
    with pytest.raises(AttributeError, match=r"'pipelines\.schemas' has no attribute 'LANDING'"):
        _ = pipelines.schemas.LANDING
