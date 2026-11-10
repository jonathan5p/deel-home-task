import pytest

from pipelines.lake.columns import ChangeOp, MetadataColumn
from pipelines.schemas.definitions import load_table_templates

TEMPLATES = load_table_templates()


@pytest.mark.parametrize("column", MetadataColumn, ids=str)
def test_every_metadata_column_is_defined_in_a_table_template(column: str) -> None:
    assert any(column in [field.name for field in t.fields] for t in TEMPLATES.values())


def test_metadata_columns_start_with_an_underscore() -> None:
    assert all(column.startswith("_") for column in MetadataColumn)


@pytest.mark.parametrize("template", ["bronze", "silver"])
def test_the_change_op_description_names_every_kind_of_change(template: str) -> None:
    (change_op,) = (
        field for field in TEMPLATES[template].fields if field.name == MetadataColumn.CHANGE_OP
    )

    assert all(op in change_op.description for op in ChangeOp)
