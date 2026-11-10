from analytics.pipelines.tests.schemas.definitions_setup import (
    Location,
    Schema,
    SchemasTree,
    edited,
)

FACT: Schema = {
    "type": "record",
    "name": "fact_things",
    "namespace": "gold",
    "doc": "Changes to things",
    "public_doc": "Changes to things, as consumers see them",
    "fields": [
        {"name": "thing_key", "type": "long", "doc": "Surrogate key of the thing"},
        {"name": "thing_id", "type": "long", "from": "things.thing_id"},
        {
            "name": "thing_name",
            "type": ["null", "string"],
            "default": None,
            "from": "things.thing_name",
        },
        {"name": "_commit_sequence", "type": "long", "doc": "Commit position"},
        {"name": "same_key", "type": "long", "from": "gold.fact_things.thing_key"},
    ],
}


def write_fact(schemas: SchemasTree, path: Location = (), value: object = None) -> None:
    """Write the valid `things` source and `fact_things` gold table, with the value at `path`
    of the gold table changed if given."""
    schemas.write_thing()
    schemas.write("gold/fact_things.avsc", edited(FACT, path, value) if path else FACT)
