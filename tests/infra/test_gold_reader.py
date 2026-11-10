import pytest

from tests.infra.gold_fixtures import PUBLIC_VIEWS
from tests.infra.gold_reader_setup import as_api_reader, break_api_reader, make_db_logins
from tests.infra.stack import Stack


@pytest.mark.parametrize("view", sorted(PUBLIC_VIEWS))
def test_the_api_login_reads_every_public_view(stack: Stack, view: str) -> None:
    run = as_api_reader(stack, f"SELECT count(*) FROM gold.{view};")

    assert run.returncode == 0, run.stderr


def test_the_api_login_cannot_read_the_internal_tables(stack: Stack) -> None:
    run = as_api_reader(stack, "SELECT count(*) FROM gold_internal.current_orders;")

    assert run.returncode != 0
    assert "permission denied for schema gold_internal" in run.stderr


def test_the_api_login_starts_read_only_with_a_statement_timeout(stack: Stack) -> None:
    run = as_api_reader(stack, "SHOW default_transaction_read_only; SHOW statement_timeout;")

    assert run.returncode == 0, run.stderr
    assert run.stdout.split() == ["on", "5s"]


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        (
            "INSERT INTO gold.dim_order_status VALUES (98, 'NOPE', true);",
            "permission denied for view dim_order_status",
        ),
        ("CREATE TABLE gold.nope (x int);", "permission denied for schema gold"),
        ("CREATE TABLE public.nope (x int);", "permission denied for schema public"),
    ],
)
def test_the_api_login_cannot_write_even_out_of_a_read_only_transaction(
    stack: Stack, statement: str, error: str
) -> None:
    run = as_api_reader(stack, f"SET default_transaction_read_only = off;\n{statement}")

    assert run.returncode != 0
    assert error in run.stderr


def test_make_db_logins_restores_the_api_login_and_can_run_again(stack: Stack) -> None:
    break_api_reader(stack)

    first = make_db_logins(stack)
    second = make_db_logins(stack)
    run = as_api_reader(stack, "SELECT count(*) FROM gold.data_freshness; SHOW statement_timeout;")

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert run.returncode == 0, run.stderr
    assert run.stdout.split() == ["4", "5s"]
