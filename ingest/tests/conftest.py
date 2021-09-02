import pytest

from src.db_connection import get_snowflake_connection
from src.config import config


@pytest.fixture(scope="session")
def cursor():
    """Creates a test schema and returns a cursor to interact with db
    """
    # ensures we don't overwrite prod if we run outside of the ingest folder
    assert config['schema'].startswith("test")
    
    ctx = get_snowflake_connection()
    cs = ctx.cursor()
    try:
        cs.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {config['schema']}
            """
        )
        yield cs
    finally:
        cs.close()
    ctx.close()
