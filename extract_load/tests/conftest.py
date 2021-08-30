import pytest

from src.db_connection import get_snowflake_connection
from src.config import config


@pytest.fixture(scope="session")
def cursor():
    """Creates a test table and returns a cursor to interact with db
    """
    ctx = get_snowflake_connection()
    cs = ctx.cursor()
    try: 
        cs.execute(
            f"""
            CREATE OR REPLACE TRANSIENT TABLE
                {config['table']} (data VARIANT)
            """
        )
        yield cs
    finally:
        cs.close()
    ctx.close()
