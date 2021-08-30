import pytest

from src.db_connection import get_snowflake_connection
from src.config import config


@pytest.fixture(scope="session")
def setup_db():
    ctx = get_snowflake_connection()
    cs = ctx.cursor()
    try: 
        cs.execute(
            f"""
            CREATE OR REPLACE TRANSIENT TABLE
                {config['table']} (data VARIANT)
            """
        )
        yield
    finally:
        cs.close()
    ctx.close()
