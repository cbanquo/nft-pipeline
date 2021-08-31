import snowflake.connector

from src.config import config


def get_snowflake_connection():
    ctx = snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        database=config['database'], 
        schema=config['schema']
    )
    return ctx
