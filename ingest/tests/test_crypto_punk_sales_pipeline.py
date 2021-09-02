from typing import List, Dict
import pytest
import json

from src.pipelines.crypto_punk_sales_pipeline import CryptoPunkSalesPipeline, TABLE_NAME, TIMESTAMP_COL
from src.config import config


class TestArtBlocksSalesPipeline:


    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        # create test table
        cursor.execute(
            f"""
            CREATE OR REPLACE TRANSIENT TABLE
                {TABLE_NAME} (data VARIANT)
            """
        )
        return cursor


    @pytest.fixture(scope="class")
    def instance(self) -> CryptoPunkSalesPipeline:
        return CryptoPunkSalesPipeline()


    @pytest.fixture(scope='class')
    def api_response(self, instance) -> List[Dict]:
        return instance._get_data(0, 5)


    def test_get_data(self, instance, api_response):
        assert len(api_response) == 5
        assert isinstance(api_response, list)
        assert isinstance(api_response[0], dict)

        offset_block_timestamp = int(api_response[-1][TIMESTAMP_COL])
        offset_response = instance._get_data(offset_block_timestamp, 1)
        assert len(offset_response) == 1
        assert offset_block_timestamp != offset_response[-1][TIMESTAMP_COL]


    def test_format_data(self, instance, api_response): 
        # first and second responses
        s_response = instance._get_data(int(api_response[-1][TIMESTAMP_COL]), 1)

        data = [api_response, s_response]

        f_data = instance._format_data(data)

        assert len(f_data) == 6
        assert isinstance(f_data, list)
        assert isinstance(f_data[0], str)

        assert json.loads(f_data[0]) == api_response[0]
        assert json.loads(f_data[-1]) ==  s_response[-1]


    def test_insert_data(self, class_cursor, instance, api_response):
        # formatted data
        f_data = instance._format_data(api_response)

        instance._insert_data(f_data)

        db_data = [col[0] for col in class_cursor.execute(f"SELECT * FROM {TABLE_NAME}")]

        assert len(f_data) == len(db_data)
        assert json.loads(f_data[0]) == json.loads(db_data[0])
        assert json.loads(f_data[-1]) == json.loads(db_data[-1])


    def test_get_last_id(self, class_cursor, instance, api_response):
        # clear tables
        class_cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        max_block_timestamp = 0

        block_timestamp = instance._get_last_block_timestamp()
        assert block_timestamp == max_block_timestamp # should be no max id

        for json in api_response:
            block_timestamp = int(json[TIMESTAMP_COL])

            if block_timestamp > max_block_timestamp:
                max_block_timestamp = block_timestamp 

        # re-ingest data
        f_data = instance._format_data(api_response)
        instance._insert_data(f_data)

        # ensure id as expected
        block_timestamp = instance._get_last_block_timestamp()
        assert block_timestamp == max_block_timestamp
