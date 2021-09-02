from typing import List, Dict
import pytest
import json

from src.pipelines.crypto_punk_sales_pipeline import CryptoPunkSalesPipeline, TABLE_NAME
from src.config import config


class TestCryptoPunkSalesPipeline: 

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
        return instance._get_data(5)

    def test_get_data(self, instance, api_response):
        assert len(api_response) == 5
        assert isinstance(api_response, list)
        assert isinstance(api_response[0], dict)

        offset_block_timestamp = int(api_response[-1]['timestamp'])
        offset_response = instance._get_data(1, offset_block_timestamp)

        print(api_response)

        assert len(offset_response) == 1
        assert offset_block_timestamp < int(offset_response[-1]['timestamp'])
