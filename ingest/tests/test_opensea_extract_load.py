from typing import List, Dict
import pytest
import json

from src.opensea_extract_load import OpenseaExtractLoad
from src.config import config


class TestOpenseaExtractLoad:

    @pytest.fixture(scope="class")
    def instance(self) -> OpenseaExtractLoad:
        return OpenseaExtractLoad()

    @pytest.fixture(scope='class')
    def api_response(self, instance) -> List[Dict]:
        return instance._get_data(5)

    def test_get_data(self, instance, api_response):
        assert len(api_response) == 5
        assert isinstance(api_response, list)
        assert isinstance(api_response[0], dict)

        offset_id = api_response[-1]['id']
        offset_response = instance._get_data(1, offset_id)
        assert len(offset_response) == 1
        assert offset_id != offset_response[-1]['id']

    def test_format_data(self, instance, api_response): 
        # first and second responses
        s_response = instance._get_data(1, api_response[-1]['id'])

        data = [api_response, s_response]

        f_data = instance._format_data(data)

        assert len(f_data) == 6
        assert isinstance(f_data, list)
        assert isinstance(f_data[0], str)

        assert json.loads(f_data[0]) == api_response[0]
        assert json.loads(f_data[-1]) ==  s_response[-1]


    def test_insert_data(self, cursor, instance, api_response):
        # formatted data
        f_data = instance._format_data(api_response)

        instance._insert_data(f_data)

        db_data = [col[0] for col in cursor.execute(f"SELECT * FROM {config['table']}")]

        assert len(f_data) == len(db_data)
        assert json.loads(f_data[0]) == json.loads(db_data[0])
        assert json.loads(f_data[-1]) == json.loads(db_data[-1])

    def test_get_last_id(self, cursor, instance, api_response):
        # clear tables
        cursor.execute(f"TRUNCATE TABLE {config['table']}")
        max_id = ""

        id = instance._get_last_id()
        assert id == max_id # should be no max id

        for json in api_response:
            if json['id'] > max_id:
                max_id = json['id'] 

        # re-ingest data
        f_data = instance._format_data(api_response)
        instance._insert_data(f_data)

        # ensure id as expected
        id = instance._get_last_id()
        assert id == max_id
