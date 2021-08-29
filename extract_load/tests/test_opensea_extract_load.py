import pytest

from src.opensea_extract_load import OpenseaExtractLoad


class TestOpenseaExtractLoad:

    @pytest.fixture(scope="class")
    def instance(self) -> OpenseaExtractLoad:
        return OpenseaExtractLoad()

    def test_get_data(self, instance):
        response = instance._get_data(5)
        assert len(response) == 5
        assert isinstance(response, list)
        assert isinstance(response[0], dict)

        offset_id = response[-1]['id']
        offset_response = instance._get_data(1, offset_id)
        assert len(offset_response) == 1
        assert offset_id != offset_response[-1]['id']

    def test_format_data(self, instance): 
        # first and second responses
        f_response = instance._get_data(5)
        s_response = instance._get_data(1, f_response[-1]['id'])

        data = [f_response, s_response]

        f_data = instance._format_data(data)

        assert len(f_data) == 6
        assert isinstance(f_data, list)
        assert isinstance(f_data[0], dict)

        assert f_data[0] == f_response[0]
        assert f_data[-1] ==  s_response[-1]
