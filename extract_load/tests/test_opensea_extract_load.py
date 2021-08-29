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
