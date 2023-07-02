import pytest
from random import randint
from unittest.mock import Mock, MagicMock, patch

import sys
from pprint import pprint
pprint(sys.path)

from helpers import MetroAreaHelper
from data_models import MetroArea

class TestMetroAreaHelper:

    def test_fetch_all_metro_areas(self, helper: MetroAreaHelper):
        collection_mock = Mock()
        helper.collection = collection_mock
        city = "nyc"
        state = "ny"
        country = "us"
        latitude = randint(0,10)
        longitude = randint(0,10)
        park_accessibility = randint(0,10)
        park_density = randint(0,10)
        robbery_index = randint(0,10)
        metro_dict = {
            "city": city,
            "state": state,
            "country": country,
            "longitude": longitude,
            "latitude": latitude,
            "park_accessibility": park_accessibility,
            "park_density": park_density,
            "robbery_index": robbery_index
        }
        collection_mock.find = Mock(return_value = [metro_dict])

        record_list = helper.fetch_all_metro_areas()
        record: MetroArea = next(iter(record_list))

        assert record.city == city
        assert record.park_accessibility == park_accessibility
        assert record.park_density == park_density
        assert record.robbery_index == robbery_index

    @pytest.fixture
    def helper(self) -> MetroAreaHelper:
        client_mock = MagicMock()
        database_mock = MagicMock()
        client_mock['database'] = database_mock
        database_mock['collection'] = Mock()
        with patch('pymongo.mongo_client.MongoClient.__new__', new = client_mock):
            return MetroAreaHelper(Mock(),'database', 'collection')