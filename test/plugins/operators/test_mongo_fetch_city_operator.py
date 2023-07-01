import pytest

from unittest.mock import Mock, MagicMock, patch
from operators import MongoFetchCityOperator

from airflow.utils.context import Context

class TestMongoFetchCityOperator:
    @pytest.mark.parametrize("existing_hook", [
        (None),
        (Mock())
    ])
    def test_get_hook(self, existing_hook, operator: MongoFetchCityOperator):
        operator.hook = existing_hook
        with patch("airflow.providers.mongo.hooks.mongo.MongoHook.get_connection", new = MagicMock()), \
             patch("airflow.providers.mongo.hooks.mongo.MongoHook._create_uri", new = Mock()):
        
            assert operator.get_hook()

    def test_execute(self, operator: MongoFetchCityOperator):
        hook_mock = Mock()
        city = "new york city"
        park_accessibility = 100
        park_density = 1
        robbery_index = 100
        metro_dict = {
            "park_accessibility": park_accessibility,
            "park_density": park_density,
            "robbery_index": robbery_index
        }
        hook_mock.find = Mock(return_value = metro_dict)
        operator.get_hook = Mock(return_value = hook_mock)
        operator.metro_area.city = city

        record = operator.execute(Mock())

        assert record['city'] == city
        assert record['park_accessibility'] == park_accessibility
        assert record['park_density'] == park_density
        assert record['robbery_index'] == robbery_index

    @pytest.fixture
    def operator(self) -> MongoFetchCityOperator:
        return MongoFetchCityOperator(conn_id='', task_id='test', metro_area=Mock())