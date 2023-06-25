import pytest

from unittest.mock import Mock, MagicMock, patch
from operators import MongoRecordWeatherDataOperator

from airflow.utils.context import Context

class TestMongoRecordWeatherDataOperator:
    @pytest.mark.parametrize("existing_hook", [
        (None),
        (Mock())
    ])
    def test_get_hook(self, existing_hook, operator: MongoRecordWeatherDataOperator):
        operator.hook = existing_hook
        with patch("airflow.providers.mongo.hooks.mongo.MongoHook.get_connection", new = MagicMock()), \
             patch("airflow.providers.mongo.hooks.mongo.MongoHook._create_uri", new = Mock()):
        
            assert operator.get_hook()

    def test_execute(self, operator: MongoRecordWeatherDataOperator, context: Context):
        expected = "success"
       
        hook_mock = Mock()
        record_mock = Mock()
        record_mock.inserted_id = expected
        hook_mock.insert_one = Mock(return_value=record_mock)
        operator.get_hook = Mock(return_value=hook_mock)

        assert operator.execute(context) == expected

    @pytest.fixture
    def context(self):
        task_instance_mock = Mock()
        task_instance_mock.xcom_pull = Mock(return_value=MagicMock())
        return {
            'ti': task_instance_mock
        }
        
    @pytest.fixture
    def operator(self):
        return MongoRecordWeatherDataOperator(conn_id='', task_id='test', metro_area=Mock())
