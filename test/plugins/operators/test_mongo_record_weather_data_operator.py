import pytest

from unittest.mock import Mock, patch
from operators import MongoRecordWeatherDataOperator

class TestMongoRecordWeatherDataOperator:
    def test_get_hook(self, operator: MongoRecordWeatherDataOperator):
        operator.hook = Mock()
        assert operator.get_hook()
        
    @pytest.fixture
    def operator(self):
        return MongoRecordWeatherDataOperator(conn_id='', task_id='test', metro_area=Mock())
