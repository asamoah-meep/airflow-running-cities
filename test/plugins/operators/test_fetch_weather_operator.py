import pytest

from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from operators import BaseFetchWeatherOperator, FetchPrecipitationOperator, FetchAirQualityOperator

from airflow.utils.context import Context

from util import constants

class TestBaseFetchWeatherOperator:
    def test_build_payload(self, operator: BaseFetchWeatherOperator):
        with pytest.raises(NotImplementedError):
            operator.build_payload()

    def test_build_record(self, operator: BaseFetchWeatherOperator):
        with pytest.raises(NotImplementedError):
            operator.build_record(Mock())

    def test_defer_when_throttled(self, operator: BaseFetchWeatherOperator):
        with pytest.raises(NotImplementedError):
            operator.defer_when_throttled(Mock())

    @pytest.mark.parametrize("status_code, event", [
        (200, None),
        (200, True),
        (400, None),
        (400, True)
    ])
    def test_execute(self, event, status_code, operator: BaseFetchWeatherOperator):
        operator.build_payload = Mock(return_value=Mock())
        operator.defer_when_throttled = Mock()
        operator.build_record = Mock(return_value = Mock())
        operator.api_url = Mock()

        logging_mock = Mock()
        status_mock = Mock()
        status_mock.status_code = status_code
        with patch("logging.info", new = logging_mock), \
             patch("requests.get", new = Mock(return_value=status_mock)):
            operator.execute(Mock(), event)

        assert (("Task resumed at " + str(event)) in logging_mock.call_args.args) != event
        assert operator.defer_when_throttled.called == (status_code >= 400)
   
    def test_execute_unhappy_path(self, operator: BaseFetchWeatherOperator):
        operator.build_payload = Mock(return_value=Mock())
        operator.build_record = Mock(return_value = Mock())
        operator.api_url = Mock()

        status_mock = Mock()
        status_mock.status_code = 500
        with patch("requests.get", new = Mock(return_value=status_mock)), \
             pytest.raises(Exception):
            
            operator.execute(Mock(), None)

    @pytest.fixture
    def operator(self) -> BaseFetchWeatherOperator: 
        return BaseFetchWeatherOperator(task_id="test", metro_area = Mock())
    
class TestFetchPercipitationOperator:

    def test_defer_when_throttled(self, operator: FetchPrecipitationOperator):
        with pytest.raises(Exception):
            operator.defer_when_throttled(Mock())

    def test_build_payload(self, operator: FetchPrecipitationOperator):
        payload = operator.build_payload()
        assert constants.LATITUDE in payload
        assert constants.LONGITUDE in payload
        assert constants.HOURLY_DATA in payload
        assert constants.FORECAST_DAYS in payload
        assert constants.TIMEZONE in payload

    def test_build_record(self, operator: FetchPrecipitationOperator):
        response_mock = Mock()
        response_dict = {
            constants.PRECIPITATION: MagicMock(),
            constants.CLOUD_COVER: MagicMock(),
            constants.APPARENT_TEMPERATURE: MagicMock(),
            constants.VISIBILITY: MagicMock(),
        }
        response_mock.json = Mock(return_value = {constants.HOURLY_DATA: response_dict})
        
        record = operator.build_record(response_mock)

        assert constants.PRECIPITATION in record
        assert constants.CLOUD_COVER in record
        assert constants.APPARENT_TEMPERATURE in record
        assert constants.VISIBILITY in record


    @pytest.fixture
    def operator(self) -> FetchPrecipitationOperator: 
        metro_mock = Mock()
        metro_mock.latitude = Mock()
        metro_mock.longitude = Mock()
        return FetchPrecipitationOperator(task_id="test", metro_area = Mock())

class TestFetchAirQualityOperator:

    def test_defer_when_throttled(self, operator: FetchAirQualityOperator):
        operator.defer = Mock()
        
        response = Mock()
        response_dict = {
            'message': 'Too Many Requests'
        }
        response.json = Mock(return_value = {constants.RESPONSE_DATA: response_dict})
        operator.defer_when_throttled(response)

    def test_defer_when_throttled_unhappy_path(self, operator: FetchPrecipitationOperator):
        response = Mock()
        response.json = Mock(return_value = MagicMock())
        with pytest.raises(Exception):
            operator.defer_when_throttled(response)

    def test_build_payload(self, operator: FetchPrecipitationOperator):
        
        with patch("airflow.models.Variable.get", new = Mock(return_value = Mock())):
            payload = operator.build_payload()
        
        assert constants.COUNTRY in payload
        assert constants.STATE in payload
        assert constants.CITY in payload
        assert constants.API_KEY in payload

    def test_build_record(self, operator: FetchPrecipitationOperator):
        response_mock = Mock()
        response_dict = {
            constants.CURRENT_DATA: {
                constants.WEATHER: {
                    constants.ABBREVIATED_TIMESTAMP: Mock(),
                    constants.ABBREVIATED_TEMPERATURE: Mock(),
                    constants.ABBREVIATED_HUMIDITY: Mock(),
                    constants.ABBREVIATED_WIND_SPEED: Mock()
                },
                constants.POLLUTION: {
                    constants.ABBREVIATED_AIR_QUALITY: Mock()
                }
            }
        }
        response_mock.json = Mock(return_value = {constants.RESPONSE_DATA: response_dict})
        
        record = operator.build_record(response_mock)

        assert constants.CITY in record
        assert constants.TIMESTAMP in record
        assert constants.AIR_QUALITY in record
        assert constants.TEMPERATURE in record
        assert constants.HUMIDITY in record
        assert constants.WIND_SPEED in record


    @pytest.fixture
    def operator(self) -> FetchAirQualityOperator: 
        metro_mock = Mock()
        metro_mock.city = Mock()
        metro_mock.state = Mock()
        metro_mock.country = Mock()
        return FetchAirQualityOperator(task_id="test", metro_area = Mock())
