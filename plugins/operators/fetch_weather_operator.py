import requests
import logging

from data_models import MetroArea
from util import constants

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowFailException

class BaseFetchWeatherOperator(BaseOperator):
    metro_area: MetroArea
    api_url: str

    def __init__(self, **kwargs):        
        self.metro_area = kwargs.pop('metro_area')
        super().__init__(**kwargs)

    def build_payload(self):
        raise NotImplementedError("Base Weather class cannot make api calls")

    def build_record(self, response):
        raise NotImplementedError("Base Weather class cannot make api calls")
    
    def defer_when_throttled(self, response):
        raise NotImplementedError("Base Weather class cannot defer")

    def execute(self, context: Context, event=None):
        if event:
            logging.info("Task resumed at " + str(event))
        payload = self.build_payload()

        response = requests.get(self.api_url, params=payload)
        if response.status_code >= 400:
            try:
                logging.info("Unable to complete request")
                self.defer_when_throttled(response)
            except Exception:
                logging.info(response.content)
                raise AirflowFailException("Unable to complete request or defer operator")
        record = self.build_record(response)
        logging.info(record)
        return record
    
class BaseOpenMeteoFetchWeatherOperator(BaseFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def defer_when_throttled(self, response):
        raise Exception
    
    @property
    def hourly_data(self):
        raise NotImplementedError("Base Open Meteo Operator cannot define hourly data")
    
    def build_payload(self):
        return {
            constants.LATITUDE: self.metro_area.latitude,
            constants.LONGITUDE: self.metro_area.longitude,
            constants.HOURLY_DATA: self.hourly_data,
            constants.FORECAST_DAYS: 1,
            constants.TIMEZONE: constants.AUTO_TIMEZONE
        }

class FetchPrecipitationOperator(BaseOpenMeteoFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "https://api.open-meteo.com/v1/forecast"

    @property
    def hourly_data(self):
        return f'{constants.APPARENT_TEMPERATURE},{constants.PRECIPITATION},{constants.CLOUD_COVER},{constants.VISIBILITY},{constants.WIND_SPEED},{constants.HUMIDITY},{constants.TEMPERATURE}'

    def build_record(self, response):
        logging.info(response)
        response_json = response.json()[constants.HOURLY_DATA]
        return {
            constants.PRECIPITATION: response_json[constants.PRECIPITATION][9],
            constants.CLOUD_COVER: response_json[constants.CLOUD_COVER][9],
            constants.APPARENT_TEMPERATURE: response_json[constants.APPARENT_TEMPERATURE][9],
            constants.VISIBILITY: response_json[constants.VISIBILITY][9],
            constants.TEMPERATURE: response_json[constants.TEMPERATURE][9],
            constants.HUMIDITY: response_json[constants.HUMIDITY][9],
            constants.WIND_SPEED: response_json[constants.WIND_SPEED][9]
        }
    
class FetchAirQualityOperatorV2(BaseOpenMeteoFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    @property
    def hourly_data(self):
        return f'{constants.PM_10},{constants.PM_2_5},{constants.UV_INDEX}'
    
    def build_record(self, response):
        logging.info(response)
        response_json = response.json()[constants.HOURLY_DATA]
        return {
            constants.PM_2_5: response_json[constants.PM_2_5][9],
            constants.PM_10: response_json[constants.PM_10][9],
            constants.UV_INDEX: response_json[constants.UV_INDEX][9]
        }
