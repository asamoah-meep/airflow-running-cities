import requests
import logging
from datetime import timedelta

from data_models import MetroArea
from util import constants

from airflow.models import BaseOperator, Variable as AirflowVariable
from airflow.utils.context import Context
from airflow.exceptions import AirflowFailException
from airflow.triggers.temporal import TimeDeltaTrigger

class BaseFetchWeatherOperator(BaseOperator):
    metro_area: MetroArea
    api_url: str

    def __init__(self, **kwargs):        
        self.metro_area = kwargs.pop('metro_area')
        super().__init__(**kwargs)

    def build_payload(self):
        raise NotImplemented("Base Weather class cannot make api calls")

    def build_record(self, response):
        raise NotImplemented("Base Weather class cannot make api calls")
    
    def defer_when_throttled(self, response):
        raise NotImplemented("Base Weather class cannot defer")

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

class FetchPrecipitationOperator(BaseFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "https://api.open-meteo.com/v1/forecast"

    def defer_when_throttled(self, response):
        raise Exception

    def build_payload(self):
        return {
            constants.LATITUDE: self.metro_area.latitude,
            constants.LONGITUDE: self.metro_area.longitude,
            constants.HOURLY_DATA: f'{constants.APPARENT_TEMPERATURE},{constants.PRECIPITATION},{constants.CLOUD_COVER},{constants.VISIBILITY}',
            constants.FORECAST_DAYS: 1,
            constants.TIMEZONE: constants.AUTO_TIMEZONE
        }

    def build_record(self, response):
        logging.info(response)
        response_json = response.json()[constants.HOURLY_DATA]
        return {
            constants.PRECIPITATION: response_json[constants.PRECIPITATION][9],
            constants.CLOUD_COVER: response_json[constants.CLOUD_COVER][9],
            constants.APPARENT_TEMPERATURE: response_json[constants.APPARENT_TEMPERATURE][9],
            constants.VISIBILITY: response_json[constants.VISIBILITY][9]
        }
        
class FetchAirQualityOperator(BaseFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "http://api.airvisual.com/v2/city"

    def defer_when_throttled(self, response):
        response_data = response.json()[constants.RESPONSE_DATA]
        if 'Too Many Requests' in response_data['message']:
            logging.info("Request throttled, deferring")
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=15)), method_name="execute")
        else:
            raise Exception

    def build_payload(self):
        return {
            constants.COUNTRY: self.metro_area.country,
            constants.STATE: self.metro_area.state,
            constants.CITY: self.metro_area.city,
            constants.API_KEY: AirflowVariable.get(constants.AIR_API_KEY)
        }

    def build_record(self, response):
        response_json = response.json()[constants.RESPONSE_DATA]
        weather_data = response_json[constants.CURRENT_DATA][constants.WEATHER]
        pollution_data = response_json[constants.CURRENT_DATA][constants.POLLUTION]

        return {
            constants.CITY: self.metro_area.city,
            constants.TIMESTAMP: weather_data[constants.ABBREVIATED_TIMESTAMP],
            constants.AIR_QUALITY: pollution_data[constants.ABBREVIATED_AIR_QUALITY],
            constants.TEMPERATURE: weather_data[constants.ABBREVIATED_TEMPERATURE],
            constants.HUMIDITY: weather_data[constants.ABBREVIATED_HUMIDITY],
            constants.WIND_SPEED: weather_data[constants.ABBREVIATED_WIND_SPEED]
        }