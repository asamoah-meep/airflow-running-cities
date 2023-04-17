import requests
import logging

from data_models.metro_area import MetroArea

from airflow.models import BaseOperator, Variable as AirflowVariable
from airflow.exceptions import AirflowFailException

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

    def execute(self, context):
        payload = self.build_payload()

        response = requests.get(self.api_url, params=payload)
        if response.status_code >= 400:
            logging.error("Unable to complete request")
            logging.info(response.content)
            raise AirflowFailException
        record = self.build_record(response)
        logging.info(record)
        return record

class FetchPrecipitationOperator(BaseFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "https://api.open-meteo.com/v1/forecast"

    def build_payload(self):
        return {
            "latitude": self.metro_area.latitude,
            "longitude": self.metro_area.longitude,
            "hourly": 'apparent_temperature,precipitation,cloudcover,visibility',
            "forecast_days": 1,
            "timezone": 'auto'
        }

    def build_record(self, response):
        logging.info(response)
        response_json = response.json()['hourly']
        return {
            "precipitation": response_json['precipitation'][9],
            "cloudcover": response_json['cloudcover'][9],
            "apparent_temperature": response_json['apparent_temperature'][9],
            "visibility": response_json['visibility'][9]
        }
        
class FetchAirQualityOperator(BaseFetchWeatherOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = "http://api.airvisual.com/v2/city"

    def build_payload(self):
        return {
            'country': self.metro_area.country,
            'state': self.metro_area.state,
            'city': self.metro_area.city,
            'key': AirflowVariable.get('AIR_API')
        }

    def build_record(self, response):
        response_json = response.json()['data']
        weather_data = response_json['current']['weather']
        pollution_data = response_json['current']['pollution']

        return {
            'city': self.metro_area.city,
            'timestamp': weather_data['ts'],
            'air_quality': pollution_data['aqius'],
            'temperature': weather_data['tp'],
            'humidity': weather_data['hu'],
            'wind_speed': weather_data['ws']
        }