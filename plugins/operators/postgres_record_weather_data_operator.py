from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.context import Context

from data_models import MetroArea
from util import constants

INSERT_WEATHER_QUERY = """
INSERT INTO WeatherReport 
(city, uv_index, pm2_5, pm10, temperature, humidity, wind_speed, precipitation, cloud_cover, visibility)
VALUES (%(city)s, %(uv_index)s, %(pm2_5)s, %(pm10)s, %(temperature_2m)s, %(relative_humidity_2m)s, 
 %(wind_speed_10m)s, %(precipitation)s, %(cloudcover)s, %(visibility)s)
"""

class PostgresRecordWeatherDataOperator(BaseOperator):

    metro_area: MetroArea

    def __init__(self, metro_area, **kwargs):
        self.metro_area = metro_area
        super().__init__(**kwargs)

        
    def execute(self, context: Context):
        task_instance = context['ti']
        air_quality_data = task_instance.xcom_pull(task_ids = f'{self.metro_area.city_name}_flow.{self.metro_area.city_name}_requirements.{self.metro_area.city_name}_air_quality')
        precipitation_data = task_instance.xcom_pull(task_ids = f'{self.metro_area.city_name}_flow.{self.metro_area.city_name}_requirements.{self.metro_area.city_name}_precipitation')

        params = {
            'city': self.metro_area.city,
            constants.UV_INDEX: air_quality_data[constants.UV_INDEX],
            constants.PM_2_5: air_quality_data[constants.PM_2_5],
            constants.PM_10: air_quality_data[constants.PM_10],
            constants.TEMPERATURE: precipitation_data[constants.TEMPERATURE],
            constants.HUMIDITY: precipitation_data[constants.HUMIDITY],
            constants.WIND_SPEED: precipitation_data[constants.WIND_SPEED],
            constants.PRECIPITATION: precipitation_data['precipitation'],
            constants.CLOUD_COVER: precipitation_data['cloudcover'],
            constants.VISIBILITY: precipitation_data['visibility']
        }

        postgres_op = SQLExecuteQueryOperator(
            task_id="insert_weather_data",
            conn_id ="postgres_default",
            sql=INSERT_WEATHER_QUERY,
            parameters=params,
        )

        postgres_op.execute(context)
        task_instance.xcom_push(self.metro_area.city_name, True)