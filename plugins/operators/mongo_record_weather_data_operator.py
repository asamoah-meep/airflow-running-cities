from datetime import datetime, timezone

from typing import Optional
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.context import Context

from data_models import MetroArea
from util import constants

class MongoRecordWeatherDataOperator(BaseOperator):

    conn_id: str
    hook: Optional[MongoHook]
    metro_area: MetroArea

    def __init__(self, conn_id, metro_area, **kwargs):
        self.conn_id = conn_id
        self.metro_area = metro_area
        self.hook = None
        super().__init__(**kwargs)

    def get_hook(self):
        if not self.hook:
            return MongoHook(self.conn_id)
        return self.hook
    
    def execute(self, context: Context):
        task_instance = context['ti']
        air_quality_data = task_instance.xcom_pull(task_ids = f'{self.metro_area.city_name}_flow.{self.metro_area.city_name}_requirements.{self.metro_area.city_name}_air_quality')
        precipitation_data = task_instance.xcom_pull(task_ids = f'{self.metro_area.city_name}_flow.{self.metro_area.city_name}_requirements.{self.metro_area.city_name}_precipitation')

        record = {
            'city': self.metro_area.city,
            constants.UV_INDEX: air_quality_data[constants.UV_INDEX],
            constants.PM_2_5: air_quality_data[constants.PM_2_5],
            constants.PM_10: air_quality_data[constants.PM_10],
            'timestamp': str(datetime.now(timezone.utc)),
            #'timestamp': air_quality_data['timestamp'],
            #'air_quality': air_quality_data['air_quality'],
            constants.TEMPERATURE: precipitation_data[constants.TEMPERATURE],
            constants.HUMIDITY: precipitation_data[constants.HUMIDITY],
            constants.WIND_SPEED: precipitation_data[constants.WIND_SPEED],
            constants.PRECIPITATION: precipitation_data['precipitation'],
            constants.CLOUD_COVER: precipitation_data['cloudcover'],
            constants.VISIBILITY: precipitation_data['visibility']
        }

        record_id = str(self.get_hook().insert_one(
            mongo_collection='WeatherReport',
            mongo_db='RunnableCities',
            doc = record
        ).inserted_id)

        task_instance.xcom_push(self.metro_area.city_name, record_id)
        return record_id
