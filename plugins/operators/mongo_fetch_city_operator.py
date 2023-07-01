import logging

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.context import Context

from data_models import MetroArea

class MongoFetchCityOperator(BaseOperator):

    conn_id: str
    hook: MongoHook|None
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
        record = self.get_hook().find(
            mongo_collection='MajorCity',
            mongo_db='RunnableCities',
            query={
                'country': self.metro_area.country,
                'state': self.metro_area.state,
                'city': self.metro_area.city
            },
            find_one=True
        )
        logging.info(record)
        #TODO: Remove this logic as we fetch all city data from DB
        reduced_record = {
            "city": self.metro_area.city,
            "park_accessibility": record['park_accessibility'],
            "park_density": record['park_density'],
            "robbery_index": record['robbery_index']
        }
        return reduced_record
