import logging
from typing import Optional
from datetime import datetime, timedelta
from data_models import MetroArea

from util import constants

from airflow.models import BaseOperator, Variable as AirflowVariable
from airflow.utils.context import Context
from airflow.providers.mongo.hooks.mongo import MongoHook

import plotly.graph_objects as go

class CreatePlotOperator(BaseOperator):

    conn_id: str
    hook: Optional[MongoHook]
    metric: str
    cities: list[str]
    report_start_date: datetime
    report_end_date: datetime

    def __init__(self, conn_id, metric, metro_areas: list[MetroArea], **kwargs):
        self.conn_id = conn_id
        self.metric = metric
        self.hook = None
        self.report_end_date = datetime.today()
        last_month = (self.report_end_date.replace(day=1) - timedelta(days=1)).month
        self.report_start_date = datetime(self.report_end_date.year, last_month, self.report_end_date.day)
        self.cities = list(map(lambda m: m.city, metro_areas))
        super().__init__(**kwargs)

    def get_hook(self):
        if not self.hook:
            return MongoHook(self.conn_id)
        return self.hook

    def execute(self, context: Context):
        city_data = {city: self.map_city_record(city) for city in self.cities}
        
        logging.info(city_data)
        
        fig = go.Figure()
        for city in city_data:
            fig.add_trace(go.Scatter(
                x=list(map(lambda ele: ele['timestamp'], city_data[city])),
                y=list(map(lambda ele: ele[self.metric], city_data[city])),
                mode='lines',
                opacity=0.5,
                name=city
            ))
        fig.update_xaxes(title_text="Date")
        fig.update_yaxes(title_text=self.metric)

        fig.show()
        p_start_date = self.report_start_date.strftime('%b %y')

        logging.info(f"Writing output to reports/{self.metric}_{p_start_date}.pdf")
        fig.write_image(f"reports/{self.metric}_{p_start_date}_.pdf")

    def map_city_record(self, city):
        return list(self.get_hook().find(
            mongo_collection='WeatherReport',
            mongo_db='RunnableCities',
            query = {
                "city": city,
                "timestamp": {
                    "$gt": self.report_start_date.strftime('%Y-%m-%d'),
                    "$lt": self.report_end_date.strftime('%Y-%m-%d')
                }
            },
            projection={
                "timestamp": 1,
                self.metric: 1,
                "_id": 0
            }
        ))