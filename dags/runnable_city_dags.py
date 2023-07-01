from datetime import datetime, timedelta

from data_models import MetroArea
from operators import (FetchAirQualityOperator, CustomEmailOperator,
    FetchPrecipitationOperator, MongoFetchCityOperator, MongoRecordWeatherDataOperator)

from airflow import DAG
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

import pymongo

mongo_client = pymongo.MongoClient(Variable.get("MONGO_CONNECTION"))
cities_db = mongo_client[Variable.get("MONGO_DATABASE")]
cities_collection = cities_db[Variable.get("MONGO_COLLECTION")]

all_metro_areas = list(cities_collection.find())
metro_areas = list(map(lambda m:MetroArea(m['country'], m['state'], m['city'], m['latitude'], m['longitude']), all_metro_areas))

with DAG(
    dag_id="Running_DAG",
    start_date= datetime(2023,1,1),
    schedule= "0 12 * * *",
    catchup=False,
    default_args={
        "retries": 2,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:

    report_operators = []
    for metro_area in metro_areas:
        with TaskGroup(group_id=f'{metro_area.city_name}_requirements') as fetch_data_operators:
            air_quality_operator = FetchAirQualityOperator(task_id=f'{metro_area.city_name}_air_quality', metro_area=metro_area)
            precipitation_operator = FetchPrecipitationOperator(task_id = f'{metro_area.city_name}_precipitation', metro_area=metro_area)
            fetch_city_operator = MongoFetchCityOperator(task_id = f'{metro_area.city_name}_city_info', conn_id='mongo_default', metro_area = metro_area)
        
        weather_report_operator = MongoRecordWeatherDataOperator(task_id = f'{metro_area.city_name}_weather_report', conn_id='mongo_default', metro_area = metro_area)
        fetch_data_operators >> weather_report_operator
        report_operators.append(weather_report_operator)

    report_operators >> CustomEmailOperator(task_id = "send_email", city_names=list(map(lambda m: m.city_name, metro_areas)))