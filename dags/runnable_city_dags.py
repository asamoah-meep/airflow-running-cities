from datetime import datetime, timedelta

from data_models.metro_area import MetroArea
from operators.fetch_weather_operator import FetchAirQualityOperator, FetchPrecipitationOperator
from operators.mongo_fetch_city_operator import MongoFetchCityOperator
from operators.mongo_record_weather_data_operator import MongoRecordWeatherDataOperator

from airflow import DAG
from airflow.utils.task_group import TaskGroup

nyc = MetroArea("USA", "New York", "New York City", 40.71, -74.01)
boston = MetroArea("USA", "Massachusetts", "Boston", 42.36, -71.06)
la = MetroArea("USA", "California", "Los Angeles", 34.05, -118.24)
sf = MetroArea("USA", "California", "San Francisco", 37.77, -122.42)
dc = MetroArea("USA", "Washington, D.C.", "Washington", 38.90, -77.04)

metro_areas = [nyc, boston, la, sf, dc]

with DAG(
    dag_id="Running_DAG",
    start_date= datetime(2023,1,1),
    schedule= "@daily",
    catchup=False,
    default_args={
        
    }
) as dag:

    for metro_area in metro_areas:
        with TaskGroup(group_id=f'{metro_area.city_name}_requirements') as fetch_data_operators:
            air_quality_operator = FetchAirQualityOperator(task_id=f'{metro_area.city_name}_air_quality', metro_area=metro_area)
            precipitation_operator = FetchPrecipitationOperator(task_id = f'{metro_area.city_name}_precipitation', metro_area=metro_area)
            fetch_city_operator = MongoFetchCityOperator(task_id = f'{metro_area.city_name}_city_info', conn_id='mongo_default', metro_area = metro_area)
        
        weather_report_operator = MongoRecordWeatherDataOperator(task_id = f'{metro_area.city_name}_weather_report', conn_id='mongo_default', metro_area = metro_area)
        fetch_data_operators >> weather_report_operator
        