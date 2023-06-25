from datetime import datetime

from data_models import MetroArea
from operators import FetchAirQualityOperator, \
    FetchPrecipitationOperator, MongoFetchCityOperator, MongoRecordWeatherDataOperator

from airflow import DAG
from airflow.utils.task_group import TaskGroup

#TODO: Move to dynamically fetch from DB
nyc = MetroArea("USA", "New York", "New York City", 40.71, -74.01)
boston = MetroArea("USA", "Massachusetts", "Boston", 42.36, -71.06)
la = MetroArea("USA", "California", "Los Angeles", 34.05, -118.24)
sf = MetroArea("USA", "California", "San Francisco", 37.77, -122.42)
dc = MetroArea("USA", "District of Columbia", "Washington, D.C.", 38.90, -77.04)
denver = MetroArea("USA", "Colorado", "Denver", 39.74, -104.98)
miami = MetroArea("USA", "Florida", "Miami", 25.79, -80.22)
metro_areas = [nyc, boston, la, sf, dc, denver, miami]

with DAG(
    dag_id="Running_DAG",
    start_date= datetime(2023,1,1),
    schedule= "0 12 * * *",
    catchup=False,
    default_args={
        "retries": 2
    }
) as dag:

    for metro_area in metro_areas:
        with TaskGroup(group_id=f'{metro_area.city_name}_requirements') as fetch_data_operators:
            air_quality_operator = FetchAirQualityOperator(task_id=f'{metro_area.city_name}_air_quality', metro_area=metro_area)
            precipitation_operator = FetchPrecipitationOperator(task_id = f'{metro_area.city_name}_precipitation', metro_area=metro_area)
            fetch_city_operator = MongoFetchCityOperator(task_id = f'{metro_area.city_name}_city_info', conn_id='mongo_default', metro_area = metro_area)
        
        weather_report_operator = MongoRecordWeatherDataOperator(task_id = f'{metro_area.city_name}_weather_report', conn_id='mongo_default', metro_area = metro_area)
        fetch_data_operators >> weather_report_operator
        