from datetime import datetime, timedelta

from helpers import MetroAreaHelperV2
from data_models import MetroArea
from operators import (FetchAirQualityOperatorV2, CustomEmailOperator,
    FetchPrecipitationOperator, PostgresRecordWeatherDataOperator)

from airflow import DAG
from airflow.models.variable import Variable
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

helper: MetroAreaHelperV2 = MetroAreaHelperV2()

metro_areas: list[MetroArea] = helper.fetch_all_metro_areas()

with DAG(
    dag_id="Running_DAG",
    start_date= datetime(2023,1,1),
    schedule= CronTriggerTimetable("0 12 * * *", timezone='America/New_York'),
    catchup=False,
    default_args={
        "retries": 2,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:

    for metro_area in metro_areas:
        with TaskGroup(group_id=f'{metro_area.city_name}_flow') as metro_area_operators: 
            with TaskGroup(group_id=f'{metro_area.city_name}_requirements') as fetch_data_operators:
                air_quality_operator = FetchAirQualityOperatorV2(task_id=f'{metro_area.city_name}_air_quality', metro_area=metro_area)
                precipitation_operator = FetchPrecipitationOperator(task_id = f'{metro_area.city_name}_precipitation', metro_area=metro_area)
            
            weather_report_operator = PostgresRecordWeatherDataOperator(task_id = f'{metro_area.city_name}_weather_report', metro_area = metro_area)
            fetch_data_operators >> Label("Aggregate API data") >> weather_report_operator

    all_metro_area_groups = list(filter(lambda g: "_flow" in g.group_id, dag.task_group_dict.values()))
    all_metro_area_groups >> Label("Send status") >> CustomEmailOperator(task_id = "send_email", trigger_rule="all_done", city_names=list(map(lambda m: m.city_name, metro_areas)))