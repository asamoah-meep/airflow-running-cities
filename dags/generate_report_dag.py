from datetime import datetime, timedelta, timezone

from helpers import MetroAreaHelper
from data_models import MetroArea
from operators import CreatePlotOperator

from airflow import DAG
from airflow.models.variable import Variable
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup

helper = MetroAreaHelper(
    mongo_client=Variable.get("MONGO_CONNECTION"),
    mongo_database=Variable.get("MONGO_DATABASE"),
    mongo_collection=Variable.get("MONGO_COLLECTION")
)

metro_areas: list[MetroArea] = helper.fetch_all_metro_areas()
with DAG(
    dag_id="Report_DAG",
    start_date= datetime(2023,1,1),
    schedule= CronTriggerTimetable("0 13 1 * *",timezone="America/New_York"),
    catchup=False,
    default_args={
        "retries": 2,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    
    for metric in ["air_quality", "temperature", "humidity", "wind_speed", "precipitation", "cloud_cover", "visibility"]:
    
        plot_operator = CreatePlotOperator(task_id = f'{metric}_plot', conn_id='mongo_default', metric = metric, metro_areas = metro_areas)