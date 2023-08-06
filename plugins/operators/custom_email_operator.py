import logging

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.operators.email import EmailOperator
from airflow.utils.context import Context

class CustomEmailOperator(BaseOperator):

    city_names: list[str]

    def __init__(self, city_names: list[str], **kwargs):
        self.city_names = city_names
        super().__init__(**kwargs)

    def execute(self, context: Context):

        task_instance: TaskInstance = context["ti"]
        date = context["logical_date"].strftime('%x')
        success_map: dict[str, bool] = { city_name: bool(task_instance.xcom_pull(task_ids=f'{city_name}_flow.{city_name}_weather_report', key=city_name, default=None))
            for city_name in self.city_names }

        dag_status = "Success" if all(success_map.values()) else "Failure"
        subject = f"{date} Dag Run {dag_status}"

        city_status_set = set(map(lambda city_name: f"{city_name} {'succeeded' if success_map[city_name] else 'failed'} on {date}", self.city_names))
        content = '\n'.join(city_status_set)
        logging.info(content)

        email_op = EmailOperator(
            task_id = "send_email",
            to="meeplings@gmail.com",
            subject = subject,
            html_content = content
        )
        email_op.execute(context)
        task_instance.xcom_push("Task status", dag_status)