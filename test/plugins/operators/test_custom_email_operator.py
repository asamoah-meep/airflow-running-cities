import pytest

from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from operators import CustomEmailOperator

from airflow.utils.context import Context

class TestCustomEmailOperator:
    @pytest.mark.parametrize("tasks_succeeded, expected", [
        (True, 'Success'),
        (False, 'Failure')
    ])
    def test_execute(self, tasks_succeeded: bool, expected: str, operator: CustomEmailOperator, context: Context):
        context['ti'].xcom_pull = Mock(return_value = tasks_succeeded)
        email_mock = Mock()

        with patch('airflow.operators.email.EmailOperator.execute', new=email_mock):
            operator.execute(context)
            context['ti'].xcom_push.assert_called_with("Task status", expected)


    @pytest.fixture
    def context(self):
        task_instance_mock = Mock()
        task_instance_mock.xcom_pull = Mock(return_value=MagicMock())
        task_instance_mock.xcom_push = Mock()
        return {
            'ti': task_instance_mock,
            'logical_date': datetime.today()
        }
        
    @pytest.fixture
    def operator(self):
        return CustomEmailOperator(task_id='test', city_names=['city_1', 'city_2'])
