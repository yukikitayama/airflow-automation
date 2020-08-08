from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Yuki',
    'start_date': datetime(2020, 7, 22)
}

dag = DAG(
    dag_id = 'psco_short_term_load_forecast',
    catchup = False,
    schedule_interval = '0 20 * * *',
    default_args = default_args
)

bc1 = '/usr/local/bin/papermill /home/ubuntu/github/airflow-automation/01_notebooks/01_train_daily.ipynb /home/ubuntu/github/airflow-automation/01_notebooks/01_output.ipynb'
task1 = BashOperator(
    task_id = 'daily_training',
    bash_command = bc1,
    dag = dag
)

bc2 = '/usr/local/bin/papermill /home/ubuntu/github/airflow-automation/01_notebooks/02_extract_nam.ipynb /home/ubuntu/github/airflow-automation/01_notebooks/02_output.ipynb'
task2 = BashOperator(
    task_id = 'extract_temperature',
    bash_command = bc2,
    dag = dag
)

bc3 = '/usr/local/bin/papermill /home/ubuntu/github/airflow-automation/01_notebooks/03_post_processing_nam.ipynb /home/ubuntu/github/airflow-automation/01_notebooks/03_output.ipynb'
task3 = BashOperator(
    task_id = 'post_processing',
    bash_command = bc3,
    dag = dag
)

bc4 = '/usr/local/bin/papermill /home/ubuntu/github/airflow-automation/01_notebooks/04_forecast_load.ipynb /home/ubuntu/github/airflow-automation/01_notebooks/04_output.ipynb'
task4 = BashOperator(
    task_id = 'forecast_load',
    bash_command = bc4,
    dag = dag
)

bc5 = '/usr/local/bin/papermill /home/ubuntu/github/airflow-automation/01_notebooks/05_monitor_performance.ipynb /home/ubuntu/github/airflow-automation/01_notebooks/05_output.ipynb'
task5 = BashOperator(
    task_id = 'monitor_performance',
    bash_command = bc5,
    dag = dag
)

task1 >> task2 >> task3 >> task4 >> task5