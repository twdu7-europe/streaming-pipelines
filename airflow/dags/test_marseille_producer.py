from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
}

dag = DAG('test_marseille_producer', default_args=default_args)

dummy = BashOperator(
    task_id='print_date',
    bash_command='date',
)

submit_job = SparkSubmitOperator(
    application="/opt/airflow/spark_apps/test_producer_spark_job.py", task_id="submit_job"
)

dummy >> submit_job
