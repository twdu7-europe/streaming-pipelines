from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
}

dag = DAG('test_marseille_producer', default_args=default_args)

submit_job = SparkSubmitOperator(
    application="/opt/airflow/spark_apps/test_producer_spark_job.py", task_id="submit_job"
)
