from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "zameen_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Zameen.pk real-estate listings",
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    run_kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="python /opt/airflow/producer/kafka_producer.py",
    )

    run_spark_streamer = BashOperator(
        task_id="run_spark_streamer",
        bash_command=(
            "spark-submit --packages "
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/opt/airflow/spark/spark_streamer.py"
        ),
    )

    run_kafka_producer >> run_spark_streamer
