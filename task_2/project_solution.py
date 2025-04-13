from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Connection ID for Spark
connection_id = 'spark-default'

dags_dir = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id='spoly82_project_solution',
    default_args=default_args,
    description='Data pipeline for athlete data processing',
    schedule_interval=None,
    catchup=False,
    tags=["spoly82"]
) as dag:

    # 1. Landing to Bronze
    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id=connection_id, 
        verbose=1,
    )

    # 2. Bronze to Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        conn_id=connection_id,
        verbose=1,
    )

    # 3. Silver to Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        conn_id=connection_id,
        verbose=1,
    )

# Pipeline dependency
landing_to_bronze >> bronze_to_silver >> silver_to_gold