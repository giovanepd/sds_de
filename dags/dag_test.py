from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from tasks.tasks import load_raw_data, validate_data, transform_and_send

with DAG(
    dag_id="Dag_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/opt/airflow/dags/tasks/spark_job.py',
        name='pyspark_job',
        conn_id='spark_default',
        application_args=['--arg1', 'valor1'],
        executor_memory='2g',
        driver_memory='2g',
        dag=dag
    )

    load_task = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
        op_args=[
            Variable.get("api_url"),
            Variable.get("json_path_hdfs")
        ]
    )

    transform_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_args=[
            Variable.get("json_path_hdfs"),
            Variable.get("validated_json_path_hdfs"),
            Variable.get("json_config")
        ]
    )

    sync_task = PythonOperator(
        task_id="transform_and_send",
        python_callable=transform_and_send,
        op_args=[
            Variable.get("validated_json_path_hdfs"),
            Variable.get("json_config_sync")
        ]
    )

    spark_submit_task >> load_task >> transform_task >> sync_task