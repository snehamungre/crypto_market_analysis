import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_PATH = Variable.get("crypto_project_base_path")

with DAG(
    dag_id="cryptoAnalysisPipeline",
    schedule="@daily",
    start_date=datetime.datetime(2026, 5, 20),
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id="api",
        bash_command=f"python {BASE_PATH}/src/main/python/api.py",
    )

    task_2 = SparkSubmitOperator(
        task_id="processing",
        conn_id="spark_default",
        application=f"{BASE_PATH}/target/scala-2.12/cryptomarketanalysis_2.12-1.1.jar",
        java_class="Processing",
        application_args=["{{ ds }}"],
        dag=dag,
    )

    task_3 = SparkSubmitOperator(
        task_id="analysis",
        conn_id="spark_default",
        application=f"{BASE_PATH}/target/scala-2.12/cryptomarketanalysis_2.12-1.1.jar",
        java_class="Analytics",
        application_args=["{{ ds }}", BASE_PATH],
        dag=dag,
    )

    task_1 >> task_2 >> task_3
