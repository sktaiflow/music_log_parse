"""
### DAG Documentation
이 DAG는 HivePartitionSensor를 사용하는 예제입니다.
"""
from __future__ import annotations

from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "retries": 2
}

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

with DAG(
    "flo_user_cluster_table",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=True,
    max_active_runs=5,
    tags=["test"],
    
) as dag:   