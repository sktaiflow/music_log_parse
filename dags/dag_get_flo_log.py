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

    "flo_log", #name of dag appear in ui
    default_args=default_args,
    description="DAG with own plugins",
    schedule="45 10 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=True,
    max_active_runs=5,
    tags=["test"],
    
) as dag:
    dag.doc_md = __doc__

    yyyy = '{{ ds_nodash[:4] }}'
    mm = '{{ ds_nodash[4:6] }}'
    dd = '{{ ds_nodash[6:8] }}'
    
    meta_path = f"/data/music/transform/flo/track_meta/{yesterday}/_SUCCESS"
    
    start = DummyOperator(task_id='start', dag=dag)
    intermediate = DummyOperator(task_id='intermediate', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)
    
    
    
    flo_meta_sensor = WebHdfsSensor(
        task_id="flo_meta_sensor",
        webhdfs_conn_id='tidcex_hadoop',
        filepath=meta_path,
        poke_interval=60 * 1,
        timeout=60 * 60 * 24,
        dag=dag )
    
    
    test1 = NesOperator(
       task_id="test_task1",
       parameters={"current_dt": "{{ ds_nodash }}"},
       input_nb="./notebook/sense_test.ipynb",
)
    
    
    
    
    start >> flo_meta_sensor
    
    flo_meta_sensor >> test1
    
    test1 >> end
