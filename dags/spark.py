from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
import os
from airflow.operators.bash import BashOperator
import pendulum

local_tz = pendulum.timezone("Asia/Tehran")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='spark_job_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

cmd = """
/spark/bin/spark-submit --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=soloshik/spark:v2 \
    local:///opt/spark/work-dir/SparkPi-assembly-0.1.0-SNAPSHOT.jar
"""
t = BashOperator(task_id='Spark_datamodel',bash_command=cmd,dag=dag)
