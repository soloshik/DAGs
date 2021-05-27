from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import os
import sys
from airflow.operators.bash import BashOperator
import pendulum

os.environ['SPARK_HOME'] = '/opt/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

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
          schedule_interval="*/1 * * * *")

t1 = BashOperator(
    task_id='print_ls',
    bash_command='ls -la',
    dag=dag)

kubernetes_full_pod = KubernetesPodOperator(
    task_id='spark_kafka_submit_job',
    name='spark-kafka-job-init-container',
    namespace='default',
    image='soloshik/sparkpy:1.0',
    cmds=['/opt/spark/bin/spark-submit'],
    arguments=[
        '--master=k8s://https://aksdns-8d6caad6.hcp.westeurope.azmk8s.io:443',
        '--deploy-mode=cluster',
        '--name=spark-kafka',
        '--package=org.apache.hadoop:hadoop-aws:jar:3.2.2',
        '--package=com.amazonaws:aws-java-sdk-bundle:jar:1.11.563',
        '--package=org.apache.spark:spark-sql-kafka-0-10_2.11:jar:2.1.0',
        '--conf',
        'spark.kubernetes.container.image=soloshik/sparkpy:1.0',
        '--conf',
        'spark.kubernetes.authenticate.driver.serviceAccountName=spark',
        'local:///opt/spark/work-dir/readkafka.py'
    ],
    dag=dag
)
