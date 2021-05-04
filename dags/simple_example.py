from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'mahdyne',
    'depends_on_past': False,
    'email': ['nematpour.ma@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='simple_ex',
          default_args=default_args,
          catchup=False,
          schedule_interval="*/1 * * * *")

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

print_path_env_task = BashOperator(
    task_id='print_path_env',
    bash_command='echo $PATH',
    dag=dag)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_default',
    java_class='org.apache.spark.examples.SparkPi',
    application='local:///opt/spark/work-dir/SparkPi-assembly-0.1.0-SNAPSHOT.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    name='airflow-spark',
    verbose=True,
    driver_memory='1g',
    conf={       
        
        'spark.kubernetes.container.image': 'soloshik/spark:v2',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
        },
    dag=dag
)


