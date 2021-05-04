"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'user@mail.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 27),
    'email': ['user@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2018, 7, 29),
}

dag = DAG(
    'tutorial_spark_operator', default_args=default_args, schedule_interval=timedelta(1))

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
    java_class='com.ibm.cdopoc.DataLoaderDB2COS',
    application='local:///opt/spark/work-dir/SparkPi-assembly-0.1.0-SNAPSHOT.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    name='airflowspark-DataLoaderDB2COS',
    verbose=True,
    driver_memory='1g',
    conf={
        'spark.DB_URL': 'jdbc:db2://dashdb-dal13.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;',
        'spark.DB_USER': Variable.get("CEDP_DB2_WoC_User"),
        'spark.DB_PASSWORD': Variable.get("CEDP_DB2_WoC_Password"),
        'spark.DB_DRIVER': 'com.ibm.db2.jcc.DB2Driver',
        'spark.DB_TABLE': 'MKT_ATBTN.MERGE_STREAM_2000_REST_API',
        'spark.COS_API_KEY': Variable.get("COS_API_KEY"),
        'spark.COS_SERVICE_ID': Variable.get("COS_SERVICE_ID"),
        'spark.COS_ENDPOINT': 's3-api.us-geo.objectstorage.softlayer.net',
        'spark.COS_BUCKET': 'data-ingestion-poc',
        'spark.COS_OUTPUT_FILENAME': 'cedp-dummy-table-cos2',
        'spark.kubernetes.container.image': 'ctipka/spark:spark-docker',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
        },
    dag=dag,
)

t1.set_upstream(print_path_env_task)
spark_submit_task.set_upstream(t1)
