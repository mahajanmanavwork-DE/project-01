from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator    
from datetime import datetime, timedelta



def get_date(**kwargs):
    date = kwargs['logical_date']
    ti = kwargs['ti']
    print(F"Date is {date}")
    ti.xcom_push(key="date",value=date) 
    print('Done')
    
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    dag_id= 'test_spark_dag',
    default_args= default_args,
    schedule= "0 0 * * *",
    start_date= datetime(2026,7,1),
    end_date = datetime(2026,7,2),
    max_active_runs=1,
    catchup= True,
    description= 'Minimal Spark test DAG'
)

task0 = PythonOperator(
    dag= dag,
    task_id= 'get_date',
    python_callable= get_date
)

task01 = SSHOperator(
    task_id='load_raw',
    dag=dag,
    ssh_conn_id='ssh_spark_minio',
    command= 'python3 /opt/spark/etl/raw/extraction.py --date {{ ti.xcom_pull(task_ids="get_date", key="date") }}',
    conn_timeout=300,
    cmd_timeout=300
)

task1 = SSHOperator(
    task_id='spark_bronze_job',
    ssh_conn_id='ssh_spark_minio',
    command='export JAVA_HOME=/opt/java/openjdk && /opt/spark/bin/spark-submit --master local[*] /opt/spark/etl/bronze/app.py --date {{ ti.xcom_pull(task_ids="get_date", key="date") }}',
    dag=dag,
    conn_timeout=300,
    cmd_timeout=300
)

task2 = SSHOperator(
    task_id='spark_silver_job',
    ssh_conn_id='ssh_spark_minio',
    command='export JAVA_HOME=/opt/java/openjdk && /opt/spark/bin/spark-submit --master local[*] /opt/spark/etl/silver/app.py',
    dag=dag,
    conn_timeout=300,
    cmd_timeout=300
)

task3 = SSHOperator(
    task_id='spark_gold_customers',
    ssh_conn_id='ssh_spark_minio',
    command='export JAVA_HOME=/opt/java/openjdk && /opt/spark/bin/spark-submit --master local[*] /opt/spark/etl/gold/app.py',
    dag=dag,
    conn_timeout=300,
    cmd_timeout=300
)


task0 >> task01 >> task1 >> task2 >> task3 # type: ignore
