import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#importa criadolocalmente
from aws_dag.Ingestor import AWS_Firestarter


aws = AWS_Firestarter()

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(
    dag_id='s3_upload',
    description='Captura Arquivos e Envia p/ S3',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

t1 = PythonOperator(
    task_id ='Captura_Arquivos',
    python_callable=aws.firestarter,
    op_args=["/opt/airflow/inputs","f1-prj-data-lake-raw"],
    dag=dag
    )


t1