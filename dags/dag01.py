import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#importa criadolocalmente
from aws_dag.Repo import AWS_Firestarter


aws = AWS_Firestarter()

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(
    dag_id='s3_checker',
    description='Verifica se Existe Bucket com Parâmetro e o Cria se não existir',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

t1 = PythonOperator(
    task_id ='Iniciar_Bucket',
    python_callable=aws.firestarter,
    op_args=["f1-prj-data-lake-raw"],
    dag=dag
    )



t1