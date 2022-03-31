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
    dag_id='s3_silver',
    description='Verifica se SilverBucket Existe e Trata os dados.',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

t1 = PythonOperator(
    task_id ='Iniciar_Bucket',
    python_callable=aws.firestarter,
    op_args=["f1-prj-data-lake-silver"],
    dag=dag
    )

t2 = PythonOperator(
    task_id ='Iniciar_Bucket',
    python_callable=aws.firestarter,
    op_args=["f1-prj-data-lake-gold"],
    dag=dag
    )

t1 >> t2