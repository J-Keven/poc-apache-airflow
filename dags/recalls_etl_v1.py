from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import io

# Função para obter a última execução da DAG a partir das variáveis do Airflow
def get_last_execution_date():
    last_execution_date = Variable.get("extract_load_s3_last_run", default_var=None)
    return last_execution_date

# Função para atualizar a data de última execução nas variáveis do Airflow
def update_last_execution_date():
    Variable.set("extract_load_s3_last_run", str(datetime.now()))

# Função para extrair dados novos ou atualizados do banco de dados
def extract_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='banco_teste_airflow')
    last_execution_date = kwargs['ti'].xcom_pull(task_ids='get_last_execution_date')
    
    if last_execution_date:
        sql = f"""
        SELECT * FROM users 
        WHERE created_at > '{last_execution_date}' 
        OR updated_at > '{last_execution_date}'
        """
    else:
        sql = "SELECT * FROM users"
    
    df = postgres_hook.get_pandas_df(sql)
    return df

# Função para carregar dados no S3
def load_data_to_s3(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_data')
    s3_hook = S3Hook(aws_conn_id='aws-teste-s3')
    
    # Convertendo o DataFrame para CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Definindo o nome do arquivo no S3
    s3_key = f"users_lake/{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    
    # Carregando o arquivo no S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name='mundo-invest-public-teste',
        replace=True
    )

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'extract_load_s3',
    default_args=default_args,
    description='Extrai dados novos ou atualizados do banco e carrega no S3 a cada 3 minutos',
    schedule_interval=timedelta(minutes=3),
    catchup=False
)

# Task para obter a data da última execução
get_last_execution_date_task = PythonOperator(
    task_id='get_last_execution_date',
    python_callable=get_last_execution_date,
    dag=dag
)

# Task para extrair os dados
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

# Task para carregar os dados no S3
load_task = PythonOperator(
    task_id='load_data_to_s3',
    python_callable=load_data_to_s3,
    provide_context=True,
    dag=dag
)

# Task para atualizar a data da última execução
update_last_execution_date_task = PythonOperator(
    task_id='update_last_execution_date',
    python_callable=update_last_execution_date,
    dag=dag
)

# Definindo a ordem das tasks
get_last_execution_date_task >> extract_task >> load_task >> update_last_execution_date_task

