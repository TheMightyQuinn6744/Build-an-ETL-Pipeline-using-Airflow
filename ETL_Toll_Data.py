# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Quinn Rodney',
    'start_date': days_ago(0),
    'email': ['roddy@dag.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1)
)

#Define tasks
#Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

#Create a task to extract data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1:4 vehicle-data.csv >> csv_data.csv',
    dag=dag,
)

#Create a task to extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d " " -f 5,1,7 tollplaza-data.tsv >> tsv_data.csv',
    dag=dag,
)

#Create a task to extract data from a Fixed Width File
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d " " -f 10,11 payment-data.txt >> fixed_width_data.csv',
    dag=dag,
)

#Create a task named consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d="," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

#Create a task named transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr [:lower:] [:upper:] < extracted_data.csv >> transformed_data.csv',
    dag=dag,
)

#task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
