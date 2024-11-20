from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from datetime import datetime


def upload_file_to_s3():
    """Upload a CSV file to S3."""
    s3Hook = S3Hook(aws_conn_id="aws_default")
    s3Hook.load_file(
        filename="/Users/hrushi/projects/geobits-airflow/region.csv",
        key="data/region.csv",
        bucket_name="athenadb2",
        replace=True
    )


def create_table_in_athena():
    """Create an Athena table from the uploaded CSV file."""
    query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS region_table (
        Region_id INT,
        Region STRING,
        State STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ','
    )
    LOCATION 's3://athenadb2/data/'
    TBLPROPERTIES (
        'skip.header.line.count'='1'
    );
    """
    
    athena_hook = AthenaHook(aws_conn_id="aws_default")
    query_id = athena_hook.run_query(
        query=query,
        query_context={"Database": "analytics_dbt"},  # Specify the Glue database
        result_configuration={"OutputLocation": "s3://athenadb2/results/"}  # Query results path
    )
    print(f"Athena query executed successfully. Query ID: {query_id}")


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="s3_to_athena_table",
    default_args=default_args,
    description="Upload CSV to S3 and create Athena table",
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Upload file to S3
    upload_file_task = PythonOperator(
        task_id="upload_file_to_s3",
        python_callable=upload_file_to_s3
    )

    # Task 2: Create table in Athena
    create_table_task = PythonOperator(
        task_id="create_table_in_athena",
        python_callable=create_table_in_athena
    )

    # Set dependencies
    upload_file_task >> create_table_task
