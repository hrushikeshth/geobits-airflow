from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook

def upload_file_to_s3():
    s3Hook = S3Hook(aws_conn_id="aws_default")
    s3Hook.load_file(
        filename="/Users/hrushi/projects/geobits-airflow/region.csv",
        key="data/region2.csv",
        bucket_name="athenadb2",
        replace=True
    )

def create_table_in_athena():
    """Creates an Athena table that references the uploaded CSV file."""
    query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS region_table_2 (
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
        query_context={"Database": "analytics_dbt"},  # Specify the Glue database where the table should be created
        result_configuration={"OutputLocation": "s3://athenadb2/results/"}  # Path for Athena query results
    )
    print(f"Athena query executed successfully.")


if __name__ == "__main__":
    # Step 1: Upload file to S3
    upload_file_to_s3()

    # Step 2: Create a table in Athena
    create_table_in_athena()