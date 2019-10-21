from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import *
from helpers import SqlQueries
from operators import S3ToRedshiftOperator

default_args = {
    'owner': 'nabeel',
    'start_date': datetime(2019, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('NYC_TLC_DAG',
          default_args=default_args,
          description='Load monthly data from S3 to Redshift for processing',
          schedule_interval='@monthly',
          catchup=False
          )

start = DummyOperator(task_id='Begin_execution',  dag=dag)

end = DummyOperator(task_id='Stop_execution',  dag=dag)

# for create_SQL in SQL_Queries.create_tables:

create_tables = PostgresOperator(
    dag=dag,
    task_id='create_tables',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_tables,
    autocommit=True
)

copy_taxi_zones = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_taxi_zones',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='taxi_zones',
    s3_bucket='nyc-tlc',
    s3_key='misc/taxi _zone_lookup.csv'
)

copy_green_data = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_green_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_green',
    s3_bucket='nyc-tlc',
    s3_key='trip data/green_tripdata_2019-06.csv'
)

copy_yellow_data = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_yellow_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_yellow',
    s3_bucket='nyc-tlc',
    s3_key='trip data/yellow_tripdata_2019-06.csv'
)

copy_fhv_data = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_fhv_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_fhv',
    s3_bucket='nyc-tlc',
    s3_key='trip data/fhv_tripdata_2019-06.csv'
)

copy_fhvhv_data = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_fhvhv_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_fhvhv',
    s3_bucket='nyc-tlc',
    s3_key='trip data/fhvhv_tripdata_2019-06.csv'
)

start >> create_tables >> [copy_taxi_zones, copy_green_data, copy_yellow_data, copy_fhv_data, copy_fhvhv_data] >> end
