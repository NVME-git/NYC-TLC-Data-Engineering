from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import *
from helpers import SqlQueries
from operators import S3ToRedshiftOperator

default_args = {
    'owner': 'nabeel',
    'start_date': datetime(2019, 1, 1),
    # 'end_date': datetime(2019, 7, 1),
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

# t0 = PostgresOperator(
#     dag=dag,
#     task_id='create_stage_tables',
#     postgres_conn_id='redshift',
#     sql=SqlQueries.create_stage_tables,
#     autocommit=True
# )
#
# t1a = S3ToRedshiftOperator(
#     dag=dag,
#     task_id='copy_taxi_zones',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     table='taxi_zones',
#     s3_bucket='nyc-tlc',
#     s3_key='misc/taxi _zone_lookup.csv'
# )
#
# t1b = S3ToRedshiftOperator(
#     dag=dag,
#     task_id='copy_green_data',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     table='stage_green',
#     s3_bucket='nyc-tlc',
#     s3_key='trip data/green_tripdata_2019-06.csv'
# )
#
# t1c = S3ToRedshiftOperator(
#     dag=dag,
#     task_id='copy_yellow_data',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     table='stage_yellow',
#     s3_bucket='nyc-tlc',
#     s3_key='trip data/yellow_tripdata_2019-06.csv'
# )
#
# t1d = S3ToRedshiftOperator(
#     dag=dag,
#     task_id='copy_fhv_data',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     table='stage_fhv',
#     s3_bucket='nyc-tlc',
#     s3_key='trip data/fhv_tripdata_2019-06.csv'
# )
#
# t1e = S3ToRedshiftOperator(
#     dag=dag,
#     task_id='copy_fhvhv_data',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     table='stage_fhvhv',
#     s3_bucket='nyc-tlc',
#     s3_key='trip data/fhvhv_tripdata_2019-06.csv'
# )

t2 = PostgresOperator(
    dag=dag,
    task_id='create_data_tables',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_data_tables,
    autocommit=True
)

t3 = PostgresOperator(
    dag=dag,
    task_id='move_time_data',
    postgres_conn_id='redshift',
    sql=SqlQueries.move_time_data,
    autocommit=True
)


# t0 >> [t1a, t1b, t1c, t1d, t1e] >> t2

t2 >> t3
