from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#5496eb'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY public.{sink}
        FROM '{source}'
        REGION 'us-east-1'
        ACCESS_KEY_ID '{id}'
        SECRET_ACCESS_KEY '{secret}'
        IGNOREHEADER 1
        delimiter ','
        IGNOREBLANKLINES
        REMOVEQUOTES
        EMPTYASNULL 
;
    """
    copy_sql_time = """
        COPY public.{sink}
        FROM '{source}_{year}-{month}.csv'
        REGION 'us-east-1'
        ACCESS_KEY_ID '{id}'
        SECRET_ACCESS_KEY '{secret}'
        IGNOREHEADER 1
        delimiter ','
        IGNOREBLANKLINES
        REMOVEQUOTES
        EMPTYASNULL 
;
    """
    copy_sql_JSON = """
        COPY public.{sink}
        FROM '{source}'
        REGION 'us-east-1'
        ACCESS_KEY_ID '{id}'
        SECRET_ACCESS_KEY '{secret}'
        FORMAT AS JSON '{jsonpath}'
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 jsonpath='',
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonPath = jsonpath
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        #         Connect to AWS and Redshift
        self.log.info('StageToRedshiftOperator implementation')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #         Remove existing data in staging table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        #         Copy new data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.jsonPath is '':
            if self.execution_date:
                formatted_sql = S3ToRedshiftOperator.copy_sql_time.format(
                    sink=self.table,
                    source=s3_path,
                    year=self.execution_date.strftime("%Y"),
                    month=self.execution_date.strftime("%m"),
                    id=credentials.access_key,
                    secret=credentials.secret_key
                )
            else:
                formatted_sql = S3ToRedshiftOperator.copy_sql.format(
                    sink=self.table,
                    source=s3_path,
                    id=credentials.access_key,
                    secret=credentials.secret_key
                )
        else:
            formatted_sql = S3ToRedshiftOperator.copy_sql_JSON.format(
                sink=self.table,
                source=s3_path,
                id=credentials.access_key,
                secret=credentials.secret_key,
                jsonpath="s3://{}/{}".format(self.s3_bucket, self.jsonPath)
            )
        redshift.run(formatted_sql)


