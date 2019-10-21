from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    # ui_color = '#358140'
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

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        #         Connect to AWS and Redshift
        self.log.info('StageToRedshiftOperator implementation')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #         Remove existing data in staging table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        #         Copy new data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # Backfill a specific date
        # if self.execution_date:
        #     formatted_sql = S3ToRedshiftOperator.copy_sql_time.format(
        #         self.table,
        #         s3_path,
        #         self.execution_date.strftime("%Y"),
        #         self.execution_date.strftime("%d"),
        #         credentials.access_key,
        #         credentials.secret_key
        #     )
        #     self.log.info("Using Date formatted SQL")
        # else:
        #     formatted_sql = S3ToRedshiftOperator.copy_sql.format(
        #         self.table,
        #         s3_path,
        #         credentials.access_key,
        #         credentials.secret_key,
        #         s3_json_path
        #     )
        #     self.log.info("Using regular formatted SQL")
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
                            sink=self.table,
                            source=s3_path,
                            id=credentials.access_key,
                            secret=credentials.secret_key
                        )
        redshift.run(formatted_sql)


