from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataAnalysisOperator(BaseOperator):
    """
    DataAnalysisOperator runs example analytics queries against the database.
    """
    ui_color = '#DA6959'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 queries=[],
                 *args, **kwargs):

        super(DataAnalysisOperator, self).__init__(*args, **kwargs)
        self.queries = queries
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator implementation')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query in self.queries:
            records = redshift_hook.get_records(query)
            logging.info('Query executed successfully')
