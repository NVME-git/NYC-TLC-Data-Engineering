from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToTimeDimensionOperator(BaseOperator):

    move_sql = '''
        INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
        SELECT 	to_date ({column}, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                extract(hour from DT), 
                extract(day from DT), 
                extract(week from DT), 
                extract(month from DT), 
                extract(year from DT), 
                extract(dayofweek from DT)
        FROM public.{table}
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns="",
                 *args, **kwargs):
        super(StageToTimeDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns

    def execute(self, context):
        # Connect to RedShift
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for column in self.columns:
            formatted_sql = StageToTimeDimensionOperator.move_sql.format(
                table=self.table,
                column=column
            )
            self.log.info("Moving {column} from {table} to Time dimension table".format(
                table=self.table,
                column=column
            ))
            redshift.run(formatted_sql)
