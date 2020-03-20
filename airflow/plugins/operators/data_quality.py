from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#0384fc'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 queries_and_results=[{'query': '', 'result': 0}],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries_and_results = queries_and_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for quality_check in self.queries_and_results:
            self.log.info("Running data validation query")
            result = redshift.get_first(quality_check['query'])
            self.log.info(f"result: {result}")
            if result[0] != quality_check['result']:
                raise ValueError