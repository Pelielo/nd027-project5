from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#03b5fc'

    insert_sql = """
        INSERT INTO {}
        {};
    """

    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_query="",
                 target_table="",
                 truncate_before_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_query = load_query
        self.target_table = target_table
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info(f"Truncating Redshift dimension table {self.target_table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.target_table))

        self.log.info(f"Inserting into Redshift dimension table {self.target_table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.target_table,
            self.load_query
        )
        redshift.run(formatted_sql)