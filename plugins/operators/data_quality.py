from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            table = test["sql_query"]
            result = test["expected_result"]
            
            records = redshift.get_records(table)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. `{table}` returned no results")

            rec = records[0][0]

            if isinstance(result, str) and result.startswith(">"):
                threshold = int(result[1:])
                if not rec > threshold:
                    raise ValueError(f"FAILED: `{table}` returned {rec}, expected > {threshold}")
                
            else:
                if rec != result:
                    raise ValueError(f"FAILED: `{table}` returned {rec}, expected {result}")

            self.log.info(f"PASSED: `{table}` returned {rec}")

        self.log.info("All Data Quality checks finished successfully!")