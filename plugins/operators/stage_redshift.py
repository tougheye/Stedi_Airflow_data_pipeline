from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


'''Operator loads any JSON formatted files from S3 to Amazon Redshift.
   
    redshift_conn_id (string):  Airflow conn_id of Redshift connection
    aws_credentials_id (string): Airflow conn_id of the aws_credentials granting access to s3 (Default: 'aws_credentials')  
    table (string):  The name of the Amazon Redshift table where the data should be loaded
    s3_path (string): The data source where to get data from          
    
    '''
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{json_path}'
        REGION 'us-east-1';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_path = "",
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_path = s3_path
        self.json_path = json_path

    def execute(self, context):
        self.log.info("Getting AWS credentials")
        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Render s3_key with context (templating)
        rendered_key = self.s3_key.format(**context).strip()
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        
        # self.log.info(f"Sanitized key: '{rendered_key}'")
        # self.log.info(f"Final s3_path: '{s3_path}'")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json_path=self.json_path
        )

        redshift.run(formatted_sql)
        self.log.info(f"Stage {self.table} complete")



