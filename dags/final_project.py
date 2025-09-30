from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable 
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2025, 9, 1, tz="UTC"),  # fixed start date
}

from airflow.models import Variable

s3_bucket = Variable.get("s3_bucket")
log_data_key = Variable.get("log_data_key")
song_data_key = Variable.get("song_data_key")


dag = DAG(
    'Data_Pipeline_final_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events", 
        s3_bucket=s3_bucket,
        s3_key=log_data_key,
        json_path=f's3://{s3_bucket}/log_json_path.json', dag=dag
    )

stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs", dag=dag,
        s3_bucket=s3_bucket,
        s3_key=song_data_key,
        json_path= 'auto'
    )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)    

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> end_operator





"""
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )
"""