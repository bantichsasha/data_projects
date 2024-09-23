from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from datetime import datetime, timedelta
import pytz
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException


INPUT_DIR = '/opt/input/'
DAILY_AGGS_DIR = '/opt/daily_aggs/'
OUTPUT_DIR = '/opt/output/'

TODAY = datetime.now().astimezone(pytz.timezone('Europe/Moscow'))
YESTERDAY = TODAY - timedelta(days=1)

LAST_WEEK_FILES = [
    (YESTERDAY - timedelta(days=i)).strftime('%Y-%m-%d') + '.csv'
    for i in range(7)
    ]


def check_files():
    """
    Checks for the existence of required input and aggregate files.

    Raises:
        AirflowSkipException: If any input file is missing.

    Returns:
        list: A list of missing aggregates.
    """
    missing_inputs = []
    missing_aggs = []

    for file in LAST_WEEK_FILES:
        if not os.path.isfile(INPUT_DIR + file):
            missing_inputs.append(file)
        if not os.path.isfile(DAILY_AGGS_DIR + file):
            missing_aggs.append(file)

    if missing_inputs:
        raise AirflowSkipException(f'Missing input files: {missing_inputs}')

    return missing_aggs


def calculate_aggregates(**kwargs):
    """
    Calculates daily and weekly aggregates from input CSV files.

    Args:
        kwargs (dict): Context variables, including task instance.

    Returns:
        None: Writes output directly to the file system.
    """
    missing_aggs = kwargs['ti'].xcom_pull(task_ids='check_files')

    with SparkSession.builder.appName('Aggregation').getOrCreate() as spark:

        if missing_aggs:
            valid_actions = ['CREATE', 'READ', 'UPDATE', 'DELETE']

            for file in missing_aggs:
                # Reading CSV without headers as synthetic data lacks them
                df = spark.read.csv(INPUT_DIR + file, header=False, inferSchema=True)

                clean_df = df.toDF('email', 'action', 'dt').na.drop().dropDuplicates()

                validated_df = clean_df.filter(col('action').isin(valid_actions))

                grouped_df = validated_df.groupBy('email', 'action').count()

                grouped_df.write.csv(
                    DAILY_AGGS_DIR + file,
                    mode='overwrite',
                    header=True,
                    )

        last_week_aggs = [DAILY_AGGS_DIR + file for file in LAST_WEEK_FILES]
        combined_df = spark.read.csv(last_week_aggs, header=True, inferSchema=True)

        result_df = combined_df.groupBy('email').pivot('action').agg(sum('count'))
        result_df = result_df.fillna(0)

        rename_dict = {
            'CREATE': 'create_count',
            'READ': 'read_count',
            'UPDATE': 'update_count',
            'DELETE': 'delete_count'
            }
        for old_name, new_name in rename_dict.items():
            result_df = result_df.withColumnRenamed(old_name, new_name)

        result_df.write.csv(
            OUTPUT_DIR + TODAY.strftime('%Y-%m-%d') + '.csv',
            mode='overwrite',
            header=True,
            )


def delete_extra_files():
    """
    Deletes CSV files from the daily aggregates directory that are older than
    a week.

    Returns:
        None: Cleans up unnecessary files to save storage space.
    """
    for file in os.listdir(DAILY_AGGS_DIR):
        if file.endswith('.csv') and file not in LAST_WEEK_FILES:
            os.remove(f'{DAILY_AGGS_DIR}{file}')


dag = DAG(
    dag_id='user_cruds_weekly',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 9, 24),
    catchup=False,
    )

check_files_task = PythonOperator(
    task_id='check_files',
    python_callable=check_files,
    dag=dag,
    )

calculate_aggregates_task = PythonOperator(
    task_id='calculate_aggregates',
    python_callable=calculate_aggregates,
    provide_context=True,
    dag=dag,
    )

delete_extra_files_task = PythonOperator(
    task_id='delete_extra_files',
    python_callable=delete_extra_files,
    dag=dag,
    )

check_files_task >> calculate_aggregates_task >> delete_extra_files_task
