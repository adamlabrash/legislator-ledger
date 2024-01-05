from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag


@dag(
    dag_id='expenditure_pipeline',
    default_args={
        'owner': 'legislator-ledger',
        'depends_on_past': False,
        'start_date': datetime(2020, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='0 0 1 */4 *',  # Run every 4 months
    description='Extract quarterly expenditures from the House of Commons website',
    catchup=True,
)
def expenditure_dag():
    @task()
    def run_task(**context):
        scrapy_command = f"scrapy crawl expenditures -a execution_date={context['ds']}"

        # runs scrape and saves to s3 bucket
        scrapy_to_aws_s3 = BashOperator(
            task_id='expenditure_scrape',
            bash_command=scrapy_command,
            dag=dag,
        )

        s3_to_local_json = BashOperator(
            task_id='expenditure_transform',
            bash_command='python3 /transform/get_expenditures_json_s3.py',
            description='Gets the unstructured expenditures.json from s3',
            dag=dag,
        )

        build_locations_csv_from_travel_expenditures = BashOperator(
            task_id='expenditure_transform',
            bash_command='python3 /transform/build_locations_csv.py',
            description='Creates locations.csv data from the travel csv data',
            dag=dag,
        )

        travel_csvs_from_json = BashOperator(
            task_id='expenditure_transform',
            bash_command='python3 /transform/s3_to_csv.py',
            description='build travel_events and travel_claims csv files',
            dag=dag,
        )

        pyspark_carbon_task = BashOperator(
            task_id='expenditure_transform',
            bash_command='python3 /transform/carbon_analysis.py',
            description='Runs carbon emissions analysis on the travel_events.csv and location.csv data',
            dag=dag,
        )

    run_task()
