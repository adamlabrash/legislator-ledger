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

        # writes data to a json file
        task = BashOperator(
            task_id='expenditure_scrape',
            bash_command=scrapy_command,
            dag=dag,
        )

        # TODO upload json file to s3 bucket
        # TODO transform data and insert into supabase & snowflake
        # TODO alert systm

    run_task()
