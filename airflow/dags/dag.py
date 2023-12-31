from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag

OUTPUT_JSON_FILE = '/data/expenditures.json'


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
        scrapy_command = (
            f"scrapy crawl expenditures -o {OUTPUT_JSON_FILE} -t jsonlines -a execution_date={context['ds']}"
        )

        task = BashOperator(
            task_id='expenditure_scrape',
            bash_command=scrapy_command,
            dag=dag,
        )

    run_task()


if "__main__" == __name__:
    expenditure_dag().test()
