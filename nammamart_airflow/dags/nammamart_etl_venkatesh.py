#### 1. ETL Pipeline DAG (`dags/nammamart_etl_venkatesh.py`)

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os
import pendulum

# Define environment targets
DATA_DIR = "/usr/local/airflow/include/data/nammamart"
ORDERS_BASE = os.path.join(DATA_DIR, "orders.csv")
PAYMENTS_BASE = os.path.join(DATA_DIR, "payments.csv")
CUSTOMERS_BASE = os.path.join(DATA_DIR, "customers.csv")
PRODUCTS_BASE = os.path.join(DATA_DIR, "products.csv")
STORES_BASE = os.path.join(DATA_DIR, "stores.csv")
INCREMENT_FILE = os.path.join(DATA_DIR, "orders_2026-10-01_increment.csv")

default_args = {
    'owner': 'FDE_Team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='nammamart_etl_venkatesh',
    default_args=default_args,
    description='Idempotent Enterprise ETL Star Schema Pipeline running daily at 9:00 AM IST',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=['nammamart', 'etl']
)

def nammamart_etl_venkatesh():

    @task()
    def etl_load_historical_base():
        """Validates, cleans, and seeds primary base records into warehouse structures."""
        from nammamart_helpers import run_etl_pipeline, run_audit_log
        try:
            run_etl_pipeline(ORDERS_BASE, PAYMENTS_BASE, CUSTOMERS_BASE, PRODUCTS_BASE, STORES_BASE, is_incremental=False)
            run_audit_log('nammamart_etl_venkatesh', 'base_run', 'ETL', 'SUCCESS')
        except Exception as e:
            run_audit_log('nammamart_etl_venkatesh', 'base_run', 'ETL', f'FAILED: {str(e)}')
            raise

    @task()
    def etl_process_daily_increment():
        """Processes next-day files incrementally without creating duplicate items."""
        from nammamart_helpers import run_etl_pipeline, run_audit_log
        try:
            run_etl_pipeline(INCREMENT_FILE, PAYMENTS_BASE, CUSTOMERS_BASE, PRODUCTS_BASE, STORES_BASE, is_incremental=True)
            run_audit_log('nammamart_etl_venkatesh', 'incremental_run', 'ETL', 'SUCCESS')
        except Exception as e:
            run_audit_log('nammamart_etl_venkatesh', 'incremental_run', 'ETL', f'FAILED: {str(e)}')
            raise

    etl_load_historical_base() >> etl_process_daily_increment()

nammamart_etl_dag = nammamart_etl_venkatesh()