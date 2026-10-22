from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os
import pendulum

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='nammamart_elt_venkatesh',
    default_args=default_args,
    description='Automated daily ELT Pipeline capturing raw elements prior to running analytic logic',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=['nammamart', 'elt']
)
def nammamart_elt_venkatesh():

    @task()
    def elt_extract_and_load_raw():
        """Extracts text elements directly into isolated raw relational staging tables."""
        from nammamart_helpers import run_elt_extract_load, run_audit_log
        try:
            run_elt_extract_load(ORDERS_BASE, PAYMENTS_BASE, CUSTOMERS_BASE, PRODUCTS_BASE, STORES_BASE, is_incremental=False)
            run_audit_log('nammamart_elt_venkatesh', 'extract_base', 'ELT_EL', 'SUCCESS')
        except Exception as e:
            run_audit_log('nammamart_elt_venkatesh', 'extract_base', 'ELT_EL', f'FAILED: {str(e)}')
            raise

    @task()
    def elt_extract_and_load_increment():
        """Appends next-day raw records to the staging layer."""
        from nammamart_helpers import run_elt_extract_load, run_audit_log
        try:
            run_elt_extract_load(INCREMENT_FILE, PAYMENTS_BASE, CUSTOMERS_BASE, PRODUCTS_BASE, STORES_BASE, is_incremental=True)
            run_audit_log('nammamart_elt_venkatesh', 'extract_increment', 'ELT_EL', 'SUCCESS')
        except Exception as e:
            run_audit_log('nammamart_elt_venkatesh', 'extract_increment', 'ELT_EL', f'FAILED: {str(e)}')
            raise

    @task()
    def elt_transform_analytical_tier():
        """Applies SQL models to build clean downstream gold tables."""
        from nammamart_helpers import run_elt_transformations, run_audit_log
        try:
            run_elt_transformations()
            run_audit_log('nammamart_elt_venkatesh', 'transform_tier', 'ELT_T', 'SUCCESS')
        except Exception as e:
            run_audit_log('nammamart_elt_venkatesh', 'transform_tier', 'ELT_T', f'FAILED: {str(e)}')
            raise

    elt_extract_and_load_raw() >> elt_extract_and_load_increment() >> elt_transform_analytical_tier()

nammamart_elt_dag = nammamart_elt_venkatesh()