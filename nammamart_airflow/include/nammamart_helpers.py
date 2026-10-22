import os
import pandas as pd
import duckdb
import hashlib

DB_PATH = os.path.join(os.path.dirname(__file__), "warehouse_nammamart.duckdb")

def get_db_connection():
    """Returns a thread-safe connection to your specific DuckDB instance."""
    return duckdb.connect(DB_PATH)

def mask_pii(val):
    """Masks PII elements like phone numbers and emails using deterministic SHA-256."""
    if pd.isna(val) or str(val).strip() == "":
        return "MASKED"
    return hashlib.sha256(str(val).strip().encode('utf-8')).hexdigest()[:16]

# ==========================================
# ETL PIPELINE HELPER FUNCTIONS
# ==========================================

def run_etl_pipeline(orders_path, payments_path, customers_path, products_path, stores_path, is_incremental=False):
    """
    Executes the ETL Pipeline:
    1. Loads and cleans dimensions (masks PII).
    2. Validates facts against business rules and referential integrity.
    3. Quarantines invalid records with a clear error reason.
    4. Reconciles transactions.
    5. Writes clean data using an Upsert strategy to enforce Idempotency.
    """
    conn = get_db_connection()
    
    # 1. Load Dimensions & Mask PII
    df_cust = pd.read_csv(customers_path)
    df_cust['email'] = df_cust['email'].apply(mask_pii)
    df_cust['phone'] = df_cust['phone'].apply(mask_pii)
    
    df_prod = pd.read_csv(products_path)
    df_store = pd.read_csv(stores_path)
    
    # 2. Load Facts
    df_ord = pd.read_csv(orders_path)
    df_pay = pd.read_csv(payments_path)
    
    # Trackers for Quarantine Rows
    quarantine_records = []
    clean_orders = []
    
    # Valid values sets for fast lookups
    valid_statuses = {'DELIVERED', 'CANCELLED', 'RETURNED'}
    valid_modes = {'UPI', 'Card', 'Cash on Delivery', 'Wallet'}
    valid_cust_ids = set(df_cust['customer_id'])
    valid_prod_ids = set(df_prod['product_id'])
    valid_store_ids = set(df_store['store_id'])
    
    # 3. Row-by-Row ETL Validation Engine
    for idx, row in df_ord.iterrows():
        order_id = row['order_id']
        reason = None
        
        # Rule Validation
        if row['order_status'] not in valid_statuses:
            reason = f"Invalid order_status: {row['order_status']}"
        elif row['payment_mode'] not in valid_modes:
            reason = f"Invalid payment_mode: {row['payment_mode']}"
        elif row['order_status'] == 'DELIVERED' and (pd.isna(row['delivery_minutes']) or row['delivery_minutes'] < 0):
            reason = "DELIVERED order missing or invalid delivery_minutes"
        # Math verification: check if calculated amount matches order_amount within small tolerance
        elif not pd.isna(row['quantity']) and not pd.isna(row['unit_price']):
            expected_amt = round(row['quantity'] * row['unit_price'] * (1 - (row['discount_pct'] / 100.0)), 2)
            if abs(row['order_amount'] - expected_amt) > 0.02:
                reason = f"Math Mismatch: Expected {expected_amt}, Got {row['order_amount']}"
        # Referential Integrity
        elif row['customer_id'] not in valid_cust_ids:
            reason = f"Referential Integrity: customer_id {row['customer_id']} not found in CRM"
        elif row['product_id'] not in valid_prod_ids:
            reason = f"Referential Integrity: product_id {row['product_id']} not found in Catalog"
        elif row['store_id'] not in valid_store_ids:
            reason = f"Referential Integrity: store_id {row['store_id']} not found in Store Master"
            
        if reason:
            quarantine_records.append({
                'file_source': 'orders_incremental.csv' if is_incremental else 'orders.csv',
                'record_key': str(order_id),
                'failure_reason': reason,
                'quality_dimension': 'Validity/Integrity' if 'Referential' not in reason else 'Consistency'
            })
        else:
            clean_orders.append(row)
            
    df_clean_ord = pd.DataFrame(clean_orders) if clean_orders else pd.DataFrame(columns=df_ord.columns)
    
    # 4. Payments Validation & Cross-Reconciliation
    clean_payments = []
    reconciliation_records = []
    
    # Create an order lookup for payments reconciliation
    all_orders_lookup = pd.concat([df_ord], ignore_index=True).set_index('order_id').to_dict('index')
    
    for idx, row in df_pay.iterrows():
        pay_id = row['payment_id']
        ord_id = row['order_id']
        reason = None
        
        if row['payment_status'] == 'SUCCESS' and ord_id in all_orders_lookup:
            ord_info = all_orders_lookup[ord_id]
            if abs(row['amount_paid'] - ord_info['order_amount']) > 0.02:
                reason = "Payment amount does not match Order amount"
                reconciliation_records.append({
                    'order_id': ord_id,
                    'payment_id': pay_id,
                    'order_amount': ord_info['order_amount'],
                    'amount_paid': row['amount_paid'],
                    'reconciliation_status': 'AMOUNT_MISMATCH'
                })
        elif row['payment_status'] == 'SUCCESS' and ord_id not in all_orders_lookup:
            reason = "Orphan Payment: Order ID missing from source tracking"
            reconciliation_records.append({
                'order_id': ord_id,
                'payment_id': pay_id,
                'order_amount': 0.0,
                'amount_paid': row['amount_paid'],
                'reconciliation_status': 'ORPHAN_PAYMENT'
            })
            
        if reason:
            quarantine_records.append({
                'file_source': 'payments.csv',
                'record_key': str(pay_id),
                'failure_reason': reason,
                'quality_dimension': 'Accuracy'
            })
        else:
            clean_payments.append(row)
            
    df_clean_pay = pd.DataFrame(clean_payments) if clean_payments else pd.DataFrame(columns=df_pay.columns)
    df_quarantine = pd.DataFrame(quarantine_records)
    df_recon = pd.DataFrame(reconciliation_records)
    
    # 5. Database Writing Strategy (Idempotent Merges)
    conn.execute("CREATE TABLE IF NOT EXISTS etl_dim_customers AS SELECT * FROM df_cust WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_dim_products AS SELECT * FROM df_prod WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_dim_stores AS SELECT * FROM df_store WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM df_clean_ord WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_fact_payments AS SELECT * FROM df_clean_pay WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_quarantine AS SELECT * FROM df_quarantine WHERE 1=0")
    conn.execute("CREATE TABLE IF NOT EXISTS etl_reconciliation AS SELECT * FROM df_recon WHERE 1=0")
    
    # Simple, high-performance upsert logic for dimensions/facts
    if not is_incremental:
        conn.execute("TRUNCATE etl_dim_customers; INSERT INTO etl_dim_customers SELECT * FROM df_cust")
        conn.execute("TRUNCATE etl_dim_products; INSERT INTO etl_dim_products SELECT * FROM df_prod")
        conn.execute("TRUNCATE etl_dim_stores; INSERT INTO etl_dim_stores SELECT * FROM df_store")
        conn.execute("TRUNCATE etl_fact_orders; INSERT INTO etl_fact_orders SELECT * FROM df_clean_ord")
        conn.execute("TRUNCATE etl_fact_payments; INSERT INTO etl_fact_payments SELECT * FROM df_clean_pay")
        conn.execute("TRUNCATE etl_quarantine; INSERT INTO etl_quarantine SELECT * FROM df_quarantine")
        conn.execute("TRUNCATE etl_reconciliation; INSERT INTO etl_reconciliation SELECT * FROM df_recon")
    else:
        # Incremental logic: delete existing keys first to avoid primary key/row inflation on re-runs
        if not df_clean_ord.empty:
            order_ids_str = ",".join([f"'{x}'" for x in df_clean_ord['order_id'].tolist()])
            conn.execute(f"DELETE FROM etl_fact_orders WHERE order_id IN ({order_ids_str})")
            conn.execute("INSERT INTO etl_fact_orders SELECT * FROM df_clean_ord")
        if not df_quarantine.empty:
            conn.execute("INSERT INTO etl_quarantine SELECT * FROM df_quarantine")
        if not df_recon.empty:
            conn.execute("INSERT INTO etl_reconciliation SELECT * FROM df_recon")
            
    conn.close()

# ==========================================
# ELT PIPELINE HELPER FUNCTIONS
# ==========================================

def run_elt_extract_load(orders_path, payments_path, customers_path, products_path, stores_path, is_incremental=False):
    """
    Executes ELT Extract & Load:
    Dumps source data entirely untouched into staging tables inside DuckDB.
    """
    conn = get_db_connection()
    
    df_ord = pd.read_csv(orders_path)
    df_pay = pd.read_csv(payments_path)
    df_cust = pd.read_csv(customers_path)
    df_prod = pd.read_csv(products_path)
    df_store = pd.read_csv(stores_path)
    
    if not is_incremental:
        conn.execute("CREATE OR REPLACE TABLE elt_raw_orders AS SELECT * FROM df_ord")
        conn.execute("CREATE OR REPLACE TABLE elt_raw_payments AS SELECT * FROM df_pay")
        conn.execute("CREATE OR REPLACE TABLE elt_raw_customers AS SELECT * FROM df_cust")
        conn.execute("CREATE OR REPLACE TABLE elt_raw_products AS SELECT * FROM df_prod")
        conn.execute("CREATE OR REPLACE TABLE elt_raw_stores AS SELECT * FROM df_store")
    else:
        # Appends raw updates incrementally
        conn.execute("INSERT INTO elt_raw_orders SELECT * FROM df_ord")
        
    conn.close()

def run_elt_transformations():
    """
    Executes ELT Transform Phase:
    Runs analytical models via SQL queries on raw staging structures.
    Uses Common Table Expressions (CTEs) to mask PII, validate metrics,
    and generate final analytical gold models.
    """
    conn = get_db_connection()
    
    # 1. Cleaned Dimensions View/Table with PII Masking via SHA256 hashing natively in SQL
    conn.execute("""CREATE OR REPLACE TABLE elt_analytics_customers AS
        SELECT
        customer_id,
        city, area, pincode, signup_date, loyalty_tier, is_active,
        SUBSTR(SHA256(CAST(email AS VARCHAR)), 1, 16) as email,
        SUBSTR(SHA256(CAST(phone AS VARCHAR)), 1, 16) as phone
        FROM elt_raw_customers;
        """)
    # 2. Final Analytics Model for Orders (Resolving updates via Row-Number sequencing over timestamp hashes)
    conn.execute("""
        CREATE OR REPLACE TABLE elt_analytics_orders AS
        WITH ranked_orders AS (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_ts DESC) as rn
        FROM elt_raw_orders
        )
        SELECT
        order_id, order_ts, customer_id, store_id, product_id, quantity,
        unit_price, discount_pct, order_amount, payment_mode, order_status,
        channel, delivery_minutes
        FROM ranked_orders
        WHERE rn = 1;
        """)
    # 3. Reconciliation Matrix Built purely in SQL
    conn.execute("""
        CREATE OR REPLACE TABLE elt_reconciliation AS
        SELECT
        o.order_id,
        p.payment_id,
        o.order_amount,
        p.amount_paid,
        CASE
        WHEN p.payment_id IS NULL AND o.order_status = 'DELIVERED' THEN 'MISSING_PAYMENT'
        WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT'
        WHEN ABS(o.order_amount - p.amount_paid) > 0.02 THEN 'AMOUNT_MISMATCH'
        ELSE 'RECONCILED'
        END as reconciliation_status
        FROM elt_analytics_orders o
        FULL OUTER JOIN elt_raw_payments p ON o.order_id = p.order_id
        WHERE o.order_status = 'DELIVERED' OR p.payment_status = 'SUCCESS';
        """)
    conn.close()

def run_audit_log(dag_id, run_id, pipeline_type, status):
    """Logs system metrics to a unified audit repository."""
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_audit_log (
        log_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dag_id VARCHAR,
        run_id VARCHAR,
        pipeline_type VARCHAR,
        status VARCHAR
        )
        """)
    conn.execute("INSERT INTO pipeline_audit_log (dag_id, run_id, pipeline_type, status) VALUES (?, ?, ?, ?)",[dag_id, run_id, pipeline_type, status])
    conn.close()