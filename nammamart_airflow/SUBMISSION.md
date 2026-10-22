# NammaMart Quick Commerce — Data Pipeline Submission

## 1. Project Overview

This project implements ETL and ELT data pipelines for NammaMart Quick Commerce using Apache Airflow and DuckDB.

The solution processes order, payment, customer, product, and store data to support business reporting, data-quality monitoring, payment reconciliation, and analytical use cases.

Two Airflow DAGs are implemented:

- `nammamart_etl_venkatesh`
- `nammamart_elt_venkatesh`

## 2. Technology Stack

- Apache Airflow
- Astro CLI
- Python
- DuckDB
- SQL
- CSV source files
- Git

## 3. Source Data

The pipeline uses the following source files:

| File | Purpose |
|---|---|
| `orders.csv` | Historical order data |
| `payments.csv` | Payment and reconciliation data |
| `customers.csv` | Customer information |
| `products.csv` | Product and category information |
| `stores.csv` | Store and location information |
| `orders_2026-10-01_increment.csv` | Incremental order feed containing new and corrected records |

## 4. ETL Pipeline

**DAG:** `nammamart_etl_venkatesh`

### Processing Steps

1. Extract data from the source CSV files.
2. Validate source records before loading.
3. Identify invalid records and capture failure reasons.
4. Mask sensitive customer information, including email addresses and phone numbers.
5. Transform and load validated data into DuckDB.
6. Build the analytical star schema.
7. Process incremental order data.
8. Generate data-quality and business reporting outputs.
9. Record execution status and audit information.

### Target Tables

- `etl_fact_orders`
- `etl_dim_customer`
- `etl_dim_product`
- `etl_dim_store`
- `etl_daily_zone_revenue`
- `etl_category_margin`

**Idempotency:** Describe the implemented strategy for preventing duplicate records when a pipeline is rerun. Include the test result.

**Error handling:** Describe how invalid records, failed tasks, retries, and execution timeouts are handled.

## 5. ELT Pipeline

**DAG:** `nammamart_elt_venkatesh`

### Processing Steps

1. Load source records into raw tables while preserving the original source values.
2. Add ingestion metadata, including `_loaded_at`, `_source_file`, and `_run_id`.
3. Transform raw records using SQL-based staging logic.
4. Standardize data types and values.
5. Apply deduplication, masking, and data-quality rules.
6. Record data-quality status and failure reasons.
7. Build analytical marts.
8. Record pipeline execution and audit information.

### Analytical Marts

- `mart_daily_store_kpis`
- `mart_category_margin`
- `mart_delivery_sla`
- `mart_payment_reconciliation`
- `mart_top_customers`

**Incremental processing:** Explain how new orders are inserted and corrected orders are updated using the order key. Include the observed insert and update counts from your tests.

## 6. Scheduling and Reliability

Both DAGs are configured to run at 9:00 AM IST, Monday through Saturday.

- Cron schedule: `0 9 * * 1-6`
- Timezone: `Asia/Kolkata`
- Catchup: Disabled
- Retries: Document the configured retry count.
- Retry delay: Document the configured delay.
- Execution timeout: Document the configured timeout.
- Failure handling: Document the implemented failure-handling approach.

## 7. Data Quality

The solution addresses the following data-quality dimensions:

1. **Completeness:** Required fields are populated.
2. **Uniqueness:** Duplicate records are identified or handled.
3. **Validity:** Values conform to the expected formats and business rules.
4. **Accuracy:** Values and calculations are checked against the available source data and reconciliation rules.
5. **Consistency:** Related fields and datasets follow consistent business rules.
6. **Timeliness:** Data freshness and processing timestamps are monitored.

Document the actual checks implemented, the rules applied, and the results observed.

The `dq_results` table records data-quality outcomes for pipeline executions.

## 8. Audit and Observability

The solution records pipeline execution information in `pipeline_runs` and data-quality outcomes in `dq_results`.

Document the audit fields implemented, including execution identifiers, timestamps, task or pipeline status, and failure details where available.

### Observability Experiments

Record the results of the 10 required observability experiments.

For each experiment, include:

- Experiment number and name
- Scenario or failure introduced
- Expected behavior
- Actual behavior
- Evidence or screenshot
- Pass/fail result

## 9. Business Questions and Reporting

Document the six business questions required by the assignment.

For each question, provide:

- Business question
- SQL query or reporting logic
- Result obtained from your data
- Brief business interpretation

Include the relevant output tables and query results as evidence.

## 10. ETL vs. ELT Recommendation

Compare ETL and ELT based on the implementation, including:

- Data validation and transformation
- Raw-data retention
- Incremental processing
- Auditability and debugging
- Data-quality handling
- Maintenance and scalability
- Suitability for the NammaMart use case

**Recommendation:** State which approach you recommend for NammaMart, explain why, and support the recommendation with evidence from the implemented pipelines.

## 11. Execution and Validation

Document the commands used to validate and test the pipelines.

Example commands:

```bash
astro dev run dags list-import-errors
astro dev run dags list
```

Example DAG test commands:

```bash
astro dev run dags test nammamart_etl_venkatesh 2026-10-22
astro dev run dags test nammamart_elt_venkatesh 2026-10-22
```

Use a test date appropriate to your environment. DAG tests execute tasks and may modify the local warehouse.

### Validation Results

| Validation | Result |
|---|---|
| DAG import validation | [Pass] |
| ETL execution | [Pass] |
| ELT execution | [Pass] |
| Historical data loading | [Pass] |
| Incremental inserts and updates | [Pass] |
| Idempotency | [Pass] |
| Data-quality checks | [Pass] |
| Audit logging | [Pass] |
| Business reports | [Pass] |
| Observability experiments | [Pass] |

Update this table with actual results. Do not mark tests as passed unless you have verified them.

## 12. Submission Contents

The submission includes:

- ETL DAG
- ELT DAG
- Supporting Python modules
- SQL transformation logic, where applicable
- Required source data files, subject to assignment requirements
- `SUBMISSION.md`
- Required screenshots and evidence

## 13. Known Limitations

Document any incomplete requirements, test failures, environment dependencies, or limitations that remain.

If no known limitations remain, state that only after completing the relevant validation.

## 14. Final Status

- Implementation status: [Complete]
- Testing status: [Complete]
- Documentation status: [Complete]
- Git commit: [Commit hash after committing]