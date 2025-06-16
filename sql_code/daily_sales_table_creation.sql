CREATE OR REPLACE TABLE `airflow-dataproc-project.product_reports.daily_sales` (

  transaction_date DATE,
  order_country STRING,
  product_id STRING,
  total_units_sold INT64,
  daily_total_paid FLOAT64,
  daily_total_discount_amount FLOAT64 ,
  daily_gross_revenue FLOAT64
)
PARTITION BY transaction_date
CLUSTER BY order_country ;

-- bq --project_id airflow-dataproc-project --location US query --use_legacy_sql=false < /Users/delgadonoriega/Desktop/gcp-data-eng-bootcamp/Module_3_class_2/airflow_project_1/sql_code/daily_sales_table_creation.sql

