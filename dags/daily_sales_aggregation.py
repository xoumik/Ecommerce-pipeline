from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default settings for our tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'daily_sales_aggregation',
    default_args=default_args,
    description='Aggregates real-time e-commerce orders into daily batch summaries',
    schedule_interval='@daily', # Runs once every midnight
    catchup=False,
) as dag:

    # Task 1: Ensure the summary table exists
    create_summary_table = PostgresOperator(
        task_id='create_summary_table',
        postgres_conn_id='postgres_ecommerce',
        sql="""
        CREATE TABLE IF NOT EXISTS daily_sales_summary (
            aggregation_date DATE,
            product VARCHAR(50),
            total_sales DECIMAL(10, 2),
            total_orders INT,
            PRIMARY KEY (aggregation_date, product)
        );
        """
    )

    # Task 2: Calculate the totals and insert them into the summary table
    # We use an UPSERT (ON CONFLICT) so if we run this twice in one day, it updates the row instead of crashing
    aggregate_sales = PostgresOperator(
        task_id='aggregate_daily_sales',
        postgres_conn_id='postgres_ecommerce',
        sql="""
        INSERT INTO daily_sales_summary (aggregation_date, product, total_sales, total_orders)
        SELECT 
            CURRENT_DATE, 
            product, 
            SUM(price), 
            COUNT(order_id) 
        FROM orders 
        GROUP BY product
        ON CONFLICT (aggregation_date, product) 
        DO UPDATE SET 
            total_sales = EXCLUDED.total_sales,
            total_orders = EXCLUDED.total_orders;
        """
    )

    # Define the order of operations
    create_summary_table >> aggregate_sales