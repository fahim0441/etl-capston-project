from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import pendulum

local_tz = pendulum.timezone("Asia/Dhaka")

# ================================
# DAG setup
# ================================
default_args = {
    'owner': 'fahim',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 4, 0, 0, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_supermarket_sales',
    default_args=default_args,
    schedule_interval=None,  # Change to '0 5 * * *' to run daily at 5 AM
    catchup=False,
    tags=['etl', 'supermarket', 'postgres'],
    description='ETL pipeline to load supermarket sales into PostgreSQL using Airflow'
) as dag:

    # ================================
    # Task 1: Extract
    # ================================
    def extract_data(**kwargs):
        url = "https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/supermarket_Sales.csv"
        df = pd.read_csv(url)
        df.to_csv('/tmp/supermarket_sales_raw.csv', index=False)

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    # ================================
    # Task 2: Transform
    # ================================
    def transform_data(**kwargs):
        df = pd.read_csv('/tmp/supermarket_sales_raw.csv')

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Drop duplicates and only rows missing required fields
        df.drop_duplicates(inplace=True)
        df.dropna(subset=['invoice_id', 'branch', 'city', 'customer_type', 'gender',
                          'product_line', 'unit_price', 'quantity', 'total',
                          'date', 'time', 'payment'], inplace=True)

        df.to_csv('/tmp/supermarket_sales_cleaned.csv', index=False)

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # ================================
    # Task 3: Load to staging
    # ================================
    def load_clean_data(**kwargs):
        df = pd.read_csv('/tmp/supermarket_sales_cleaned.csv')

        db_user = 'airflow'
        db_pass = 'airflow'
        db_host = 'postgres'
        db_port = '5432'
        db_name = 'airflow'

        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
        df.to_sql('stg_supermarket_sales', engine, if_exists='replace', index=False)

    load_clean_data_task = PythonOperator(
        task_id='load_clean_data',
        python_callable=load_clean_data
    )

    # ================================
    # Task 4: Create dimension & fact tables
    # ================================
    def create_tables(**kwargs):
        db_user = 'airflow'
        db_pass = 'airflow'
        db_host = 'postgres'
        db_port = '5432'
        db_name = 'airflow'

        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

        create_sql = """
        DROP TABLE IF EXISTS fact_sales, dim_customer, dim_product, dim_branch, dim_time CASCADE;

        CREATE TABLE dim_branch (
            branch_id SERIAL PRIMARY KEY,
            branch_name TEXT,
            city TEXT
        );

        CREATE TABLE dim_customer (
            customer_id SERIAL PRIMARY KEY,
            customer_type TEXT,
            gender TEXT
        );

        CREATE TABLE dim_product (
            product_id SERIAL PRIMARY KEY,
            product_line TEXT,
            unit_price NUMERIC
        );

        CREATE TABLE dim_time (
            time_id SERIAL PRIMARY KEY,
            date DATE,
            time TIME
        );

        CREATE TABLE fact_sales (
            sale_id SERIAL PRIMARY KEY,
            invoice_id TEXT,
            branch_id INTEGER REFERENCES dim_branch(branch_id),
            customer_id INTEGER REFERENCES dim_customer(customer_id),
            product_id INTEGER REFERENCES dim_product(product_id),
            time_id INTEGER REFERENCES dim_time(time_id),
            quantity INTEGER,
            total NUMERIC,
            payment TEXT,
            gross_income NUMERIC,
            rating NUMERIC
        );
        """

        with engine.begin() as conn:
            conn.execute(text(create_sql))

    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )

    # ================================
    # Task 5: Load into fact/dim tables
    # ================================
    def load_fact_and_dims(**kwargs):
        df = pd.read_csv('/tmp/supermarket_sales_cleaned.csv')

        db_user = 'airflow'
        db_pass = 'airflow'
        db_host = 'postgres'
        db_port = '5432'
        db_name = 'airflow'

        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dim_branch (branch_name, city)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row['branch'], row['city']))

            cursor.execute("""
                INSERT INTO dim_customer (customer_type, gender)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row['customer_type'], row['gender']))

            cursor.execute("""
                INSERT INTO dim_product (product_line, unit_price)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row['product_line'], row['unit_price']))

            cursor.execute("""
                INSERT INTO dim_time (date, time)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row['date'], row['time']))

        conn.commit()

        for _, row in df.iterrows():
            cursor.execute("SELECT branch_id FROM dim_branch WHERE branch_name = %s AND city = %s",
                           (row['branch'], row['city']))
            branch_id = cursor.fetchone()[0]

            cursor.execute("SELECT customer_id FROM dim_customer WHERE customer_type = %s AND gender = %s",
                           (row['customer_type'], row['gender']))
            customer_id = cursor.fetchone()[0]

            cursor.execute("SELECT product_id FROM dim_product WHERE product_line = %s AND unit_price = %s",
                           (row['product_line'], row['unit_price']))
            product_id = cursor.fetchone()[0]

            cursor.execute("SELECT time_id FROM dim_time WHERE date = %s AND time = %s",
                           (row['date'], row['time']))
            time_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO fact_sales (
                    invoice_id, branch_id, customer_id, product_id, time_id,
                    quantity, total, payment, gross_income, rating
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['invoice_id'], branch_id, customer_id, product_id, time_id,
                row['quantity'], row['total'], row['payment'],
                row.get('gross_income', 0), row.get('rating', 0)
            ))

        conn.commit()
        cursor.close()
        conn.close()

    load_fact_task = PythonOperator(
        task_id='load_fact_dim_tables',
        python_callable=load_fact_and_dims
    )

    # ================================
    # Task chain
    # ================================
    extract_task >> transform_task >> load_clean_data_task >> create_tables_task >> load_fact_task
