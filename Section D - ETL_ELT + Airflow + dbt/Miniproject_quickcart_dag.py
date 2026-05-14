from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import pandas as pd
import os

TABLES = ["customers", "orders", "order_items", "products", "sessions", "events"]
SOURCE_DIR = "/opt/airflow/data/raw" 


RAW_ORDERS = "/opt/airflow/data/raw/orders.csv"
RAW_CUSTOMERS = "/opt/airflow/data/raw/customers.csv"
RAW_ORDER_ITEMS = "/opt/airflow/data/raw/order_items.csv"
RAW_PRODUCTS = "/opt/airflow/data/raw/products.csv"
RAW_EVENTS = "/opt/airflow/data/raw/events.csv"
RAW_SESSIONS = "/opt/airflow/data/raw/sessions.csv"


PROCESSED_ORDERS_PARQUET = "/opt/airflow/data/processed/cleaned_orders.parquet"
PROCESSED_CUSTOMERS_PARQUET = "/opt/airflow/data/processed/cleaned_customers.parquet"
PROCESSED_ORDER_ITEMS_PARQUET = "/opt/airflow/data/processed/cleaned_order_items.parquet"
PROCESSED_PRODUCTS_PARQUET = "/opt/airflow/data/processed/cleaned_products.parquet"
PROCESSED_EVENTS_PARQUET = "/opt/airflow/data/processed/cleaned_events.parquet"
PROCESSED_SESSIONS_PARQUET = "/opt/airflow/data/processed/cleaned_sessions.parquet"


DAILY_SALES_KPI = "/opt/airflow/data/reports/daily_sales_kpi.parquet"
CUSTOMER_LIFETIME_VALUE = "/opt/airflow/data/reports/customer_lifetime_value.parquet"
CATEGORY_WISE_REVENUE = "/opt/airflow/data/reports/category_wise_revenue.parquet"
MOST_VISITED = "/opt/airflow/data/reports/most_visited.parquet"
DEVICE_TRAFFIC = "/opt/airflow/data/reports/device_traffic.parquet"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': 'crackrojo89@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,           
    'execution_timeout': timedelta(hours=1),
}


def extraction_validation():

    logging.info("Starting data extraction...")
    df_customers = pd.read_csv(RAW_CUSTOMERS)
    df_orders = pd.read_csv(RAW_ORDERS)
    df_products = pd.read_csv(RAW_PRODUCTS)
    df_order_items = pd.read_csv(RAW_ORDER_ITEMS)
    df_events = pd.read_csv(RAW_EVENTS)
    df_sessions = pd.read_csv(RAW_SESSIONS)
    logging.info("Extraction completed.")

    logging.info("Starting data validation...")
    logging.info("Starting schema validation...")
    expected_customers = {
        "customer_id": "int64",
        "name": "object",
        "email": "object",
        "country": "object",
        "age": "int64",
        "signup_date": "object",
        "marketing_opt_in": "bool"
    }
    for col, dtype in expected_customers.items():
        if col not in df_customers.columns:
            raise ValueError(f"Missing columns in customers dataset: {col}.")
        elif df_customers[col].dtype != dtype:
            raise ValueError(f" Column: {col} data type is {df_customers[col].dtype} but it should be {dtype}.")
    logging.info("Customers schema validation completed.")

    expected_orders = {
        "order_id": "int64",
        "customer_id": "int64",
        "order_time": "object", 
        "payment_method": "object",
        "discount_pct": "int64", 
        "subtotal_usd": "float64",
        "total_usd": "float64",
        "country": "object", 
        "device": "object",
        "source": "object"
    }

    for col, dtype in expected_orders.items():
        if col not in df_orders.columns:
            raise ValueError(f"Missing columns in orders dataset: {col}.")
        elif df_orders[col].dtype != dtype:
            raise ValueError(f" Column: {col} data type is {df_orders[col].dtype} but it should be {dtype}.")
    logging.info("Orders schema validation completed.")



    logging.info("Checking duplicated Customer IDs in progress...")
    duplicated_ids = df_customers[
        df_customers.duplicated(subset=["customer_id"], keep=False)
    ]
    if not duplicated_ids.empty:
        print(duplicated_ids)
        raise ValueError("There are duplicated Customer IDs.")
    logging.info("Duplicated Customer IDs check completed.")
    

    logging.info("Checking duplicated Emails in progress...")
    duplicated_emails = df_customers[
        df_customers.duplicated(subset=["email"], keep=False)
    ]
    if not duplicated_emails.empty:
        print(duplicated_emails)
        raise ValueError("There are duplicated Customers by Email.")
        
    logging.info("Duplicated Emails check completed.")


    logging.info("Checking for invalid orders in progress...")
    invalid_orders = df_orders[(df_orders["subtotal_usd"] <= 0) | (df_orders["total_usd"] <= 0)]
    if not invalid_orders.empty:
        print(invalid_orders)
        raise ValueError("There are invalid Orders with negative amount")
        

    orders_without_customer = df_orders[df_orders["customer_id"].isnull()]
    if not orders_without_customer.empty:
        print(orders_without_customer) 
        raise ValueError("There are orders without customers")
           
    logging.info("Invalid orders check completed.")
    
    
    logging.info("Checking for invalid customers in progress...")
    invalid_customers = df_customers[(df_customers["name"].isnull()) | (df_customers["email"].isnull())]
    if not invalid_customers.empty:
        print(invalid_customers)
        raise ValueError("There are invalid customers with either no name or no email")
    
    logging.info("Invalid customers check completed.")



def cleaning():
    logging.info("Data Cleaning in progres...")
    df_customers = pd.read_csv(RAW_CUSTOMERS)
    df_orders = pd.read_csv(RAW_ORDERS)
    df_products = pd.read_csv(RAW_PRODUCTS)
    df_order_items = pd.read_csv(RAW_ORDER_ITEMS)
    df_events = pd.read_csv(RAW_EVENTS)
    df_sessions = pd.read_csv(RAW_SESSIONS)
    
    # Clean country values
    df_customers["country"] = (df_customers["country"].str.strip().str.upper())

    # Convert datetime columns
    df_orders["order_time"] = pd.to_datetime(df_orders["order_time"],errors="coerce")
    df_events["timestamp"] = pd.to_datetime(df_events["timestamp"],errors="coerce")

    # Remove invalid rows
    df_orders.dropna(subset=["order_id", "total_usd"],inplace=True)
    df_customers.dropna(subset=["name"],inplace=True)
    df_products.dropna(subset=["name"],inplace=True)
    df_events.dropna(subset=["event_type"],inplace=True)
    df_sessions.dropna(subset=["session_id"],inplace=True)

    # Fill missing values
    df_orders["customer_id"] = df_orders["customer_id"].fillna(0)
    df_customers["email"] = df_customers["email"].fillna("Unknown")
    df_products["category"] = df_products["category"].fillna("Unknown")
    df_events["product_id"] = df_events["product_id"].fillna(0)

    # Remove duplicates
    df_orders.drop_duplicates(inplace=True)
    df_customers.drop_duplicates(inplace=True)
    df_products.drop_duplicates(inplace=True)
    df_events.drop_duplicates(inplace=True)
    df_order_items.drop_duplicates(inplace=True)
    df_sessions.drop_duplicates(inplace=True)

    logging.info("Data Cleaning completed.")

    logging.info("Creation of clean files in silver layer in progress...")
    df_customers.to_parquet(PROCESSED_CUSTOMERS_PARQUET,index=False)
    df_orders.to_parquet(PROCESSED_ORDERS_PARQUET,index=False)
    df_products.to_parquet(PROCESSED_PRODUCTS_PARQUET,index=False)
    df_events.to_parquet(PROCESSED_EVENTS_PARQUET,index=False)
    df_order_items.to_parquet(PROCESSED_ORDER_ITEMS_PARQUET,index=False)
    df_sessions.to_parquet(PROCESSED_SESSIONS_PARQUET,index=False)

    logging.info("Creation of clean files in silver layer completed.")


def aggregations():
    
    logging.info("Transformations and aggregations in progres...")
    #Read clean files
    df_clean_orders = pd.read_parquet(PROCESSED_ORDERS_PARQUET)
    df_clean_products = pd.read_parquet(PROCESSED_PRODUCTS_PARQUET)
    df_clean_order_items = pd.read_parquet(PROCESSED_ORDER_ITEMS_PARQUET)
    df_clean_events = pd.read_parquet(PROCESSED_EVENTS_PARQUET)
    df_clean_sessions = pd.read_parquet(PROCESSED_SESSIONS_PARQUET)
    
    #Daily sales KPI 
    df_clean_orders["date"] =df_clean_orders["order_time"].dt.date
    daily_sales_kpi = df_clean_orders.groupby("date").agg(
        daily_sales = ("total_usd", "sum")).reset_index().sort_values("date")
    
    #Customer lifetime value
    customer_lifetime_value = df_clean_orders.groupby("customer_id").agg(
        sales_costumer = ("total_usd", "sum")).reset_index()

    #Category-wise revenue
    orders_products = df_clean_orders.merge(df_clean_order_items,
        on="order_id",
        how="left").merge(df_clean_products,
            on="product_id",
            how="left")
    category_wise_revenue = orders_products.groupby("category").agg(
        category_revenue = ("total_usd", "sum")).reset_index()
    
    #Repeat customer percentage
    customer_orders = df_clean_orders.groupby("customer_id").agg(
        total_orders = ("order_id", "count")).reset_index()
    repeat_customers = customer_orders[customer_orders["total_orders"] > 1]
    repeat_customer_percentage = (len(repeat_customers)/len(customer_orders)) * 100                 
    logging.info(f"Repeat Customer Percentage: {repeat_customer_percentage}%")


    #Find most visited pages.
    page_views = df_clean_events[df_clean_events["event_type"] == "page_view"]
    product_visited = page_views.groupby("product_id").agg(
        views = ("event_id", "count")).reset_index().sort_values("views", ascending=False)
    most_visited = product_visited.head(10)
   

    #Calculate session counts.
    total_sessions = df_clean_events["session_id"].nunique()
    logging.info(f"Total Sessions:, {total_sessions}")
    
    #Find bounce rate.
    session_counts = df_clean_events.groupby("session_id").agg(
    event_count = ("event_id", "count")).reset_index()
    bounced = session_counts[session_counts["event_count"] == 1]
    bounce_rate = (len(bounced) / total_sessions) * 100
    logging.info(f"Bounce Rate is: {bounce_rate}%")
    
    #Find mobile vs desktop traffic percentage.
    
    events_sessions = session_counts.merge(df_clean_sessions,
        on="session_id",
        how="left")
    total_traffic = events_sessions["event_count"].sum()
    device_traffic = events_sessions.groupby('device').agg(
        device_sessions =("event_count", "sum")).reset_index()
    device_traffic["traffic %"] = 100 * device_traffic["device_sessions"] / total_traffic

    logging.info("Transformations and aggregations completed.")
   
    logging.info("Extraction of aggregated files in progress...")
    # Export aggregated files      
    daily_sales_kpi.to_parquet(DAILY_SALES_KPI,index=False)
    customer_lifetime_value.to_parquet(CUSTOMER_LIFETIME_VALUE,index=False)
    category_wise_revenue.to_parquet(CATEGORY_WISE_REVENUE,index=False)
    most_visited.to_parquet(MOST_VISITED,index=False)
    device_traffic.to_parquet(DEVICE_TRAFFIC,index=False)
    logging.info("Extraction of aggregated files completed.")

with DAG(
    dag_id             = "quickcart_daily_elt",
    description="QuickCart daily ELT pipeline",
    tags=["elt", "quickcart", "pandas"],
    schedule           = "0 2 * * *",          # 02:00 UTC every day
    catchup            = False,                # don't back-fill historical runs
    max_active_runs    = 1,                    # prevent concurrent DAG runs
    default_args       = default_args,
) as dag:

    sensors = []
    for table in TABLES:
        sensor = FileSensor(
            task_id        = f"sense_{table}_csv",
            filepath       = os.path.join(SOURCE_DIR, f"{table}.csv"),
            fs_conn_id     = "fs_default",
            poke_interval  = 10,               
            timeout        = 60,             
            mode           = "poke",      
            soft_fail      = False,
        )
        sensors.append(sensor)
    
    extract_validate = PythonOperator(
        task_id         = "extract_validate_csv_files",
        python_callable = extraction_validation,
    )


    clean_data = PythonOperator(
        task_id         = "clean_data",
        python_callable = cleaning,
    )

  
    aggregation = PythonOperator(
        task_id         = "aggregations_export",
        python_callable = aggregations,
    )

   
    notify_success = EmailOperator(
        task_id  = "notify_success",
        to       = 'crackrojo89@gmail.com',
        subject  = "✅ daily pipeline succeeded",
        html_content = """
            <h3>Daily ELT pipeline completed successfully</h3>
            <p><b>Date:</b> {{ ds }}</p>
            <p><b>DAG:</b> {{ dag.dag_id }}</p>
            <p><b>Run ID:</b> {{ run_id }}</p>
            <p>All KPI exports are available in the outputs bucket.</p>
        """,
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )


    notify_failure = EmailOperator(
        task_id  = "notify_failure",
        to       = 'crackrojo89@gmail.com',
        subject  = "❌ daily pipeline failed",
        html_content = """
            <h3 style="color:red;">Daily ELT pipeline FAILED</h3>
            <p><b>Date:</b> {{ ds }}</p>
            <p><b>DAG:</b> {{ dag.dag_id }}</p>
            <p><b>Run ID:</b> {{ run_id }}</p>
            <p>Please check the Airflow logs immediately.</p>
        """,
        trigger_rule = TriggerRule.ONE_FAILED,   # fires when any task fails
    )


    for sensor in sensors:
        sensor >> extract_validate

    extract_validate >> clean_data >> aggregation

    aggregation >> notify_success

    [*sensors, extract_validate, clean_data, aggregation] >> notify_failure
    