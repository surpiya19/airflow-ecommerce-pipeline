from __future__ import annotations

import logging
import pandas as pd
from datetime import datetime
import os
import glob  # For cleaning up PySpark output directory
import shutil

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession  # Used for PySpark tasks

# --- Configuration ---
# Set the base directory for shared data volume
AIRFLOW_DATA_DIR = "/opt/airflow/data"

# Source CSV files (should be present in the data/ directory on the host)
CUSTOMERS_CSV = f"{AIRFLOW_DATA_DIR}/olist_customers_dataset.csv"
ORDERS_CSV = f"{AIRFLOW_DATA_DIR}/olist_orders_dataset.csv"

# Intermediate Parquet files (will be cleaned up)
CLEANED_CUSTOMERS_PARQUET = f"{AIRFLOW_DATA_DIR}/customers_cleaned.parquet"
CLEANED_ORDERS_PARQUET = f"{AIRFLOW_DATA_DIR}/orders_cleaned.parquet"
MERGED_DATA_PARQUET = f"{AIRFLOW_DATA_DIR}/merged_ecommerce_data.parquet"

# Database and Analysis Configuration
POSTGRES_CONN_ID = "postgres_default"  # Assumes default connection from docker-compose
TARGET_TABLE = "ecommerce_merged_data"
ANALYSIS_OUTPUT_DIR = f"{AIRFLOW_DATA_DIR}/customer_state_analysis"

# --- Stage 1: Data Ingestion Tasks ---


@task
def ingest_data(csv_path: str, output_parquet_path: str) -> str:
    """Ingests CSV data and saves it to Parquet for inter-task efficiency."""
    logging.info(f"Starting ingestion for: {csv_path}")
    df = pd.read_csv(csv_path)

    # Save the data to Parquet format
    df.to_parquet(output_parquet_path, index=False)

    logging.info(f"Ingested data saved to {output_parquet_path} with {len(df)} rows.")
    return output_parquet_path  # XCom pushes the output path


# --- Stage 2: Data Transformation Tasks (inside TaskGroup) ---


@task
def clean_customers(input_parquet_path: str) -> str:
    """Cleans and standardizes the customers dataset."""
    logging.info(f"Starting customer data cleaning on {input_parquet_path}")
    df = pd.read_parquet(input_parquet_path)

    # Transformation 1: Convert zip code prefix to string (padding with zeros)
    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"].astype(str).str.zfill(5)
    )

    # Transformation 2: Convert city/state to uppercase for standardization
    df["customer_city"] = df["customer_city"].str.upper()
    df["customer_state"] = df["customer_state"].str.upper()

    df.to_parquet(input_parquet_path, index=False)
    logging.info("Customer data cleaning complete.")
    return input_parquet_path


@task
def clean_orders(input_parquet_path: str) -> str:
    import pandas as pd
    import numpy as np
    import logging

    logging.info(f"Starting orders data cleaning on {input_parquet_path}")
    df = pd.read_parquet(input_parquet_path)

    # Clean malformed rows: drop rows that are fully empty or missing order_id
    df = df[df["order_id"].notna()]

    # Identify timestamp-like columns
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    # Convert safely to datetime
    for c in timestamp_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Fill null approvals with purchase timestamp
    if "order_approved_at" in df.columns:
        df["order_approved_at"] = df["order_approved_at"].fillna(
            df["order_purchase_timestamp"]
        )

    # Explicitly cast to datetime64[ns] (removes tz & mixed types)
    for c in timestamp_cols:
        if c in df.columns:
            df[c] = df[c].astype("datetime64[ns]")

    # Optional: ensure status lowercase/consistent
    df["order_status"] = df["order_status"].str.lower()

    # Save clean version to a *new* file
    cleaned_path = input_parquet_path.replace(".parquet", "_cleaned.parquet")
    df.to_parquet(cleaned_path, index=False)
    logging.info(f"Orders data cleaning complete. Saved to {cleaned_path}")
    return cleaned_path


@task
def merge_data(customers_path: str, orders_path: str) -> str:
    """Merges the cleaned customers and orders datasets on 'customer_id'."""
    logging.info("Starting data merge.")
    customers_df = pd.read_parquet(customers_path)
    orders_df = pd.read_parquet(orders_path)

    # Merge using a left join (keeping all orders)
    merged_df = pd.merge(orders_df, customers_df, on="customer_id", how="left")

    merged_df.to_parquet(MERGED_DATA_PARQUET, index=False)
    logging.info(
        f"Merged data saved to {MERGED_DATA_PARQUET} with {len(merged_df)} rows."
    )
    return MERGED_DATA_PARQUET


# --- Stage 3: Data Loading Task ---


@task
def load_to_postgres(input_parquet_path: str, target_table: str):
    """Loads the final merged Parquet file into the PostgreSQL database."""
    logging.info(
        f"Starting load of {input_parquet_path} to PostgreSQL table {target_table}"
    )
    df = pd.read_parquet(input_parquet_path)

    # Setup Postgres Hook
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Use pandas to_sql for loading
    df.to_sql(
        target_table,
        con=pg_hook.get_sqlalchemy_engine(),
        if_exists="replace",  # Replace table for clean re-runs
        index=False,
    )
    logging.info(f"Successfully loaded {len(df)} rows into table {target_table}.")


# --- Stage 4: Data Analysis Task (Super Bonus - PySpark) ---

'''
@task
def perform_pyspark_analysis(target_table: str, output_path: str):
    """
    Reads data from Postgres using PySpark, performs a simple analysis
    (Unique Customers per State), and saves the result.
    """
    logging.info(f"Starting PySpark analysis on table: {target_table}")

    # Initialize Spark Session (Configured for local use in the container)
    spark = SparkSession.builder.appName("PostgresReadAnalysis").getOrCreate()

    # Database connection properties (using the values from docker-compose.yaml)
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }

    # Read data from PostgreSQL
    spark_df = spark.read.jdbc(
        url=jdbc_url, table=target_table, properties=db_properties
    )

    # Analysis: Group by state and count unique customer IDs
    analysis_df = spark_df.groupBy("customer_state").agg(
        {"customer_unique_id": "count"}
    )
    analysis_df = analysis_df.withColumnRenamed(
        "count(customer_unique_id)", "unique_customer_count"
    )

    # Sort and log a sample result
    sorted_result = analysis_df.sort("unique_customer_count", ascending=False)
    logging.info("--- Top 5 States by Unique Customers ---")
    for row in sorted_result.limit(5).collect():
        logging.info(
            f"State: {row['customer_state']}, Count: {row['unique_customer_count']}"
        )

    # Save the result to the shared volume (PySpark creates a directory of files)
    sorted_result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    spark.stop()
    logging.info(f"PySpark analysis complete. Results saved to {output_path}")
'''


# --- Updated perform_pyspark_analysis ---
@task
def perform_pyspark_analysis(target_table: str, output_path: str):
    """
    Reads data from Postgres using PySpark, performs unique customers per state,
    and saves result as CSV.
    """
    import logging
    from pyspark.sql import SparkSession

    logging.info(f"Starting PySpark analysis on table: {target_table}")

    # Initialize Spark Session with JDBC driver
    spark = (
        SparkSession.builder.appName("PostgresReadAnalysis")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }

    spark_df = spark.read.jdbc(
        url=jdbc_url, table=target_table, properties=db_properties
    )

    # Count unique customers per state
    analysis_df = spark_df.groupBy("customer_state").agg(
        {"customer_unique_id": "count"}
    )
    analysis_df = analysis_df.withColumnRenamed(
        "count(customer_unique_id)", "unique_customer_count"
    )
    analysis_df = analysis_df.sort("unique_customer_count", ascending=False)

    logging.info("--- Top 5 States by Unique Customers ---")
    for row in analysis_df.limit(5).collect():
        logging.info(
            f"State: {row['customer_state']}, Count: {row['unique_customer_count']}"
        )

    # Save as single CSV
    analysis_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    spark.stop()
    logging.info(f"PySpark analysis complete. Results saved to {output_path}")


# --- Stage 5: Cleanup Task ---


@task
def cleanup_intermediate_files(
    customers_cleaned_path: str,
    orders_cleaned_path: str,
    merged_path: str,
    analysis_dir: str,
):
    """Deletes intermediate Parquet files and the PySpark output directory."""

    import os
    import logging

    # Delete Parquet files
    for f in [customers_cleaned_path, orders_cleaned_path, merged_path]:
        if os.path.exists(f):
            os.remove(f)
            logging.info(f"Deleted intermediate file: {f}")
        else:
            logging.warning(f"File not found for deletion: {f}")

    # Delete PySpark output directory safely
    if os.path.isdir(analysis_dir):
        try:
            shutil.rmtree(analysis_dir)
            logging.info(f"Deleted PySpark analysis directory: {analysis_dir}")
        except Exception as e:
            logging.error(f"Failed to delete analysis directory {analysis_dir}: {e}")
    else:
        logging.warning(f"Analysis directory not found: {analysis_dir}")


'''
def cleanup_intermediate_files(
    customers_path: str, orders_path: str, merged_path: str, analysis_dir: str
):
    """Deletes intermediate Parquet files and the PySpark output directory."""

    # 1. Delete Parquet files
    files_to_delete = [customers_path, orders_path, merged_path]
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Cleaned up intermediate file: {file_path}")
        else:
            logging.warning(f"File not found for cleanup: {file_path}")

    # 2. Delete PySpark output directory (it's a directory, not a single file)
    if os.path.isdir(analysis_dir):
        # Delete all files/subdirectories within the output directory
        for filename in glob.glob(os.path.join(analysis_dir, "*")):
            if os.path.isfile(filename):
                os.remove(filename)
            elif os.path.isdir(filename):
                os.rmdir(filename)
        os.rmdir(analysis_dir)
        logging.info(f"Cleaned up PySpark analysis directory: {analysis_dir}")
    else:
        logging.warning(f"Analysis directory not found for cleanup: {analysis_dir}")
'''

# --- DAG Definition ---


@dag(
    dag_id="ecommerce_data_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run manually
    catchup=False,
    tags=["ecommerce", "etl", "pyspark", "assignment"],
    doc_md="""
    # E-commerce Data Pipeline
    End-to-end Airflow pipeline for ingesting, transforming, merging, loading, and analyzing Brazilian e-commerce data.
    Implements parallelism and the PySpark Super Bonus.
    """,
)
def ecommerce_pipeline():

    # Stage 1: Data Ingestion (Parallel)
    # The output of these tasks (the path strings) are implicitly XCom-pushed.
    ingested_cust_path = ingest_data.override(task_id="ingest_customers")(
        csv_path=CUSTOMERS_CSV, output_parquet_path=CLEANED_CUSTOMERS_PARQUET
    )
    ingested_order_path = ingest_data.override(task_id="ingest_orders")(
        csv_path=ORDERS_CSV, output_parquet_path=CLEANED_ORDERS_PARQUET
    )

    # Stage 2: Data Transformation (TaskGroup for Organization and Dependency Management)
    with TaskGroup("transform_data") as transform_group:

        # Parallel Cleaning
        cleaned_cust_path = clean_customers(ingested_cust_path)
        cleaned_order_path = clean_orders(ingested_order_path)

        # Merge (must wait for both cleaning tasks via dependency inheritance)
        merged_path = merge_data(cleaned_cust_path, cleaned_order_path)

        # Set the TaskGroup output to the path of the final merged file
        # transform_group.set_upstream([cleaned_cust_path, cleaned_order_path])

    # Stage 3: Data Loading
    load_task = load_to_postgres(merged_path, target_table=TARGET_TABLE)

    # Stage 4: Data Analysis (Super Bonus)
    analysis_task = perform_pyspark_analysis(
        target_table=TARGET_TABLE, output_path=ANALYSIS_OUTPUT_DIR
    )

    # Stage 5: Cleanup
    cleanup_task = cleanup_intermediate_files(
        ingested_cust_path,
        ingested_order_path,
        merged_path,  # Path from the merge_data task
        analysis_dir=ANALYSIS_OUTPUT_DIR,  # PySpark output dir
    )

    # Define the final pipeline structure
    # [Parallel Ingestion] -> [Transformation Group] -> [Load] -> [Analyze] -> [Cleanup]
    chain(
        [ingested_cust_path, ingested_order_path],
        transform_group,
        load_task,
        analysis_task,
        cleanup_task,
    )


ecommerce_pipeline()
