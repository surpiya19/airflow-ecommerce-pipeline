# 🇧🇷 E-commerce Data Pipeline (Airflow, Pandas, PySpark, PostgreSQL)

## 🎯 Project Overview
This project implements an end-to-end Extract, Transform, Load, and Analyze (ETLA) data pipeline using Apache Airflow. The goal is to process raw E-commerce data (Brazilian Olist datasets), standardize, merge, load it into a PostgreSQL data warehouse, and perform a final analysis using PySpark.

The pipeline leverages Airflow's TaskFlow API for efficient, parallel execution and uses intermediate Parquet files for fast data transfer between tasks.

### ⚙️ Tech Stack Used

| Component | Role | Notes |
| :--- | :--- | :--- |
| **Airflow** | Orchestration | TaskFlow API (`@task` decorator) and TaskGroups. |
| **Python/Pandas** | Ingestion & Transformation | Data reading, cleaning, merging, and writing to Parquet. |
| **PostgreSQL** | Data Warehouse | Target database for final merged data (`ecommerce_merged_data`). |
| **PySpark** | Advanced Analysis | Reads from Postgres, groups by customer state, and counts unique customers (Super Bonus). |
| **Docker Compose** | Environment | Local development and deployment of Airflow, Postgres, and Redis services. |

--- 

## 🏗️ Pipeline Architecture (`dags/ecom_dag.py`)
The DAG is divided into 5 distinct stages, emphasizing parallelism where possible:
| Stage | Tasks | Logic / Data Flow | Execution Mode |
| :--- | :--- | :--- | :--- |
| **Stage 1: Ingestion** | `ingest_customers`, `ingest_orders` | CSV $\to$ Parquet files in `/opt/airflow/data`. | **Parallel** |
| **Stage 2: Transformation** | `transform_data` (TaskGroup) | 1. Clean individual datasets (parallel). 2. Merge `orders` and `customers` on `customer_id`. | Parallel Cleaning, Sequential Merge |
| **Stage 3: Loading** | `load_to_postgres` | Load final merged Parquet file into the `ecommerce_merged_data` table in PostgreSQL. | Sequential |
| **Stage 4: Analysis** | `perform_pyspark_analysis` | Read from Postgres (using PySpark JDBC), aggregate unique customers by state, and save results to CSV. | Sequential |
| **Stage 5: Cleanup** | `cleanup_intermediate_files` | Deletes all intermediate Parquet files and the PySpark output directory/temp files. | Sequential |

---

## 🛠️ Setup and Installation

### Prerequisites

1.  Docker and Docker Compose installed.
2.  The required Python libraries (`pandas`, `pyarrow`, `pyspark`, `apache-airflow-providers-postgres`, etc.) must be defined in your `requirements.txt`.
3.  The necessary PySpark dependencies (like the Postgres JDBC driver) must be available in the container environment.

### 1. Data Placement

Place the required CSV files into the **`data/`** directory on your host machine. This directory is volume-mounted to `/opt/airflow/data` inside the Airflow container.

* `data/olist_customers_dataset.csv`
* `data/olist_orders_dataset.csv`

### 2. Airflow Connection Configuration (Crucial)

Before running the DAG, the connection ID **`postgres_default`** must be configured in your Airflow UI or via environment variables in `docker-compose.yaml`.

**Configuration Details:**

| Field | Value |
| :--- | :--- |
| **Conn Id** | `postgres_default` |
| **Conn Type** | `PostgreSQL` |
| **Host** | `postgres` |
| **Schema/Database** | `airflow` |
| **Login** | `airflow` |
| **Password** | `airflow` |
| **Port** | `5432` |

### 3. Build and Launch the Stack

Execute the following commands from your project root:

1.  **Stop and Clean (If running):**
    ```bash
    docker compose down
    ```
2.  **Build the Custom Image:** (Includes the DAG code and Python dependencies)
    ```bash
    docker compose build
    ```
3.  **Start Services:**
    ```bash
    docker compose up -d
    ```

---

## ▶️ Execution

1.  **Access Airflow UI:** Navigate to `http://localhost:8080` (or your configured port).
2.  **Unpause the DAG:** Locate `ecommerce_data_pipeline` and toggle the switch ON.
3.  **Trigger the Run:** Click the "Play" icon to trigger a manual run.

### Expected Outcome

The pipeline should run without errors, performing the following key actions:

* **Ingestion:** Creates `customers_cleaned.parquet` and `orders_cleaned.parquet`.
* **Merge:** Creates `merged_ecommerce_data.parquet`.
* **Loading:** Creates the `ecommerce_merged_data` table in Postgres.
* **Analysis:** Creates the directory `data/customer_state_analysis/` containing the unique customer count CSV results.
* **Cleanup:** Deletes all intermediate Parquet files and the PySpark analysis directory content, leaving only the original CSV files.