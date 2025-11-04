# 🇧🇷 E-commerce Data Pipeline (Airflow, Pandas, PySpark, PostgreSQL)

## 🎯 Project Overview
This project implements an end-to-end Extract, Transform, Load, and Analyze (ETLA) data pipeline using Apache Airflow. The goal was to process raw E-commerce data (Brazilian Olist datasets), standardize, merge, load it into a PostgreSQL data warehouse, and perform a final analysis using PySpark.

The pipeline leverages Airflow's TaskFlow API for efficient, parallel execution and uses intermediate Parquet files for fast data transfer between tasks.

### ⚙️ Tech Stack Used

| Component | Role | Notes |
| :--- | :--- | :--- |
| **Airflow** | Orchestration | TaskFlow API (`@task` decorator) and TaskGroups. |
| **Python/Pandas** | Ingestion & Transformation | Data reading, cleaning, merging, and writing to Parquet. |
| **PostgreSQL** | Data Warehouse | Target database for final merged data (`ecommerce_merged_data`). |
| **PySpark** | Advanced Analysis | Reads from Postgres, groups by customer state, and counts unique customers  |
| **Docker Compose** | Environment | Local development and deployment of Airflow, Postgres, and Redis services. |

--- 

## 📁 Project Folder Structure
```
.
├── airflow-ecom-pipeline/
│   ├── .devcontainer/           # VS Code Dev Container Configuration
│   │   ├── devcontainer.json
│   │   └── config/
│   │       └── airflow.cfg
│   ├── dags/
│   │   ├── __pycache__/
│   │   └── ecom_dag.py           # The main Airflow DAG definition
│   ├── data/                   # Shared volume for source files and pipeline output
│   │   ├── olist_customers_dataset.csv  # Source Data 1
│   │   └── olist_orders_dataset.csv     # Source Data 2
│   ├── logs/                   # Airflow runtime logs
│   │   ├── dag_id=ecommerce_data_pipeline/
│   │   └── ... (manual run logs)
│   ├── plugins/
│   ├── .Dockerfile             # Custom image to install Python/PySpark dependencies
│   ├── .env                    # Environment variables for the Docker stack
│   ├── db.env                  # Environment variables for the Postgres service
│   ├── docker-compose.yaml     # Defines all services (Airflow, Postgres, Redis, etc.)
│   ├── README.md               # This file!
│   └── requirements.txt        # Python dependencies (pandas, pyarrow, pyspark, postgres-provider)
└── .gitignore
```

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

## 🛠️ Setup 

### Building and Launching the Stack

Execute the following commands from project root:

1.  **Stopping and Cleaning (If running):**
    ```bash
    docker compose down
    ```
2.  **Build the Custom Image:** (Includes the DAG code and Python dependencies)
    ```bash
    docker compose build
    ```
3.  **Starting the Services:**
    ```bash
    docker compose up -d
    ```

### ▶️ Execution

1.  **Access Airflow UI:** Navigate to `http://localhost:8080` (or your configured port). [depending on the ports]
2.  **Unpause the DAG:** Locate `ecommerce_data_pipeline` and toggle the switch ON.
3.  **Trigger the Run:** Click the "Play" icon to trigger a manual run.

### Expected Outcome

The pipeline should run without errors, performing the following key actions:

* **Ingestion:** Creates `customers_cleaned.parquet` and `orders_cleaned.parquet`.
* **Merge:** Creates `merged_ecommerce_data.parquet`.
* **Loading:** Creates the `ecommerce_merged_data` table in Postgres.
* **Analysis:** Creates the directory `data/customer_state_analysis/` containing the unique customer count CSV results.
* **Cleanup:** Deletes all intermediate Parquet files and the PySpark analysis directory content, leaving only the original CSV files.

---

## ✅ Verification

A successful run is confirmed when:
* The DAG Run status is Success (Green).
* The output directory data/customer_state_analysis/ contains the final PySpark CSV result.
* The intermediate files (*cleaned.parquet, merged*.parquet) in the data/ folder have been automatically deleted by the final cleanup task.

---

## Outputs:
1. Data Pipeline
![alt text](</screenshots/ecommerce_data_pipeline-graph (1).png>)

2. Task Completion
![alt text](</screenshots/Screenshot 2025-11-04 at 6.19.23 PM.png>)

3. Verification (successful run step 2 and 3):
![alt text](</screenshots/Screenshot 2025-11-04 at 6.34.45 PM.png>)

This log verifies two things:
1. The perform_pyspark_analysis task successfully ran and created the `data/customer_state_analysis/` directory (containing the final CSV).
2. The subsequent cleanup task then successfully deleted the directory, fulfilling the cleanup requirement.