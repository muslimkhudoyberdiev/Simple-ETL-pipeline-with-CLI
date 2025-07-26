# Data Ingestion Sample - CLI Only

A simple ETL pipeline for processing catalog and product data using SQLite and Airflow.

## Quickstart

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the ETL pipeline:
```bash
python cli.py run-etl
```

3. Query the data:
```bash
# Show top products by price
python cli.py top-products --n 5

# Query products by catalog
python cli.py query-products 5

# Show catalog counts
python cli.py catalog-counts

# View pipeline logs
python cli.py show-logs

# View bad records
python cli.py show-bad-records
```

### Airflow Orchestration

1. Start Airflow:
```bash
docker-compose up airflow
```

2. Access the Airflow UI at [http://localhost:8080](http://localhost:8080)
   - Login: `admin` / `admin`

3. The ETL DAG (`etl_pipeline`) will appear in the Airflow UI. Trigger it manually or let it run on schedule.

---

## Features

- **Simple SQLite Database**: No complex database setup required
- **CLI Interface**: Easy-to-use command-line tools for ETL operations
- **Data Validation**: Automatic detection and logging of bad records
- **Batch Processing**: Support for multiple ETL runs with batch tracking
- **Airflow Integration**: Orchestrate ETL jobs with Apache Airflow

## Data Structure

- **Catalogs**: Product categories with names and creation dates
- **Products**: Items with names, prices, and catalog associations
- **Pipeline Logs**: Track ETL run history and statistics
- **Bad Records**: Log invalid data for review

## Notes
- Data is stored in SQLite database (`shop_inventory.db`)
- Sample data is in the `data/` directory
- No environment configuration needed - everything is self-contained 

---

## 🛠️ **Checklist to Make Your DAGs Appear in Airflow**

### 1. **DAGs Directory Mount**
- Make sure your `docker-compose.yml` mounts the `dags/` directory into the Airflow container:
  ```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./cli.py:/opt/airflow/cli.py
  ```
- Your DAG file (e.g., `etl_dag.py`) should be in the `dags/` folder at the root of your project.

### 2. **DAG File Location**
- The DAG file must be at:  
  `your_project_root/dags/etl_dag.py`

### 3. **DAG Definition**
- The DAG file must define a variable named `dag` or use the `@dag` decorator.
- Example:
  ```python
  from airflow import DAG
  from airflow.operators.bash import BashOperator
  from datetime import datetime, timedelta

  default_args = {...}

  dag = DAG(
      'etl_pipeline',
      default_args=default_args,
      ...
  )

  run_etl = BashOperator(
      task_id='run_etl_cli',
      bash_command='python /opt/airflow/cli.py run-etl',
      dag=dag,
  )
  ```

### 4. **Airflow Scheduler is Running**
- Make sure the Airflow scheduler is running. In Docker Compose, it should be started automatically, but you can check logs:
  ```sh
  docker-compose logs airflow
  ```

### 5. **DAG File Syntax**
- If there is a syntax error in your DAG file, it will not show up. Check Airflow logs for errors:
  ```sh
  docker-compose logs airflow
  ```

### 6. **DAGs Refresh**
- The Airflow UI may take a minute to refresh. Click the refresh button in the top right of the UI.

---

## 🟢 **Quick Fix Steps**

1. **Check that `dags/etl_dag.py` exists and is correct.**
2. **Restart Airflow:**
   ```sh
   docker-compose restart airflow
   ```
3. **Wait 1-2 minutes and refresh the Airflow UI.**

---

## 📝 **If Still Not Working**

- Run:
  ```sh
  docker-compose exec airflow ls /opt/airflow/dags
  ```
  You should see `etl_dag.py` listed.
- Check for errors in:
  ```sh
  docker-compose logs airflow
  ```

---

**If you still don’t see your DAG, please share the output of:**
- `ls dags/` (on your host)
- `docker-compose exec airflow ls /opt/airflow/dags`
- Any relevant lines from `docker-compose logs airflow`

I’ll help you debug further! 