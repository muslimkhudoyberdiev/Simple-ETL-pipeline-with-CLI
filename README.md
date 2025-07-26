# Data Ingestion ETL Pipeline

A simple ETL pipeline that processes CSV data and loads it into a SQLite database. Includes both CLI and Airflow integration.

## Quick Start

### Local Development
```bash
# Setup virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline
python cli.py run-etl
```

### With Docker & Airflow
```bash
# Start Airflow
docker-compose up -d

# Access Airflow UI
# http://localhost:8080 (admin/admin)
```

## Project Structure
```
├── cli.py              # Main CLI application
├── etl/                # ETL modules
│   ├── extract.py      # Data extraction
│   ├── transform.py    # Data transformation
│   └── load.py         # Data loading
├── db/                 # Database models
├── dags/               # Airflow DAGs
├── data/               # Sample CSV files
└── tests/              # Unit tests
```

## Features
- Extract data from CSV files
- Transform and clean data
- Load into SQLite database
- CLI interface with Typer
- Airflow integration for scheduling
- Unit tests with pytest

## Data Sources
- `data/catalogs.csv` - Product catalog data
- `data/products.csv` - Product details

## Testing
```bash
pytest tests/
```

## Requirements
- Python 3.8+
- Docker (for Airflow)
- See `requirements.txt` for Python packages 