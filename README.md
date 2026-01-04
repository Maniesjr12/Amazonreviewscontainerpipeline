# Amazon Reviews Container Pipeline

A containerized data pipeline for processing and analyzing Amazon business reviews using Apache Airflow, Apache Spark, and PostgreSQL.

## Overview

This project implements an end-to-end data pipeline that ingests, processes, and stores Amazon business reviews. The pipeline is fully containerized using Docker, making it easy to deploy and scale across different environments.

## Architecture

The pipeline consists of the following components:

- **Apache Airflow**: Orchestrates the data pipeline workflow and schedules tasks
- **Apache Spark**: Performs distributed data processing and transformations
- **PostgreSQL**: Stores processed review data and metadata
- **Docker**: Containerizes all services for consistent deployment

## Project Structure

```
Amazonreviewscontainerpipeline/
├── dags/                      # Airflow DAG definitions
├── spark_scripts/             # PySpark processing scripts
├── docker-compose.yaml        # Multi-container Docker application
├── dockerfile.airflow         # Custom Airflow image
├── dockerfile.spark           # Custom Spark image
├── init.sql                   # Database initialization script
├── requirements.txt           # Python dependencies
├── spark-defaults.conf        # Spark configuration
└── README.md                  # Project documentation
```

## Prerequisites

Before running this project, ensure you have the following installed:

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- At least 4GB of available RAM
- 10GB of free disk space

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Maniesjr12/Amazonreviewscontainerpipeline.git
cd Amazonreviewscontainerpipeline
```

### 2. Start the Pipeline

Launch all services using Docker Compose:

```bash
docker-compose up -d
```

This command will:

- Build custom Docker images for Airflow and Spark
- Start the PostgreSQL database
- Initialize the Airflow webserver and scheduler
- Launch Spark master and worker nodes

### 3. Access the Services

Once all containers are running, you can access:

- **Airflow Web UI**: http://localhost:8080
  - Default credentials: (check your docker-compose.yaml)
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL Database**: localhost:5432

### 4. Trigger the Pipeline

1. Navigate to the Airflow web interface
2. Enable the DAG from the list of available workflows
3. Trigger the DAG manually or wait for the scheduled run

## Pipeline Workflow

The data pipeline follows these steps:

1. **Data Ingestion**: Extracts Amazon review data from source
2. **Data Validation**: Validates data quality and schema
3. **Spark Processing**: Transforms and enriches review data
4. **Data Loading**: Stores processed data in PostgreSQL
5. **Monitoring**: Logs pipeline execution and status

## Configuration

### Airflow Configuration

Modify Airflow settings in `docker-compose.yaml`:

```yaml
environment:
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@postgres/airflow
```

### Spark Configuration

Adjust Spark settings in `spark-defaults.conf`:

```conf
spark.master                     spark://spark-master:7077
spark.executor.memory            2g
spark.driver.memory              1g
```

## Development

### Adding New DAGs

1. Create a new Python file in the `dags/` directory
2. Define your DAG using Airflow operators
3. The DAG will be automatically detected by Airflow

Example:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_reviews():
    # Your processing logic here
    pass

dag = DAG(
    'amazon_reviews_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily'
)

task = PythonOperator(
    task_id='process_reviews',
    python_callable=process_reviews,
    dag=dag
)
```

### Adding Spark Jobs

1. Create a new PySpark script in `spark_scripts/`
2. Reference the script in your Airflow DAG using SparkSubmitOperator

## Monitoring and Logs

### View Container Logs

```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs airflow-webserver
docker-compose logs spark-master
```

### Monitor Pipeline Status

- Check Airflow UI for DAG run history and task status
- Monitor Spark UI for job execution and resource usage
- Query PostgreSQL database for processed data

## Troubleshooting

### Common Issues

**Container fails to start:**

```bash
# Check container status
docker-compose ps

# Restart specific service
docker-compose restart airflow-webserver
```

**Database connection errors:**

- Verify PostgreSQL container is running
- Check connection string in docker-compose.yaml
- Ensure database initialization completed successfully

**Spark job failures:**

- Check Spark worker logs for memory issues
- Verify Spark configuration in spark-defaults.conf
- Ensure sufficient resources are allocated to Docker

## Stopping the Pipeline

To stop all services:

```bash
# Stop containers
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v
```

## Data Schema

The processed Amazon reviews are stored with the following schema:

- `review_id`: Unique identifier for each review
- `product_id`: Amazon product identifier
- `reviewer_id`: Anonymous reviewer identifier
- `rating`: Product rating (1-5 stars)
- `review_text`: Full text of the review
- `review_date`: Date the review was posted
- `verified_purchase`: Boolean indicating verified purchase
- `helpful_votes`: Number of helpful votes received

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
