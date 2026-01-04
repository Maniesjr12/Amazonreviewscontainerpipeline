# spark_dag.py
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta 


# Define the command exactly as Spark needs it
SPARK_SUBMIT_COMMAND = """
spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.postgresql:postgresql:42.7.4 \\
    --conf spark.driver.memory=512m \\
    --conf spark.executor.memory=512m \\
    --num-executors 1 \\
    --executor-cores 1 \\
    --executor-memory 1g \\
    /opt/spark/spark_app/test.py
"""

with DAG(
    dag_id='spark_job_testing_dag',
    # Uses the imported datetime class
    start_date=datetime(2025, 11, 12),
    schedule=None,
    catchup=False,
    tags=['spark', 'test'],
) as dag:
    
    create_spark_events_dir = BashOperator(
        task_id='create_spark_events_dir',
        # Create directory with permissions for the airflow user
        bash_command='mkdir -p /opt/spark/events && chmod -R 777 /opt/spark/events',
    )
    
    submit_spark_job = BashOperator(
        task_id='submit_spark_job_to_cluster',
        bash_command=SPARK_SUBMIT_COMMAND,
        execution_timeout=timedelta(minutes=10), 
    )
    
    create_spark_events_dir >> submit_spark_job # Set the dependency!