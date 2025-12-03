"""
Test DAG to verify Airflow setup is working properly.
This DAG contains simple tasks to test the basic Airflow functionality.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

# Define the DAG
dag = DAG(
    'test_setup_dag',
    default_args=default_args,
    description='Test DAG to verify Airflow setup is working',
    schedule_interval=timedelta(days=1),
    catchup=False,
)


def print_hello():
    """Simple Python task to print hello."""
    print("Hello from Airflow Test DAG!")
    return "Hello executed successfully"


def print_date():
    """Simple Python task to print current date."""
    from datetime import datetime
    print(f"Current date and time: {datetime.now()}")
    return datetime.now().isoformat()


# Define tasks
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task_date = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

task_bash = BashOperator(
    task_id='bash_test',
    bash_command='echo "Bash command executed successfully in Airflow"',
    dag=dag,
)

task_final = BashOperator(
    task_id='final_success',
    bash_command='echo "✓ Airflow setup is working correctly!"',
    dag=dag,
)

# Define dependencies
task_hello >> task_date >> task_bash >> task_final
