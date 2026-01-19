"""
### A5 Wrong Partition Example

This DAG demonstrates proper partition handling for date-based partitions.
The scenario tests handling of partition paths correctly using execution dates.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import os


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["2-10", "partition", "demo"],
    default_args={"retries": 0},
    dag_id="A5_wrong_partition",
)
def A5_wrong_partition():

    @task
    def t1_setup(**context):
        """Setup task to create directory structure"""
        run_id = context["run_id"]
        ds = context["ds"]  # execution date in YYYY-MM-DD format
        
        # Create base directory
        base_path = f"/tmp/airflow_data/{run_id}"
        partition_path = f"{base_path}/dt={ds}"
        
        os.makedirs(partition_path, exist_ok=True)
        
        # Write some data to the partition
        data_file = f"{partition_path}/data.txt"
        with open(data_file, "w") as f:
            f.write(f"Data for partition {ds}\n")
        
        print(f"Created partition: {partition_path}")
        return partition_path

    @task
    def t2_write_raw(**context):
        """Write task that reads from the correct partition"""
        run_id = context["run_id"]
        ds = context["ds"]  # Use execution date, not hardcoded date
        
        # Use the correct partition based on execution date
        base_path = f"/tmp/airflow_data/{run_id}"
        partition_path = f"{base_path}/dt={ds}"
        data_file = f"{partition_path}/data.txt"
        
        # Check if partition exists
        if not os.path.exists(partition_path):
            raise FileNotFoundError(
                f"partition dt={ds} not found for run_id={run_id.split('__')[1]}"
            )
        
        # Read from partition
        with open(data_file, "r") as f:
            data = f.read()
        
        print(f"Read data from partition {ds}: {data}")
        
        # Write output
        output_file = f"{base_path}/output.txt"
        with open(output_file, "w") as f:
            f.write(f"Processed: {data}")
        
        return output_file

    # Set up task dependencies
    partition_path = t1_setup()
    t2_write_raw()


A5_wrong_partition()
