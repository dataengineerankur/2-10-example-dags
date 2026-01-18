"""
### DAG that shows dag.test() with auto marking sensors as success

"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.models.baseoperator import chain
from airflow.models import Variable
from pendulum import datetime
import logging
import requests
import json
import os

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    tags=["2-10", "dag.test()"],
)
def dag_test_example():

    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def dog_check_availability() -> PokeReturnValue:
        r = requests.get("https://random.dog/woof.json")  # correct URL
        # r = requests.get("https://random.dog/wwwwoof.json")  # URL with TYPO
        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.json()
        else:
            condition_met = False
            operator_return_value = None
            print(f"Woof URL returned the status code {r.status_code}")
        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    @task
    def dog_print_picture_url(url):
        print(url)

    @task
    def only_test_this_task():
        return "HELLO !!!! :) "

    @task
    def write_raw_payload():
        """Write raw payload to file, potentially malformed depending on scenario."""
        scenario = Variable.get("scenario", default_var="default")
        output_file = "/tmp/raw_payload.json"
        
        if scenario == "A2_wrong_shape":
            # Write empty/malformed JSON for this scenario
            with open(output_file, "w") as f:
                f.write("")  # Empty string causes JSONDecodeError
            t_log.info(f"Scenario {scenario}: Wrote empty payload to {output_file}")
        else:
            # Default: write valid JSON
            with open(output_file, "w") as f:
                json.dump({"status": "ok", "data": [1, 2, 3]}, f)
            t_log.info(f"Scenario {scenario}: Wrote valid JSON to {output_file}")
        
        return output_file

    @task
    def t3_parse(input_file):
        """Parse JSON from file with proper error handling."""
        with open(input_file, "r") as f:
            raw = f.read()
        
        # Handle empty or malformed JSON gracefully
        if not raw or raw.strip() == "":
            t_log.warning("Empty input detected, returning empty dict")
            return {}
        
        try:
            payload = json.loads(raw)
            t_log.info(f"Successfully parsed JSON: {payload}")
            return payload
        except json.JSONDecodeError as e:
            t_log.warning(f"JSONDecodeError: {e}, returning empty dict")
            return {}

    chain(dog_print_picture_url(dog_check_availability()), only_test_this_task())
    
    # Add scenario test tasks
    t3_parse(write_raw_payload())


dag = dag_test_example()

if __name__ == "__main__":
    conn_path = "connections.yaml"
    variables_path = "variables.yaml"
    my_conf_var = 23

    dag.test(
        execution_date=datetime(2023, 1, 29),
        conn_file_path=conn_path,
        variable_file_path=variables_path,
        run_conf={"my_conf_var": my_conf_var},
        # new in Airflow 2.10
        mark_success_pattern="dog.*",  # regex of task ids to be auto-marked as successful
        use_executor=True
    )

    # if you are using the CLI to test:
    # airflow dags test dag_test_example --mark-success-pattern "dog.*"
