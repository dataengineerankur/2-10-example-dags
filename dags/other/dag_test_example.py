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
import tempfile

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    tags=["2-10", "dag.test()"],
)
def dag_test_example():
    scenario = Variable.get("scenario", default_var=None)

    @task
    def write_raw_payload():
        """Write JSON payload to a temporary file."""
        json_file = os.path.join(tempfile.gettempdir(), "test_payload.json")
        payload = {
            "status": "success",
            "data": {
                "id": 123,
                "name": "Test Item",
                "description": "A test description for validation"
            },
            "timestamp": "2026-01-18T19:22:19"
        }
        # Write complete JSON (not partial) to fix the A1_partial_write scenario
        with open(json_file, 'w') as f:
            json.dump(payload, f)
        return json_file

    @task
    def parse_json(json_file_path):
        """Parse JSON from file."""
        with open(json_file_path, 'r') as f:
            raw = f.read()
        payload = json.loads(raw)
        t_log.info(f"Successfully parsed JSON: {payload}")
        return payload

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

    # Conditional task chain based on scenario
    if scenario == "A1_partial_write":
        json_file = write_raw_payload()
        t3_parse = parse_json(json_file)
        chain(t3_parse, only_test_this_task())
    else:
        chain(dog_print_picture_url(dog_check_availability()), only_test_this_task())


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
