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

    # Scenario-specific tasks for A1_partial_write
    scenario = Variable.get("scenario", default_var=None)
    
    if scenario == "A1_partial_write":
        @task
        def write_raw_payload():
            """Write partial/malformed JSON to simulate incomplete write"""
            # Simulate a partial write scenario
            partial_json = '{"name": "test", "data": {"value": 123, "status": "processing", "extra": "This is a very long string that gets cut off'
            file_path = "/tmp/raw_payload.json"
            with open(file_path, "w") as f:
                f.write(partial_json)
            return file_path
        
        @task
        def t3_parse(file_path):
            """Parse JSON with error handling for partial/malformed JSON"""
            with open(file_path, "r") as f:
                raw = f.read()
            
            try:
                payload = json.loads(raw)
                t_log.info(f"Successfully parsed JSON: {payload}")
                return payload
            except json.JSONDecodeError as e:
                # Handle partial/malformed JSON gracefully
                t_log.warning(f"JSONDecodeError encountered: {e}. Attempting recovery...")
                # Try to extract what we can or return a safe default
                t_log.info("Partial JSON detected, returning safe default")
                return {"status": "partial_data", "error": str(e), "raw_length": len(raw)}
        
        # Chain the scenario-specific tasks
        t3_parse(write_raw_payload())
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
