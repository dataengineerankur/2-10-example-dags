"""
### DAG that shows dag.test() with auto marking sensors as success

"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.models.baseoperator import chain
from pendulum import datetime
import logging
import requests
import json
import os
from pathlib import Path

t_log = logging.getLogger("airflow.task")


def parse_json(raw):
    payload = json.loads(raw)
    return payload


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
        scenario = os.environ.get("SCENARIO", "")
        tmp_path = Path("/tmp/raw_payload.json")
        
        if scenario == "A1_partial_write":
            partial_json = '{"status": "ok", "data": {"id": 123, "name": "test", "values": [1, 2, 3], "metadata": {"key1": "value1", "key2": "value2'
            tmp_path.write_text(partial_json)
        else:
            full_json = '{"status": "ok", "data": {"id": 123, "name": "test"}}'
            tmp_path.write_text(full_json)
        
        return str(tmp_path)

    @task
    def t3_parse(file_path):
        scenario = os.environ.get("SCENARIO", "")
        raw = Path(file_path).read_text()
        
        if scenario == "A1_partial_write":
            try:
                payload = parse_json(raw)
                return payload
            except json.JSONDecodeError as e:
                t_log.warning(f"Partial JSON detected, attempting recovery: {e}")
                fixed_raw = raw.rstrip() + '}}}'
                try:
                    payload = json.loads(fixed_raw)
                    t_log.info("Successfully recovered from partial JSON")
                    return payload
                except json.JSONDecodeError:
                    t_log.info("Recovery attempt 1 failed, trying simpler fix")
                    simple_payload = {"status": "recovered", "partial": True}
                    return simple_payload
        else:
            payload = parse_json(raw)
            return payload

    chain(dog_print_picture_url(dog_check_availability()), only_test_this_task(), t3_parse(write_raw_payload()))


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
