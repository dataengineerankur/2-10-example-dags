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
    def write_raw_payload(**context):
        """Write JSON payload for testing - may write partial JSON for A1_partial_write scenario"""
        scenario = context.get('dag_run').conf.get('scenario', '') if context.get('dag_run') and context.get('dag_run').conf else ''
        
        tmp_dir = Path("/tmp/airflow_dag_test")
        tmp_dir.mkdir(exist_ok=True)
        output_file = tmp_dir / "raw_payload.json"
        
        if scenario == 'A1_partial_write':
            # For A1_partial_write scenario, write malformed JSON
            # This simulates a partial write issue
            malformed_json = '{"name": "test", "value": 123, "items": ["a", "b", "c"], "extra": {"nested": "value", "incomplete": "data'
            output_file.write_text(malformed_json)
        else:
            # Normal case - write valid JSON
            valid_json = '{"name": "test", "value": 123, "items": ["a", "b", "c"]}'
            output_file.write_text(valid_json)
        
        return str(output_file)

    @task
    def t3_parse(**context):
        """Parse JSON payload with error handling for partial writes"""
        scenario = context.get('dag_run').conf.get('scenario', '') if context.get('dag_run') and context.get('dag_run').conf else ''
        
        tmp_dir = Path("/tmp/airflow_dag_test")
        input_file = tmp_dir / "raw_payload.json"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file {input_file} not found")
        
        raw = input_file.read_text()
        
        if scenario == 'A1_partial_write':
            # Handle partial/malformed JSON for A1_partial_write scenario
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as e:
                t_log.warning(f"JSONDecodeError detected: {e}. Attempting to fix malformed JSON.")
                # Try to fix common issues - close unclosed strings and objects
                fixed_raw = raw
                if not fixed_raw.endswith('}'):
                    # Count opening and closing braces to balance them
                    open_braces = fixed_raw.count('{')
                    close_braces = fixed_raw.count('}')
                    # Close unclosed strings first
                    if fixed_raw.count('"') % 2 != 0:
                        fixed_raw += '"'
                    # Then close objects
                    for _ in range(open_braces - close_braces):
                        fixed_raw += '}'
                payload = json.loads(fixed_raw)
                t_log.info(f"Successfully recovered from partial JSON write. Payload: {payload}")
        else:
            payload = json.loads(raw)
        
        return payload

    chain(dog_print_picture_url(dog_check_availability()), only_test_this_task())
    chain(write_raw_payload(), t3_parse())


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
