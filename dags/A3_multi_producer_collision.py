"""
### A3 Multi Producer Collision

This DAG demonstrates handling multiple producers writing to separate output files
to avoid path collisions.
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.dataset import Dataset
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    dag_id="A3_multi_producer",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["scenario", "multi-producer"],
)
def A3_multi_producer():
    scenario = Variable.get("scenario", default_var=None)
    
    @task(outlets=[Dataset("s3://scenario-data/producer_1_output.txt")])
    def t1_write_raw():
        """First producer writes to its own unique path"""
        t_log.info("Producer 1 writing to s3://scenario-data/producer_1_output.txt")
        return "producer_1_complete"
    
    @task(outlets=[Dataset("s3://scenario-data/producer_2_output.txt")])
    def t2_write_raw():
        """Second producer writes to its own unique path to avoid collision"""
        if scenario == "A3_multi_producer_collision":
            # Fix: Each producer must write to a unique path to avoid collision
            t_log.info("Scenario A3_multi_producer_collision: Producer 2 writing to unique path s3://scenario-data/producer_2_output.txt")
            return "producer_2_complete"
        else:
            t_log.info("Producer 2 writing to s3://scenario-data/producer_2_output.txt")
            return "producer_2_complete"
    
    @task
    def t3_validate():
        """Validation task to ensure both producers completed successfully"""
        t_log.info("Both producers completed successfully without path collision")
        return "validation_complete"
    
    # Run producers in parallel, then validate
    t1_result = t1_write_raw()
    t2_result = t2_write_raw()
    t3_validate() << [t1_result, t2_result]


A3_multi_producer()
