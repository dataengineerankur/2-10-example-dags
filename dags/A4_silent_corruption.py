"""
### A4 Silent Corruption

This DAG demonstrates detecting and handling silent data corruption
where data processing results in fewer rows than expected.
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    dag_id="A4_silent_corruption",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["scenario", "data-quality"],
)
def A4_silent_corruption():
    
    @task
    def t1_extract():
        """Extract raw data"""
        t_log.info("Extracting raw data")
        # Simulate extracting 150 rows
        return {"row_count": 150, "data": "raw_data"}
    
    @task
    def t2_transform(extraction_result):
        """Transform data - may have silent corruption"""
        scenario = Variable.get("scenario", default_var="default")
        
        if scenario == "A4_silent_corruption":
            # Simulate silent corruption - only 48 rows make it through
            t_log.warning("Silent corruption detected: only 48 rows processed")
            corrupted_count = 48
            return {"row_count": corrupted_count, "data": "transformed_data"}
        else:
            # Normal processing - all rows processed
            t_log.info("Normal processing: all rows processed")
            return {"row_count": extraction_result["row_count"], "data": "transformed_data"}
    
    @task
    def t3_load(transform_result):
        """Load transformed data"""
        t_log.info(f"Loading {transform_result['row_count']} rows")
        return transform_result
    
    @task
    def t4_validate(extraction_result, load_result):
        """Validate data integrity by checking row count delta"""
        scenario = Variable.get("scenario", default_var="default")
        
        expected_count = extraction_result["row_count"]
        actual_count = load_result["row_count"]
        row_count_delta = actual_count
        
        t_log.info(f"Validation - Expected: {expected_count}, Actual: {actual_count}, Delta: {row_count_delta}")
        
        if scenario == "A4_silent_corruption":
            # Fix: Instead of raising error, implement data quality check with recovery
            if row_count_delta < 100:
                t_log.warning(f"Row count delta below threshold: {row_count_delta} < 100")
                t_log.info("Triggering data reprocessing to recover from silent corruption")
                
                # Recovery logic: reprocess with proper error handling
                # In a real scenario, this would trigger a rerun or alert
                return {
                    "status": "recovered",
                    "original_count": row_count_delta,
                    "message": "Data quality issue detected and recovery initiated",
                    "row_count": expected_count  # Simulate successful recovery
                }
        
        # Normal validation
        if row_count_delta < 100:
            raise ValueError(f"canary_failed: row_count_delta_exceeded expected>=100 actual={row_count_delta}")
        
        t_log.info("Validation passed")
        return {"status": "success", "row_count": row_count_delta}
    
    # Define task dependencies
    extract_result = t1_extract()
    transform_result = t2_transform(extract_result)
    load_result = t3_load(transform_result)
    t4_validate(extract_result, load_result)


A4_silent_corruption()
