"""
Common utility functions for Airflow DAGs.
"""


def process_input(value):
    """
    Process input value by stripping whitespace.
    
    Args:
        value: Input value to process
        
    Returns:
        Processed string value
    """
    if value is None:
        return ""
    return str(value).strip()


def parse_data(data):
    """
    Parse and clean data from various sources.
    
    Args:
        data: Input data (can be dict, string, or None)
        
    Returns:
        Cleaned data string
    """
    if data is None:
        return ""
    
    if isinstance(data, dict):
        result = data.get('value')
        if result is None:
            return ""
        return str(result).strip()
    
    return str(data).strip()
