from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files

@log
def ingest_turbine_data():
    """
    This is a dummy function that simulates the ingestion of turbine data to the 'data/raw/' location
    """
    # Get Config dictionary for process
    bronze_configs = get_confs("b_bronze","ingest_turbine_data")
    
    # Read all the input and metadata tables
    data_dict = read_files(bronze_configs.get("input"))
    ingestion_metadata = read_files(bronze_configs.get("metadata")).get("ingestion_metadata")

    # Perform transformations on data

    # Write Data according to output configs

    return None