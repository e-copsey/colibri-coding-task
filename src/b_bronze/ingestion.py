from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files
from utils.writer import write_files
from b_bronze.transforms import get_batch_turbine_data


@log
def ingest_turbine_data():
    """
    This is the orchestrating function that ingests the raw turbine data incrementally using batch processing and a duckdb metadata table
    to incrementally process new data.

    
    """
    # Get Config dictionary for process
    bronze_configs = get_confs("b_bronze", "ingest_turbine_data")

    # Read all the input and metadata tables
    data_dict = read_files(bronze_configs.get("input"))
    ingestion_metadata = read_files(bronze_configs.get("metadata")).get("ingestion_metadata")

    # Perform transformations on data
    processed_data = {}
    for df_name, dataframe in data_dict.items():
        processed_data[df_name], ingestion_metadata = get_batch_turbine_data(
            dataframe, ingestion_metadata
        )

    # Add updated metadata to processed data dict for writing
    processed_data["ingestion_metadata"] = ingestion_metadata

    # Write Data according to output configs
    write_files(bronze_configs.get("output"), processed_data)

    return None
