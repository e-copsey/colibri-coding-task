from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files
from utils.writer import write_files
from c_silver.transforms import run_silver_dq


@log
def data_cleaning():
    """
    This is a dummy function that simulates the ingestion of turbine data to the 'data/raw/' location
    """
    # Get Config dictionary for process
    silver_configs = get_confs("c_silver", "data_cleaning")
    metadata_confs = silver_configs.get("metadata")
    if metadata_confs:
        silver_configs["input"][0]["db_path"] = metadata_confs[0].get("db_path")
        silver_configs["input"][0]["ingestion_metadata_table"] = metadata_confs[0].get(
            "metadata_table_name"
        )

    # Read all the input batch files
    data_dict = read_files(silver_configs.get("input"))
    turbine_cleaned = data_dict.get("bronze_data")

    # Exit here if no new batches to process
    if turbine_cleaned.empty:
        print("INFO: Exiting the pipeline...")
        return None

    processed_data = {
        "turbine_clean": turbine_cleaned,
        "silver_data_quality_report": run_silver_dq(turbine_cleaned),
    }

    # Write Data according to output configs
    write_files(silver_configs.get("output"), processed_data)

    return None
