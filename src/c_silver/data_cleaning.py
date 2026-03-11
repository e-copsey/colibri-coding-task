import pandas as pd
from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files, apply_metadata_to_configs
from utils.writer import write_files, load_into_duckdb
from c_silver.transforms import run_silver_dq


@log
def data_cleaning():
    """
    This function generates clean turbine data and also outputs a data quality report for the BRONZE data.
    Missing values are filled by linear interpolation, and outliers are removed based on a simple IQR method.
    The cleaned data and the data quality report are then written to the SILVER layer and loaded into DuckDB for analytics.
    """
    # Get Config dictionary for process
    silver_configs = get_confs("c_silver", "data_cleaning")
    silver_configs = apply_metadata_to_configs(silver_configs)

    # Read all the input batch files
    data_dict = read_files(silver_configs.get("input"))
    turbine_cleaned = data_dict.get("bronze_data", pd.DataFrame())

    # Exit here if no new batches to process
    if turbine_cleaned.empty:
        print("INFO: Exiting the pipeline...")
        return None

    dq_report, cleaned_data = run_silver_dq(turbine_cleaned)
    processed_data = {
        "turbine_clean": cleaned_data,
        "silver_data_quality_report": dq_report,
    }

    # Write Data according to output configs
    write_files(silver_configs.get("output"), processed_data)

    return None
