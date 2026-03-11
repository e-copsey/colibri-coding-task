import pandas as pd
from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files,apply_metadata_to_configs
from utils.writer import write_files, load_into_duckdb
from d_gold.transforms import aggregate_daily, highlight_anomalies


@log
def generate_turbine_summary():
    """
    This function generates daily summary statistics for turbine data and highlights anomalies based on the cleaned SILVER data.
    It reads the cleaned data, applies transformations to create summary statistics, identifies anomalies, and writes the results to the GOLD layer
    while also loading it into DuckDB for analytics.
    """
    # Get Config dictionary for process
    gold_configs = get_confs("d_gold", "generate_turbine_summary")
    gold_configs = apply_metadata_to_configs(gold_configs)

    # Read all the input batch files
    data_dict = read_files(gold_configs.get("input"))

    turbine_cleaned = data_dict.get("turbine_clean", pd.DataFrame())
    # Exit here if no new batches to process
    if turbine_cleaned.empty:
        print("INFO: Exiting the pipeline...")
        return None
    
    # Transform the tables as requesetd for summary stats and anomaly detection
    summary_stats_df = aggregate_daily(turbine_cleaned)
    df_with_anoms = highlight_anomalies(summary_stats_df)
    
    processed_data = {"turbine_summary": df_with_anoms}
    
    # Write Data according to output configs
    write_files(gold_configs.get("output"), processed_data)

    return None
