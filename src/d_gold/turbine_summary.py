from utils.logger import log
from utils.configs import get_confs
from utils.reader import read_files
from utils.writer import write_files
from d_gold.transforms import aggregate_daily, highlight_anomalies


@log
def generate_turbine_summary():
    """
    This is a dummy function that simulates the ingestion of turbine data to the 'data/raw/' location
    """
    # Get Config dictionary for process
    gold_configs = get_confs("d_gold", "generate_turbine_summary")
    metadata_confs = gold_configs.get("metadata")
    if metadata_confs:
        gold_configs["input"][0]["db_path"] = metadata_confs[0].get("db_path")
        gold_configs["input"][0]["ingestion_metadata_table"] = metadata_confs[0].get(
            "metadata_table_name"
        )

    # Read all the input batch files
    data_dict = read_files(gold_configs.get("input"))

    # Transform the tables as requesetd for summary stats and anomaly detection
    summary_stats_df = aggregate_daily(data_dict.get("turbine_clean"))
    df_with_anoms = highlight_anomalies(summary_stats_df)
    
    processed_data = {"turbine_summary": df_with_anoms}
    
    # Write Data according to output configs
    write_files(gold_configs.get("output"), processed_data)

    return None
