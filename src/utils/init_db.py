import duckdb
from pathlib import Path
from utils.logger import log


@log
def initialise_db():
    """
    Initialises the DuckDB database by creating required schemas and empty metadata tables for tracking processing states if they do not already exist.
    """

    con = duckdb.connect("db/windfarm.duckdb")

    # Create Schemas for metadata and analytics if they don't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
    print("INFO: Created metadata schema if it did not exist")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    print("INFO: Created analytics schema if it did not exist")

    # Create empty tables for ingestion metadata if they don't exist
    # This table refers to the BRONZE tables as inputs to SILVER
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.bronze_processing_state (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    print("INFO: Created bronze processing state table if it did not exist")

    # This table refers to the SILVER tables as inputs to GOLD SUMMARY PIPE
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.silver_processing_state_turbine_summary (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    print("INFO: Created silver processing state table for turbine summary if it did not exist")

    # This table refers to the SILVER tables as inputs to GOLD ANOMALY DETECTION PIPE
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.silver_processing_state_anomaly_detection (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    print("INFO: Created silver processing state table for anomaly detection if it did not exist")

    con.close()


def wipe_pipeline_data(layer=None):
    """
    Deletes all files and directories in the specified pipeline data layer, supporting 'bronze', 'silver', 'gold', or 'all' layers, with recursive cleanup.
    """

    import os
    import shutil

    def _del_recursively(f):
        """
        Recursively deletes all files and folders under the given path `f`.
        """
        if not os.path.exists(f):
            print(f"Path does not exist: {f}")
            return

        for entry in os.listdir(f):
            file_path = os.path.join(f, entry)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Deletes directory and all contents
                print(f"Deleted directory: {file_path}")

    if layer is None or layer == "raw":
        print("INFO: Wipe layer not defined properly")

    if layer == "all":
        folder = ["data/b_bronze/", "data/c_silver/", "data/d_gold/"]
    elif layer == "bronze":
        folder = "data/b_bronze/"
    elif layer == "silver":
        folder = "data/c_silver/"
    elif layer == "gold":
        folder = "data/d_gold/"
    else:
        raise ValueError(
            "Invalid layer specified. Choose from 'raw', 'bronze', 'silver', or 'gold'."
        )

    if isinstance(folder, list):
        for f in folder:
            _del_recursively(f)
    else:
        _del_recursively(folder)


@log
def wipe_db(wipe_data=False, wipe_layer=None):
    """
    Drops the DuckDB metadata and analytics schemas, optionally wipes pipeline data layers, and resets ingestion metadata with default placeholder values.
    """

    import pandas as pd

    con = duckdb.connect("db/windfarm.duckdb")
    con.execute("DROP SCHEMA IF EXISTS metadata CASCADE")
    con.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
    con.close()

    if wipe_data:
        wipe_pipeline_data(wipe_layer)

   
    for data_group in range(1, 4):
        df_base = pd.read_csv(f"data/mock_extra/original/data_group_{data_group}.csv")
        df_base.to_csv(f"data/a_raw/data_group_{data_group}.csv", index=False)
    
    ingestion_marker_data = [
        ("data_group_1.csv", 1, "1900-01-01 23:00:00"),
        ("data_group_1.csv", 2, "1900-01-01 23:00:00"),
        ("data_group_1.csv", 3, "1900-01-01 23:00:00"),
        ("data_group_1.csv", 4, "1900-01-01 23:00:00"),
        ("data_group_1.csv", 5, "1900-01-01 23:00:00"),
        ("data_group_2.csv", 6, "1900-01-01 23:00:00"),
        ("data_group_2.csv", 7, "1900-01-01 23:00:00"),
        ("data_group_2.csv", 8, "1900-01-01 23:00:00"),
        ("data_group_2.csv", 9, "1900-01-01 23:00:00"),
        ("data_group_2.csv", 10, "1900-01-01 23:00:00"),
        ("data_group_3.csv", 11, "1900-01-01 23:00:00"),
        ("data_group_3.csv", 12, "1900-01-01 23:00:00"),
        ("data_group_3.csv", 13, "1900-01-01 23:00:00"),
        ("data_group_3.csv", 14, "1900-01-01 23:00:00"),
        ("data_group_3.csv", 15, "1900-01-01 23:00:00"),
    ]

    # Create the DataFrame
    reset_ingestion_markers = pd.DataFrame(
        ingestion_marker_data,
        columns=["source_file", "turbine_id", "last_timestamp_ingested"],
    )

    reset_ingestion_markers.to_csv("data/b_bronze/ingestion_metadata.csv", index=False)
