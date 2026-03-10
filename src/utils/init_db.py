import duckdb


def initialise_db():
    con = duckdb.connect("db/windfarm.duckdb")

    # Create Schemas for metadata and analytics if they don't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # Create empty tables for ingestion metadata if they don't exist
    # This table refers to the BRONZE tables as inputs to SILVER
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.bronze_processing_state (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    # This table refers to the SILVER tables as inputs to GOLD SUMMARY PIPE
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.silver_processing_state_turbine_summary (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    # This table refers to the SILVER tables as inputs to GOLD ANOMALY DETECTION PIPE
    con.execute(
        """CREATE TABLE IF NOT EXISTS metadata.silver_processing_state_anomaly_detection (
        file_path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )

    print("INFO: Following tables have been initialised:")
    print(
        "schema = Metadata\n",
        con.execute(f"SET SCHEMA 'metadata';SHOW TABLES;").fetchdf(),
    )
    print(
        "schema = Analytics\n",
        con.execute(f"SET SCHEMA 'analytics';SHOW TABLES;").fetchdf(),
    )
    con.close()

def wipe_pipeline_data(layer=None):
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
        raise ValueError("Invalid layer specified. Choose from 'raw', 'bronze', 'silver', or 'gold'.")
    
    
    if isinstance(folder, list):
        for f in folder:
            _del_recursively(f)
    else:
        _del_recursively(folder)


    

def wipe_db(wipe_data=False, wipe_layer=None):
    import pandas as pd

    con = duckdb.connect("db/windfarm.duckdb")
    con.execute("DROP SCHEMA IF EXISTS metadata CASCADE")
    con.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
    con.close()

    if wipe_data:
        wipe_pipeline_data(wipe_layer)

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
