import pandas as pd
import glob
import duckdb
from datetime import datetime
import os
from utils.constants import FILE_FORMATS

def load_into_duckdb(output_configs:list):
    """
    Loads Parquet files from specified directories into DuckDB tables, creating the tables if they do not already exist.
    """

    for table_config in output_configs:
        table_name = table_config.get("table_name")
        file_path = table_config.get("file_path")
        file_format = table_config.get("file_format")
        duck_db = table_config.get("db_path")
        
        if file_format == "parquet" and os.path.isdir(file_path):
            # Get list of parquet files in the directory
            parquet_files = glob.glob(os.path.join(file_path, "*.parquet"))
            
            if parquet_files:  # Only proceed if there are matching parquet files
                con = duckdb.connect(duck_db)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM '{file_path}/*.parquet';")
                con.close()
            else:
                print(f"No parquet files found in {file_path}. Skipping {table_name}.")
        else:
            print(f"Path does not exist or file format is not parquet. Skipping {table_name}.")


def create_path_if_not_exists(path):
    """
    Creates parent directories for a file path or a directory path if they don't exist.
    Does NOT create a file.
    """
    # List of file extensions to detect files
    file_extensions = FILE_FORMATS

    # Extract extension
    ext = os.path.splitext(path)[-1].lower()

    # If path ends with a known file extension, take its parent directory
    if ext in file_extensions:
        dir_path = os.path.dirname(path)  # parent folder
    else:
        dir_path = path  # path is already a directory

    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    # else -> dir already exists or path is empty (current directory), do nothing


def write_files(output_configs: list, data_dict: dict):
    """
    Writes dataframes from the provided dictionary to files in the specified formats, optionally adding a timestamp to filenames and skipping empty dataframes.
    """

    for table_config in output_configs:
        
        dataframe_name = table_config.get("dataframe_name")
        file_path = table_config.get("file_path")
        file_format = table_config.get("file_format")
        if table_config.get("add_timestamp_to_filename", False):
            file_name = f"{dataframe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
        else:
            file_name = f"{dataframe_name}.{file_format}"

        # skip writing if dataframe is empty to avoid creating empty files
        if data_dict[dataframe_name].empty:
            continue

        create_path_if_not_exists(file_path)

        # Writer logic based on file format - exrendable for more formats as needed
        if file_format == "csv":
            print("Data written to", file_path)
            data_dict[dataframe_name].to_csv(file_path, index=False)
        elif file_format == "parquet":
            print("Data written to", file_path + "/" + file_name)
            data_dict[dataframe_name].to_parquet(
                file_path + "/" + file_name,
                index=False,
                engine="pyarrow",
            )

    return data_dict
