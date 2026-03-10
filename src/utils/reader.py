import os

import pandas as pd
from pathlib import Path
import duckdb


def get_unprocessed_files(db_path, root, ingestions_metadata_table):

    all_files = []

    for group_dir in Path(root).iterdir():
        if group_dir.is_dir():
            all_files.extend(group_dir.glob("*.parquet"))
        elif group_dir.is_file() and group_dir.suffix == ".parquet":
            all_files.append(group_dir)

    all_files = [str(f) for f in all_files]
    all_files = list(set(all_files))
    
    with duckdb.connect(db_path) as con:
        processed = con.execute(
            f"""
            SELECT file_path
            FROM metadata.{ingestions_metadata_table}
        """
        ).fetchall()
        processed = set(r[0] for r in processed)

    return [f for f in all_files if f not in processed]


def load_batches(files):

    dfs = []

    for f in files:
        df = pd.read_parquet(f)
        df["file"] = f
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def mark_processed(files, db_path, table_name):

    with duckdb.connect(db_path) as con:

        rows = [(f,) for f in files]

        con.executemany(
            f"""
            INSERT INTO metadata.{table_name}
            (file_path, processed_at)
            VALUES (?, CURRENT_TIMESTAMP)
        """,
            rows,
        )


def read_files(input_configs: list):

    data_dict = {}

    for table_config in input_configs:
        dataframe_name = table_config.get("dataframe_name")
        file_path = table_config.get("file_path")
        file_format = table_config.get("file_format")
        add_source_col = table_config.get("add_source_col", False)
        is_batch = table_config.get("is_batch", False)
        read_options = table_config.get("read_options", {})

        if file_format == "csv":
            data_dict[dataframe_name] = pd.read_csv(file_path, **read_options)
        elif file_format == "excel":
            data_dict[dataframe_name] = pd.read_excel(file_path, **read_options)
        elif file_format == "parquet":
            if is_batch:
                db_path = table_config.get("db_path")
                ingestion_metadata_table = table_config.get("ingestion_metadata_table")
                files = get_unprocessed_files(
                    db_path, file_path, ingestion_metadata_table
                )
                if not files:
                    print("INFO: No new bronze batches")
                    continue
                data_dict[dataframe_name] = load_batches(files)
                mark_processed(files, db_path, ingestion_metadata_table)
            else:
                data_dict[dataframe_name] = pd.read_parquet(file_path, **read_options)

        if add_source_col == True:
            data_dict[dataframe_name]["source_file"] = file_path.split("/")[-1]

    return data_dict
