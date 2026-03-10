import pandas as pd

def get_batch_turbine_data(sensor_df, metadata_df):

    # Ensure timestamps are datetime
    sensor_df["timestamp"] = pd.to_datetime(sensor_df["timestamp"])
    metadata_df["last_timestamp_ingested"] = pd.to_datetime(metadata_df["last_timestamp_ingested"])

    # Merge metadata onto sensor data
    merged = sensor_df.merge(
        metadata_df,
        on=["source_file", "turbine_id"],
        how="left"
    )

    # Filter only new records
    new_records = merged[
        merged["timestamp"] > merged["last_timestamp_ingested"]
    ].copy()

    # Drop metadata column after filtering
    new_records = new_records[sensor_df.columns]

    # ---------------------------
    # Update ingestion metadata
    # ---------------------------

    if not new_records.empty:

        # Get latest timestamp per turbine + source
        latest = (
            new_records.groupby(["source_file", "turbine_id"])["timestamp"]
            .max()
            .reset_index()
            .rename(columns={"timestamp": "last_timestamp_ingested"})
        )

        # Update metadata
        metadata_df = metadata_df.merge(
            latest,
            on=["source_file", "turbine_id"],
            how="left",
            suffixes=("", "_new")
        )

        metadata_df["last_timestamp_ingested"] = metadata_df[
            "last_timestamp_ingested_new"
        ].combine_first(metadata_df["last_timestamp_ingested"])

        metadata_df = metadata_df.drop(columns=["last_timestamp_ingested_new"])

    return new_records, metadata_df