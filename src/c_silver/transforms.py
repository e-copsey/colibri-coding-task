import pandas as pd

def detect_missing_records(df):
    """
    Identifies missing hourly records for each turbine by comparing the dataset against a complete timestamp range,
    returning both the missing records and a merged full dataset.
    """
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    turbines = df["turbine_id"].unique()

    full_index = pd.MultiIndex.from_product(
        [
            turbines,
            pd.date_range(df.timestamp.min(), df.timestamp.max(), freq="h")
        ],
        names=["turbine_id", "timestamp"]
    )

    expected = pd.DataFrame(index=full_index).reset_index()

    merged = expected.merge(
        df,
        on=["turbine_id", "timestamp"],
        how="left"
    )

    missing = merged[merged["power_output"].isna()]

    missing["issue_type"] = "missing_record"

    return missing, merged

def impute_missing(df):
    """
    Fills missing wind_speed, wind_direction, and power_output values by performing linear interpolation within each turbine's time series.
    """

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    cols_to_impute = ["wind_speed", "wind_direction", "power_output"]

    for col in cols_to_impute:
        df[col] = (
            df.groupby("turbine_id")[col]
            .transform(lambda x: x.interpolate(method="linear"))
        )

    return df

def detect_outliers(df):
    """
    Detects outlier power_output values for each turbine based on a z-score threshold of 2, labeling them as outliers.
    """
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    stats = df.groupby("turbine_id")["power_output"].agg(["mean", "std"])

    df = df.merge(stats, on="turbine_id")

    df["zscore"] = (df["power_output"] - df["mean"]) / df["std"]

    outliers = df[abs(df["zscore"]) > 2]
    outliers["issue_type"] = "outlier"

    df_no_outliers = df[abs(df["zscore"]) <= 2].copy()

    return outliers, df_no_outliers

def run_silver_dq(data_to_test):
    """
    Performs data quality checks on turbine data by detecting missing records, imputing missing values,
    and identifying outliers, returning a combined dataframe of issues and the cleaned dataset.
    """
    # Detect missing records and impute first
    missing, merged = detect_missing_records(data_to_test)

    clean = impute_missing(merged)
    # Then detect and remove outliers
    outliers, df_no_outliers = detect_outliers(clean)

    # We now need to check for missing records and impute again in case we have removed outliers.
    missing_2, merged_2 = detect_missing_records(df_no_outliers)

    clean_2 = impute_missing(merged_2)

    # output table ready
    df_no_outliers_2 = clean_2.drop(columns=["mean", "std", "zscore"], errors="ignore")

    dq_checked_df = pd.concat([missing, outliers])
    
    return dq_checked_df, df_no_outliers_2