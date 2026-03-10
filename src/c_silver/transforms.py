import pandas as pd

def detect_missing_records(df):

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

    df = df.sort_values(["turbine_id", "timestamp"])

    df["power_output"] = (
        df.groupby("turbine_id")["power_output"]
        .transform(lambda x: x.interpolate(method="linear"))
    )

    return df

def detect_outliers(df):

    stats = df.groupby("turbine_id")["power_output"].agg(["mean", "std"])

    df = df.merge(stats, on="turbine_id")

    df["zscore"] = (df["power_output"] - df["mean"]) / df["std"]

    outliers = df[abs(df["zscore"]) > 2]

    outliers["issue_type"] = "outlier"

    return outliers

def run_silver_dq(data_to_test):
    
    missing, merged = detect_missing_records(data_to_test)

    clean = impute_missing(merged)

    outliers = detect_outliers(clean)

    dq_checked_df = pd.concat([missing, outliers])
    
    return dq_checked_df