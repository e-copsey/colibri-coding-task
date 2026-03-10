import pandas as pd


def aggregate_daily(cleaned_df):

    cleaned_df["timestamp"] = pd.to_datetime(cleaned_df["timestamp"])

    cleaned_df = cleaned_df.set_index("timestamp")

    summary_df = (
        cleaned_df.groupby("turbine_id")
        .resample("D")["power_output"]
        .agg(["min", "max", "mean"])
        .reset_index()
    )

    summary_df = summary_df.rename(
        columns={
            "timestamp": "date",
            "min": "turbine_power_min",
            "max": "turbine_power_max",
            "mean": "turbine_power_avg",
        }
    )

    return summary_df


def highlight_anomalies(summary_df):
    """
    Function that highlights anomalies based on the rule:
        Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
    """
    # Fleet daily average (across all turbines)
    summary_df["fleet_avg"] = summary_df.groupby("date")["turbine_power_avg"].transform(
        "mean"
    )

    # 2 * standard deviation across turbines for that day
    summary_df["two_std_dev"] = (
        summary_df.groupby("date")["turbine_power_avg"].transform("std") * 2
    )

    # Difference between turbine daily avg and fleet mean
    summary_df["turbine_minus_fleet_mean"] = (
        summary_df["turbine_power_avg"] - summary_df["fleet_avg"]
    )
    summary_df["is_anomaly"] = (
        summary_df["turbine_minus_fleet_mean"].abs() > summary_df["two_std_dev"]
    )

    summary_with_anom = summary_df.sort_values(["date", "turbine_id"])

    return summary_with_anom[[
        "date",
        "turbine_id",
        "turbine_power_min",
        "turbine_power_max",
        "turbine_power_avg",
        "fleet_avg",
        "is_anomaly",
    ]]
