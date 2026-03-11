import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from d_gold.transforms import aggregate_daily


def test_aggregate_daily_basic():
    sample_data = [
        ("2026-01-01 02:00:00", 1, 10.0, 180.0, 1.1),
        ("2026-01-01 03:00:00", 1, None, None, None),
        ("2026-01-01 04:00:00", 1, 12.0, 200.0, 1.7),
    ]

    expected_data = [
        (1,"2026-01-01", 1.1, 1.7, 1.4),
    ]

    input_df = pd.DataFrame(
        sample_data,
        columns=["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    expected = pd.DataFrame(
        expected_data,
        columns=["turbine_id", "date", "turbine_power_min", "turbine_power_max", "turbine_power_avg"],
    )

    input_df["timestamp"] = pd.to_datetime(input_df["timestamp"])
    expected["date"] = pd.to_datetime(expected["date"])

    result = aggregate_daily(input_df)

    assert_frame_equal(result, expected, atol=1e-6)