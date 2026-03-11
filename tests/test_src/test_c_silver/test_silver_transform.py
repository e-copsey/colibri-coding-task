import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from c_silver.transforms import impute_missing


def test_impute_missing_basic():
    sample_data = [
        ("2026-01-01 02:00:00", 1, 10.0, 180.0, 1.1),
        ("2026-01-01 03:00:00", 1, None, None, None),
        ("2026-01-01 04:00:00", 1, 12.0, 200.0, 1.7),
    ]

    expected_data = [
        ("2026-01-01 02:00:00", 1, 10.0, 180.0, 1.1),
        ("2026-01-01 03:00:00", 1, 11.0, 190.0, 1.4),
        ("2026-01-01 04:00:00", 1, 12.0, 200.0, 1.7),
    ]

    input_df = pd.DataFrame(
        sample_data,
        columns=["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    expected = pd.DataFrame(
        expected_data,
        columns=["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    )

    input_df["timestamp"] = pd.to_datetime(input_df["timestamp"])
    expected["timestamp"] = pd.to_datetime(expected["timestamp"])

    result = impute_missing(input_df)

    assert_frame_equal(result, expected, atol=1e-6)