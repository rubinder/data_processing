"""Unit tests for volume_check_rs: verify Rust implementation matches Python."""

import pytest
import volume_check_rs


class TestCheckVolume:
    """Test cases matching spark_applications/utils/quality.py:check_volume()."""

    def test_zero_rows_is_always_anomaly(self):
        """Zero rows is an anomaly regardless of history."""
        result = volume_check_rs.check_volume(
            current=0,
            baselines=[100_000, 105_000, 98_000],
        )
        assert result.status == "anomaly"
        assert result.reason == "zero rows landed"
        assert result.current == 0

    def test_no_baseline_returns_no_baseline_status(self):
        """When there's no history, we can't judge."""
        result = volume_check_rs.check_volume(current=100_000, baselines=[])
        assert result.status == "no_baseline"
        assert result.current == 100_000
        assert result.baseline is None
        assert result.ratio is None

    def test_baseline_median_is_zero(self):
        """If all historical values were zero, we can't compare."""
        result = volume_check_rs.check_volume(
            current=100_000,
            baselines=[0, 0, 0],
        )
        assert result.status == "no_baseline"
        assert result.baseline == 0.0
        assert result.reason == "baseline median is zero"

    def test_volume_within_normal_range(self):
        """Current volume within [min_ratio, max_ratio] → ok."""
        result = volume_check_rs.check_volume(
            current=100_000,
            baselines=[95_000, 98_000, 102_000, 101_000],
            min_ratio=0.5,
            max_ratio=2.0,
        )
        assert result.status == "ok"
        assert result.current == 100_000
        # Sorted: [95k, 98k, 101k, 102k], median = (98k + 101k) / 2 = 99.5k
        assert result.baseline == 99_500.0
        assert 1.004 < result.ratio < 1.006  # ~1.005x

    def test_volume_anomaly_below_min_ratio(self):
        """Current volume < min_ratio * baseline → anomaly."""
        result = volume_check_rs.check_volume(
            current=40_000,  # 40% of 100k baseline
            baselines=[100_000, 99_000, 101_000],
            min_ratio=0.5,
            max_ratio=2.0,
        )
        assert result.status == "anomaly"
        assert result.current == 40_000
        assert result.baseline == 100_000.0
        assert result.ratio == 0.4
        assert "below the 50% floor" in result.reason

    def test_volume_anomaly_above_max_ratio(self):
        """Current volume > max_ratio * baseline → anomaly."""
        result = volume_check_rs.check_volume(
            current=300_000,  # 300% of 100k baseline
            baselines=[100_000, 99_000, 101_000],
            min_ratio=0.5,
            max_ratio=2.0,
        )
        assert result.status == "anomaly"
        assert result.current == 300_000
        assert result.baseline == 100_000.0
        assert result.ratio == 3.0
        assert "above the 200% ceiling" in result.reason

    def test_median_of_even_number_of_baselines(self):
        """Median of even-length list is average of middle two."""
        result = volume_check_rs.check_volume(
            current=100_000,
            baselines=[100_000, 200_000],  # median = 150_000
        )
        assert result.baseline == 150_000.0
        assert result.ratio == 100_000 / 150_000

    def test_median_of_odd_number_of_baselines(self):
        """Median of odd-length list is the middle element."""
        result = volume_check_rs.check_volume(
            current=100_000,
            baselines=[50_000, 100_000, 200_000],  # median = 100_000
        )
        assert result.baseline == 100_000.0
        assert result.ratio == 1.0

    def test_custom_ratio_thresholds(self):
        """Custom min/max ratio thresholds work."""
        result = volume_check_rs.check_volume(
            current=80_000,
            baselines=[100_000],
            min_ratio=0.7,  # Tighter threshold
            max_ratio=1.3,
        )
        # 0.8 is still within [0.7, 1.3]
        assert result.status == "ok"
        assert result.ratio == 0.8

    def test_large_dataset_unsorted_baselines(self):
        """Median is computed correctly even with unsorted input."""
        unsorted = [150_000, 90_000, 120_000, 110_000, 95_000]
        # Sorted: [90k, 95k, 110k, 120k, 150k], median = 110k
        result = volume_check_rs.check_volume(current=110_000, baselines=unsorted)
        assert result.baseline == 110_000.0
        assert result.ratio == 1.0
        assert result.status == "ok"

    def test_as_fields_returns_dict_like_structure(self):
        """as_fields() returns dict with logging fields."""
        result = volume_check_rs.check_volume(
            current=100_000,
            baselines=[95_000, 105_000],
        )
        fields = result.as_fields()
        assert "volume_status" in fields
        assert "volume_current" in fields
        assert "volume_baseline" in fields
        assert "volume_ratio" in fields
        assert "volume_reason" in fields
        assert fields["volume_current"] == 100_000
        assert fields["volume_status"] == "ok"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
