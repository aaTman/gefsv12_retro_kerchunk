import pytest
import datetime
from ..kerchunk_zarr import RetrospectivePull  # Adjust the import as necessary


class TestRetrospectivePull:
    def setup_method(self):
        self.retropull = RetrospectivePull(centered_date_range=3)  # Example setup

    @pytest.mark.parametrize(
        "date, expected_patterns",
        [
            (
                datetime.datetime(2023, 1, 15),
                ["0112", "0113", "0114", "0115", "0116", "0117", "0118"],
            ),
            (
                datetime.datetime(2023, 12, 31),
                ["1228", "1229", "1230", "1231", "0101", "0102", "0103"],
            ),
        ],
    )
    def test_date_to_glob_pattern(self, date, expected_patterns):
        result = self.retropull.date_to_glob_pattern(date)
        assert sorted(result) == sorted(expected_patterns)

    def test_date_to_glob_pattern_invalid_date(self):
        with pytest.raises(TypeError):
            self.retropull.date_to_glob_pattern("invalid_date")
