import pytest
from ..kerchunk_zarr import RetrospectivePull


def test_fhour_to_message_num_valid():
    """Test valid forecast hours"""
    test_cases = [
        (3, 0),  # fhour 3 -> message 0
        (6, 1),  # fhour 6 -> message 1
        (9, 2),  # fhour 9 -> message 2
        (12, 3),  # fhour 12 -> message 3
    ]

    for fhour, expected in test_cases:
        retro = RetrospectivePull(fhour=fhour)
        assert retro.fhour_to_message_num() == expected


def test_fhour_to_message_num_zero():
    """Test that hour 0 raises assertion error"""
    with pytest.raises(AssertionError, match="No hour 0 forecast available"):
        retro = RetrospectivePull(fhour=0)
        retro.fhour_to_message_num()


def test_fhour_to_message_num_non_multiple():
    """Test that non-multiple of 3 raises assertion error"""
    with pytest.raises(AssertionError, match="Forecast hour must be divisible by 3"):
        retro = RetrospectivePull(fhour=4)
        retro.fhour_to_message_num()
