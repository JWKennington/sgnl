"""Tests for sgnl.utils module."""

from sgnl.utils import ram


class TestRam:
    """Tests for ram function."""

    def test_ram_returns_numeric(self):
        """Test ram returns a numeric value representing memory usage in MB."""
        result = ram()
        assert isinstance(result, float)
        # Memory usage should be positive
        assert result >= 0

    def test_ram_returns_reasonable_value(self):
        """Test ram returns a reasonable memory value."""
        result = ram()
        # The Python process should use at least some memory (> 0 MB)
        # and less than some reasonable upper bound (< 100 GB)
        assert 0 <= result < 100000
