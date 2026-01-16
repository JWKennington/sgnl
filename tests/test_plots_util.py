"""Tests for sgnl.plots.util module."""

import os
from unittest import mock

import numpy

from sgnl.plots import util as plotutil


class TestSetMatplotlibCacheDirectory:
    """Tests for set_matplotlib_cache_directory function."""

    def test_set_cache_directory_when_condor_scratch_set(self):
        """Test that MPLCONFIGDIR is set when _CONDOR_SCRATCH_DIR exists."""
        with mock.patch.dict(
            os.environ, {"_CONDOR_SCRATCH_DIR": "/tmp/condor_scratch"}, clear=False
        ):
            plotutil.set_matplotlib_cache_directory()
            assert os.environ.get("MPLCONFIGDIR") == "/tmp/condor_scratch"

    def test_set_cache_directory_when_condor_scratch_not_set(self):
        """Test that MPLCONFIGDIR is not set when _CONDOR_SCRATCH_DIR is not set."""
        # Save original value if it exists
        original_mplconfigdir = os.environ.get("MPLCONFIGDIR")

        # Remove _CONDOR_SCRATCH_DIR if it exists
        env_copy = os.environ.copy()
        env_copy.pop("_CONDOR_SCRATCH_DIR", None)

        with mock.patch.dict(os.environ, env_copy, clear=True):
            plotutil.set_matplotlib_cache_directory()
            # MPLCONFIGDIR should not be modified (or set)
            # The function only sets it when _CONDOR_SCRATCH_DIR is set

        # Restore original if it existed
        if original_mplconfigdir is not None:
            os.environ["MPLCONFIGDIR"] = original_mplconfigdir


class TestColourFromInstruments:
    """Tests for colour_from_instruments function."""

    def test_single_instrument_h1(self):
        """Test color for single H1 instrument."""
        colour = plotutil.colour_from_instruments(["H1"])
        assert isinstance(colour, numpy.ndarray)
        assert len(colour) == 3

    def test_single_instrument_l1(self):
        """Test color for single L1 instrument."""
        colour = plotutil.colour_from_instruments(["L1"])
        assert isinstance(colour, numpy.ndarray)
        assert len(colour) == 3

    def test_multiple_instruments_mixed(self):
        """Test color mixing for multiple instruments."""
        colour = plotutil.colour_from_instruments(["H1", "L1"])
        assert isinstance(colour, numpy.ndarray)
        assert len(colour) == 3
        # Color should be normalized (max value should be 1.0)
        assert colour.max() <= 1.0

    def test_three_instruments(self):
        """Test color mixing for three instruments."""
        colour = plotutil.colour_from_instruments(["H1", "L1", "V1"])
        assert isinstance(colour, numpy.ndarray)
        assert len(colour) == 3
        # Color should be normalized
        assert colour.max() <= 1.0

    def test_custom_colours(self):
        """Test with custom color dictionary."""
        custom_colours = {
            "X1": numpy.array([1.0, 0.0, 0.0]),
            "Y1": numpy.array([0.0, 1.0, 0.0]),
        }
        colour = plotutil.colour_from_instruments(["X1"], colours=custom_colours)
        numpy.testing.assert_array_equal(colour, numpy.array([1.0, 0.0, 0.0]))

    def test_custom_colours_multiple(self):
        """Test color mixing with custom colors for multiple instruments."""
        custom_colours = {
            "X1": numpy.array([1.0, 0.0, 0.0]),
            "Y1": numpy.array([0.0, 1.0, 0.0]),
        }
        colour = plotutil.colour_from_instruments(["X1", "Y1"], colours=custom_colours)
        assert isinstance(colour, numpy.ndarray)
        # Should be mixed and normalized
        assert colour.max() <= 1.0


class TestLatexnumber:
    """Tests for latexnumber function."""

    def test_no_exponent_unchanged(self):
        """Test that strings without exponents are unchanged."""
        result = plotutil.latexnumber("123.456")
        assert result == "123.456"

    def test_scientific_notation_lowercase_e(self):
        """Test conversion of scientific notation with lowercase e."""
        result = plotutil.latexnumber("3.14e10")
        assert "times 10^{10}" in result
        assert "3.14" in result

    def test_scientific_notation_uppercase_e(self):
        """Test conversion of scientific notation with uppercase E."""
        result = plotutil.latexnumber("2.5E-5")
        assert "times 10^{-5}" in result
        assert "2.5" in result

    def test_scientific_notation_negative_mantissa(self):
        """Test conversion with negative mantissa."""
        result = plotutil.latexnumber("-1.5e3")
        assert "times 10^{3}" in result
        assert "-1.5" in result

    def test_scientific_notation_positive_sign(self):
        """Test conversion with explicit positive sign."""
        result = plotutil.latexnumber("+2.0e+6")
        assert "times 10^{6}" in result
        assert "+2.0" in result

    def test_malformed_exponent_unchanged(self):
        """Test that malformed exponent strings are unchanged."""
        # Has 'e' but doesn't match the pattern
        result = plotutil.latexnumber("hello_e_world")
        assert result == "hello_e_world"


class TestLatexfilename:
    """Tests for latexfilename function."""

    def test_no_special_chars(self):
        """Test string with no special characters."""
        result = plotutil.latexfilename("simple")
        assert result == "simple"

    def test_underscore_escaped(self):
        """Test that underscores are escaped."""
        result = plotutil.latexfilename("file_name")
        assert result == r"file\_name"

    def test_backslash_escaped(self):
        """Test that backslashes are escaped."""
        result = plotutil.latexfilename(r"path\to\file")
        assert result == r"path\\to\\file"

    def test_space_replaced(self):
        """Test that spaces are replaced with non-breaking space."""
        result = plotutil.latexfilename("file name")
        assert result == "file~name"

    def test_all_special_chars(self):
        """Test string with all special characters."""
        result = plotutil.latexfilename(r"path\to\file_name with spaces")
        assert r"\\" in result
        assert r"\_" in result
        assert "~" in result


class TestGoldenRatio:
    """Tests for golden_ratio constant."""

    def test_golden_ratio_value(self):
        """Test that golden_ratio has the correct value."""
        import math

        expected = (1.0 + math.sqrt(5.0)) / 2.0
        assert plotutil.golden_ratio == expected
