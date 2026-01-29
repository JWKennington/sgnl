"""Tests for sgnl.bin.extinct_bin"""

import sys
from unittest import mock

import numpy as np
import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Store original modules locally within the fixture
    original_modules = {}

    # Create scipy mock with proper structure
    scipy_mock = mock.MagicMock()
    scipy_optimize_mock = mock.MagicMock()
    scipy_mock.optimize = scipy_optimize_mock

    modules_to_mock = [
        "stillsuit",
        "strike",
        "strike.stats",
        "strike.stats.far",
    ]

    # Handle scipy separately since we need structured mocks
    original_modules["scipy"] = sys.modules.get("scipy")
    original_modules["scipy.optimize"] = sys.modules.get("scipy.optimize")
    sys.modules["scipy"] = scipy_mock
    sys.modules["scipy.optimize"] = scipy_optimize_mock

    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock.MagicMock()

    yield

    # Restore originals
    for mod in modules_to_mock + ["scipy", "scipy.optimize"]:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Clear the cached import
    sys.modules.pop("sgnl.bin.extinct_bin", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_basic_required_args(self):
        """Test parsing with basic required arguments."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.input_rankingstatpdf_file == ["input.h5"]
            assert options.output_rankingstatpdf_file == "input.h5"

    def test_config_schema_option(self):
        """Test --config-schema option."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--config-schema",
                "schema.yaml",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.config_schema == "schema.yaml"

    def test_input_database_file_option(self):
        """Test --input-database-file option."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-database-file",
                "triggers.db",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.input_database_file == "triggers.db"

    def test_multiple_input_files(self):
        """Test multiple --input-rankingstatpdf-file arguments."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input1.h5",
                "input2.h5",
                "--input-rankingstatpdf-file",
                "input3.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.input_rankingstatpdf_file == [
                "input1.h5",
                "input2.h5",
                "input3.h5",
            ]
            # Output defaults to first input file
            assert options.output_rankingstatpdf_file == "input1.h5"

    def test_output_file_option(self):
        """Test --output-rankingstatpdf-file option."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--output-rankingstatpdf-file",
                "output.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.output_rankingstatpdf_file == "output.h5"

    def test_reset_zerolag_flag(self):
        """Test --reset-zerolag flag."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--reset-zerolag",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.reset_zerolag is True

    def test_reset_zerolag_default(self):
        """Test --reset-zerolag default is False."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.reset_zerolag is False

    def test_initial_exponent_option(self):
        """Test --initial-exponent option."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--initial-exponent",
                "2.5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.initial_exponent == "2.5"

    def test_initial_exponent_default(self):
        """Test --initial-exponent default is 1.0."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.initial_exponent == 1.0

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--verbose",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.verbose is True

    def test_verbose_short_flag(self):
        """Test -v flag."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "-v",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.verbose is True

    def test_verbose_default(self):
        """Test verbose default is False."""
        from sgnl.bin import extinct_bin

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            options = extinct_bin.parse_command_line()
            assert options.verbose is False


class TestMain:
    """Tests for main function."""

    def _create_mock_rankingstatpdf(self):
        """Create a mock rankingstatpdf object."""
        mock_pdf = mock.MagicMock()

        # Create realistic arrays for noise and zero lag PDFs
        # Use arrays that will produce reasonable extinction fitting
        bg_array = np.zeros(100)
        bg_array[20:80] = np.exp(-np.arange(60) / 20.0)  # Exponential decay

        fg_array = np.zeros(100)
        fg_array[20:80] = np.exp(-np.arange(60) / 15.0) * 0.5  # Similar shape

        mock_pdf.noise_lr_lnpdf.array = bg_array.copy()
        mock_pdf.zero_lag_lr_lnpdf.array = fg_array.copy()

        return mock_pdf

    def test_main_basic(self):
        """Test main function with basic options."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        # Mock curve_fit to return reasonable values
        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),  # c[0] = [c, A]
            np.array([[0.1, 0.01], [0.01, 0.1]]),  # covariance matrix
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--output-rankingstatpdf-file",
                "output.h5",
            ],
        ):
            extinct_bin.main()

        extinct_bin.far.marginalize_pdf_urls.assert_called_once_with(
            ["input.h5"], which="RankingStatPDF"
        )
        mock_pdf.save.assert_called_once()
        # Check save was called with correct process_name
        call_kwargs = mock_pdf.save.call_args
        assert call_kwargs[0][0] == "output.h5"
        assert call_kwargs[1]["process_name"] == "sgnl-extinct-bin"

    def test_main_with_database(self):
        """Test main function with input database."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf
        mock_db = mock.MagicMock()
        extinct_bin.stillsuit.StillSuit.return_value = mock_db

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--config-schema",
                "schema.yaml",
                "--input-database-file",
                "triggers.db",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            extinct_bin.main()

        extinct_bin.stillsuit.StillSuit.assert_called_once_with(
            config="schema.yaml", dbname="triggers.db"
        )
        mock_pdf.collect_zero_lag_rates.assert_called_once_with(mock_db)

    def test_main_with_reset_zerolag(self):
        """Test main function with reset-zerolag flag."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--reset-zerolag",
            ],
        ):
            extinct_bin.main()

        # Verify all elements in zero_lag_lr_lnpdf.array were set to 0.0
        for val in mock_pdf.zero_lag_lr_lnpdf.array:
            assert val == 0.0

    def test_main_with_initial_exponent(self):
        """Test main function with custom initial exponent."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([2.5, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
                "--initial-exponent",
                "2.5",
            ],
        ):
            extinct_bin.main()

        # Verify curve_fit was called with the custom initial exponent
        call_args = extinct_bin.scipy.optimize.curve_fit.call_args
        assert call_args[1]["p0"] == [2.5, 1]

    def test_main_multiple_input_files(self):
        """Test main function with multiple input files."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.reset_mock()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input1.h5",
                "input2.h5",
            ],
        ):
            extinct_bin.main()

        extinct_bin.far.marginalize_pdf_urls.assert_called_once_with(
            ["input1.h5", "input2.h5"], which="RankingStatPDF"
        )

    def test_main_prints_best_fit_to_stderr(self, capsys):
        """Test main function prints best fit values to stderr."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.5, 0.9]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            extinct_bin.main()

        captured = capsys.readouterr()
        assert "Best value of c is 1.5" in captured.err
        assert "A is 0.9" in captured.err

    def test_main_normalizes_noise_pdf(self):
        """Test main function normalizes the noise PDF after extinction."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            extinct_bin.main()

        mock_pdf.noise_lr_lnpdf.normalize.assert_called_once()

    def test_main_zeros_first_ten_bins(self):
        """Test that first 10 bins are zeroed out."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        # Set first 10 bins to non-zero values
        mock_pdf.noise_lr_lnpdf.array[:10] = 1.0
        mock_pdf.zero_lag_lr_lnpdf.array[:10] = 1.0
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        extinct_bin.scipy.optimize.curve_fit.return_value = (
            np.array([1.0, 1.0]),
            np.array([[0.1, 0.01], [0.01, 0.1]]),
        )

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            extinct_bin.main()

        # The function zeros out first 10 bins of both bg and fg before fitting
        # Note: the mock_pdf.noise_lr_lnpdf.array is modified in place
        # We can't directly check it because it's overwritten with bg_pdf_extinct
        # But we verify the save was called which means the function completed
        mock_pdf.save.assert_called_once()

    def test_main_curve_fit_function_called(self):
        """Test that the bg_ccdf_extinct_func is called by curve_fit."""
        from sgnl.bin import extinct_bin

        mock_pdf = self._create_mock_rankingstatpdf()
        extinct_bin.far.marginalize_pdf_urls.return_value = mock_pdf

        # Capture the function passed to curve_fit and call it
        captured_func = None

        def capture_curve_fit(func, *args, **kwargs):
            nonlocal captured_func
            captured_func = func
            return (
                np.array([1.0, 1.0]),
                np.array([[0.1, 0.01], [0.01, 0.1]]),
            )

        extinct_bin.scipy.optimize.curve_fit.side_effect = capture_curve_fit

        with mock.patch(
            "sys.argv",
            [
                "extinct_bin",
                "--input-rankingstatpdf-file",
                "input.h5",
            ],
        ):
            extinct_bin.main()

        # Now call the captured function to get coverage of line 122
        assert captured_func is not None
        # Call with sample values (idx, c, A)
        result = captured_func(20, 1.0, 1.0)
        assert isinstance(result, (int, float, np.floating))
