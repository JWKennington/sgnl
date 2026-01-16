"""Unit test for inspiral pipeline"""

from unittest import mock

import pytest
from sgnligo.sources import DataSourceInfo
from sgnligo.transforms import ConditionInfo

from sgnl.bin import reference_psd


class TestReferencePsd:
    """Unit test for inspiral pipeline"""

    def test_reference_psd(self):
        """Test reference_psd pipeline"""
        data_source_info = DataSourceInfo(
            data_source="white",
            channel_name=["H1=FAKE", "L1=FAKE", "V1=FAKE"],
            input_sample_rate=1024,
            gps_start_time=0,
            gps_end_time=10,
        )

        condition_info = ConditionInfo(
            psd_fft_length=4,
            whiten_sample_rate=512,
        )

        reference_psd.reference_psd(
            data_source_info=data_source_info,
            condition_info=condition_info,
        )

    def test_reference_psd_with_whitened_output(self, tmp_path):
        """Test reference_psd with whitened data output."""
        data_source_info = DataSourceInfo(
            data_source="white",
            channel_name=["H1=FAKE"],
            input_sample_rate=1024,
            gps_start_time=0,
            gps_end_time=10,
        )

        condition_info = ConditionInfo(
            psd_fft_length=4,
            whiten_sample_rate=512,
        )

        whitened_output = str(tmp_path / "whitened.dat")

        reference_psd.reference_psd(
            data_source_info=data_source_info,
            condition_info=condition_info,
            whitened_data_output_name=whitened_output,
            verbose=True,
        )


@pytest.fixture
def mock_external_options():
    """Mock external classes to avoid test pollution from inspiral tests.

    The DataSourceInfo and ConditionInfo classes have global state that gets
    polluted by other tests (particularly inspiral tests). By mocking their
    append_options methods, we can test parse_command_line independently.
    """
    with (
        mock.patch.object(
            reference_psd.DataSourceInfo, "append_options", lambda parser: None
        ),
        mock.patch.object(
            reference_psd.ConditionInfo, "append_options", lambda parser: None
        ),
    ):
        yield


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_output_name(self, mock_external_options):
        """Test parsing with required --output-name option."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.output_name == "output.xml.gz"

    def test_verbose_flag(self, mock_external_options):
        """Test parsing with verbose flag."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
                "-v",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.verbose is True

    def test_verbose_long_flag(self, mock_external_options):
        """Test parsing with --verbose long flag."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
                "--verbose",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.verbose is True

    def test_verbose_default_false(self, mock_external_options):
        """Test verbose defaults to False."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.verbose is False

    def test_search_option_ew(self, mock_external_options):
        """Test parsing with --search ew option."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
                "--search",
                "ew",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.search == "ew"

    def test_search_default_none(self, mock_external_options):
        """Test search defaults to None."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.search is None

    def test_whitened_data_output_name(self, mock_external_options):
        """Test parsing with --whitened-data-output-name option."""
        with mock.patch(
            "sys.argv",
            [
                "reference_psd",
                "--output-name",
                "output.xml.gz",
                "--whitened-data-output-name",
                "whitened.dat",
            ],
        ):
            options = reference_psd.parse_command_line()
            assert options.whitened_data_output_name == "whitened.dat"


class TestMain:
    """Tests for main function."""

    def test_main_default_config(self, mock_external_options):
        """Test main function with default config (no search option)."""
        mock_data_source_info = mock.MagicMock()
        mock_condition_info = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "reference_psd",
                    "--output-name",
                    "output.xml.gz",
                ],
            ),
            mock.patch.object(
                reference_psd.DataSourceInfo,
                "from_options",
                return_value=mock_data_source_info,
            ),
            mock.patch.object(
                reference_psd.ConditionInfo,
                "from_options",
                return_value=mock_condition_info,
            ),
            mock.patch.object(reference_psd, "reference_psd") as mock_ref_psd,
        ):
            reference_psd.main()

            mock_ref_psd.assert_called_once()
            call_kwargs = mock_ref_psd.call_args[1]
            assert call_kwargs["data_source_info"] == mock_data_source_info
            assert call_kwargs["condition_info"] == mock_condition_info
            assert call_kwargs["output_name"] == "output.xml.gz"
            assert call_kwargs["verbose"] is False

    def test_main_with_search_option(self, mock_external_options):
        """Test main function with search option (ew)."""
        mock_data_source_info = mock.MagicMock()
        mock_condition_info = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "reference_psd",
                    "--output-name",
                    "output.xml.gz",
                    "--search",
                    "ew",
                    "--whitened-data-output-name",
                    "/path/to/whitened.dat",
                    "-v",
                ],
            ),
            mock.patch.object(
                reference_psd.DataSourceInfo,
                "from_options",
                return_value=mock_data_source_info,
            ),
            mock.patch.object(
                reference_psd.ConditionInfo,
                "from_options",
                return_value=mock_condition_info,
            ),
            mock.patch.object(reference_psd, "reference_psd") as mock_ref_psd,
        ):
            reference_psd.main()

            mock_ref_psd.assert_called_once()
            call_kwargs = mock_ref_psd.call_args[1]
            assert call_kwargs["data_source_info"] == mock_data_source_info
            assert call_kwargs["condition_info"] == mock_condition_info
            assert call_kwargs["output_name"] == "output.xml.gz"
            assert call_kwargs["whitened_data_output_name"] == "/path/to/whitened.dat"
            assert call_kwargs["verbose"] is True
