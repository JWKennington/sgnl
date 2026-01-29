"""Tests for sgnl.bin.inspiral_svd_bank"""

import sys
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    sys.modules.pop("sgnl.bin.inspiral_svd_bank", None)

    original_modules = {}

    # Create mocks for external dependencies
    lsctables_mock = mock.MagicMock()
    ligolw_utils_mock = mock.MagicMock()
    lal_utils_mock = mock.MagicMock()
    svd_bank_mock = mock.MagicMock()
    psd_mock = mock.MagicMock()

    modules_to_mock = {
        "igwn_ligolw": mock.MagicMock(),
        "igwn_ligolw.lsctables": lsctables_mock,
        "igwn_ligolw.utils": ligolw_utils_mock,
        "lal": mock.MagicMock(),
        "lal.utils": lal_utils_mock,
        "sgnl.svd_bank": svd_bank_mock,
        "sgnl.psd": psd_mock,
    }

    for mod, mock_obj in modules_to_mock.items():
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock_obj

    yield {
        "lsctables": lsctables_mock,
        "ligolw_utils": ligolw_utils_mock,
        "lal_utils": lal_utils_mock,
        "svd_bank": svd_bank_mock,
        "psd": psd_mock,
    }

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    sys.modules.pop("sgnl.bin.inspiral_svd_bank", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_args_only(self):
        """Test parsing with only required arguments."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.reference_psd == "psd.xml"
            assert options.write_svd_bank == "output.xml"
            # Check defaults
            assert options.flow == 40.0
            assert options.sample_rate is None
            assert options.padding == 1.5
            assert options.svd_tolerance == 0.9999
            assert options.autocorrelation_length == 201
            assert options.samples_min == 1024
            assert options.samples_max_256 == 1024
            assert options.samples_max_64 == 2048
            assert options.samples_max == 4096
            assert options.verbose is False

    def test_flow_option(self):
        """Test --flow option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--flow",
                "20.0",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.flow == 20.0

    def test_sample_rate_option(self):
        """Test --sample-rate option with valid power of two."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--sample-rate",
                "4096",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.sample_rate == 4096

    def test_sample_rate_not_power_of_two_raises(self):
        """Test --sample-rate with non-power-of-two raises error."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--sample-rate",
                "3000",
            ],
        ):
            with pytest.raises(ValueError, match="must be a power of two"):
                inspiral_svd_bank.parse_command_line()

    def test_padding_option(self):
        """Test --padding option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--padding",
                "2.0",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.padding == 2.0

    def test_svd_tolerance_option(self):
        """Test --svd-tolerance option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--svd-tolerance",
                "0.999",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.svd_tolerance == 0.999

    def test_instrument_override_option(self):
        """Test --instrument-override option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--instrument-override",
                "H1",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.instrument_override == "H1"

    def test_template_bank_cache_option(self):
        """Test --template-bank-cache option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--template-bank-cache",
                "cache.txt",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.template_bank_cache == "cache.txt"

    def test_verbose_option(self):
        """Test --verbose option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--verbose",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.verbose is True

    def test_verbose_short_option(self):
        """Test -v option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "-v",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.verbose is True

    def test_autocorrelation_length_option(self):
        """Test --autocorrelation-length option with valid odd value."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--autocorrelation-length",
                "301",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.autocorrelation_length == 301

    def test_autocorrelation_length_even_raises(self):
        """Test --autocorrelation-length with even value raises error."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--autocorrelation-length",
                "200",
            ],
        ):
            with pytest.raises(ValueError, match="must be odd"):
                inspiral_svd_bank.parse_command_line()

    def test_samples_min_option(self):
        """Test --samples-min option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--samples-min",
                "512",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.samples_min == 512

    def test_samples_max_256_option(self):
        """Test --samples-max-256 option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--samples-max-256",
                "2048",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.samples_max_256 == 2048

    def test_samples_max_64_option(self):
        """Test --samples-max-64 option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--samples-max-64",
                "4096",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.samples_max_64 == 4096

    def test_samples_max_option(self):
        """Test --samples-max option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--samples-max",
                "8192",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.samples_max == 8192

    def test_max_duration_option(self):
        """Test --max-duration option."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--max-duration",
                "100.0",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.max_duration == 100.0

    def test_template_banks_positional(self):
        """Test --template-banks positional arguments."""
        from sgnl.bin import inspiral_svd_bank

        with mock.patch(
            "sys.argv",
            [
                "inspiral_svd_bank",
                "--reference-psd",
                "psd.xml",
                "--write-svd-bank",
                "output.xml",
                "--template-banks",
                "bank1.xml",
                "bank2.xml",
            ],
        ):
            options = inspiral_svd_bank.parse_command_line()
            assert options.template_banks == ["bank1.xml", "bank2.xml"]


class TestExtractSubbankInfo:
    """Tests for extract_subbank_info function."""

    def test_extract_subbank_info_single_bank(self, mock_dependencies):
        """Test extracting info from a single bank file."""
        from sgnl.bin import inspiral_svd_bank

        # Mock the XML doc and process params
        mock_xmldoc = mock.MagicMock()
        mock_process_params = [
            mock.MagicMock(param="--clipleft", value="10"),
            mock.MagicMock(param="--clipright", value="20"),
            mock.MagicMock(param="--bank-id", value="bank0"),
        ]

        with mock.patch.object(
            inspiral_svd_bank.ligolw_utils, "load_url", return_value=mock_xmldoc
        ):
            with mock.patch.object(
                inspiral_svd_bank.lsctables.ProcessParamsTable,
                "get_table",
                return_value=mock_process_params,
            ):
                cliplefts, cliprights, bank_ids = (
                    inspiral_svd_bank.extract_subbank_info(["bank1.xml"])
                )

        assert cliplefts == ["10"]
        assert cliprights == ["20"]
        assert bank_ids == ["bank0"]

    def test_extract_subbank_info_multiple_banks(self, mock_dependencies):
        """Test extracting info from multiple bank files."""
        from sgnl.bin import inspiral_svd_bank

        # Mock the XML docs and process params for two banks
        mock_xmldoc1 = mock.MagicMock()
        mock_xmldoc2 = mock.MagicMock()
        mock_process_params1 = [
            mock.MagicMock(param="--clipleft", value="10"),
            mock.MagicMock(param="--clipright", value="20"),
            mock.MagicMock(param="--bank-id", value="bank0"),
        ]
        mock_process_params2 = [
            mock.MagicMock(param="--clipleft", value="15"),
            mock.MagicMock(param="--clipright", value="25"),
            mock.MagicMock(param="--bank-id", value="bank1"),
        ]

        with mock.patch.object(
            inspiral_svd_bank.ligolw_utils,
            "load_url",
            side_effect=[mock_xmldoc1, mock_xmldoc2],
        ):
            with mock.patch.object(
                inspiral_svd_bank.lsctables.ProcessParamsTable,
                "get_table",
                side_effect=[mock_process_params1, mock_process_params2],
            ):
                cliplefts, cliprights, bank_ids = (
                    inspiral_svd_bank.extract_subbank_info(["bank1.xml", "bank2.xml"])
                )

        assert cliplefts == ["10", "15"]
        assert cliprights == ["20", "25"]
        assert bank_ids == ["bank0", "bank1"]

    def test_extract_subbank_info_verbose_false(self, mock_dependencies):
        """Test extract_subbank_info with verbose=False."""
        from sgnl.bin import inspiral_svd_bank

        mock_xmldoc = mock.MagicMock()
        mock_process_params = [
            mock.MagicMock(param="--clipleft", value="10"),
            mock.MagicMock(param="--clipright", value="20"),
            mock.MagicMock(param="--bank-id", value="bank0"),
        ]

        with mock.patch.object(
            inspiral_svd_bank.ligolw_utils, "load_url", return_value=mock_xmldoc
        ) as mock_load:
            with mock.patch.object(
                inspiral_svd_bank.lsctables.ProcessParamsTable,
                "get_table",
                return_value=mock_process_params,
            ):
                inspiral_svd_bank.extract_subbank_info(["bank1.xml"], verbose=False)

        # Verify verbose=False was passed
        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args
        assert call_kwargs[1]["verbose"] is False


class MockOptions:
    """Mock class for argparse options that supports __dict__.copy()."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestMain:
    """Tests for main function."""

    def test_main_basic(self, mock_dependencies):
        """Test main function with basic options."""
        from sgnl.bin import inspiral_svd_bank

        # Use a real object instead of MagicMock to support __dict__.copy()
        mock_options = MockOptions(
            template_banks=["bank1.xml"],
            template_bank_cache=None,
            reference_psd="psd.xml",
            write_svd_bank="output.xml",
            flow=40.0,
            max_duration=float("inf"),
            svd_tolerance=0.9999,
            padding=1.5,
            verbose=False,
            autocorrelation_length=201,
            samples_min=1024,
            samples_max_256=1024,
            samples_max_64=2048,
            samples_max=4096,
            sample_rate=None,
            instrument_override=None,
        )

        # Mock extract_subbank_info
        mock_subbank_info = (["10"], ["20"], ["bank0"])

        # Mock read_psd
        mock_psd = mock.MagicMock()

        # Mock build_bank
        mock_bank = mock.MagicMock()

        with mock.patch.object(
            inspiral_svd_bank, "parse_command_line", return_value=mock_options
        ):
            with mock.patch.object(
                inspiral_svd_bank,
                "extract_subbank_info",
                return_value=mock_subbank_info,
            ):
                with mock.patch.object(
                    inspiral_svd_bank, "read_psd", return_value=mock_psd
                ):
                    with mock.patch.object(
                        inspiral_svd_bank.svd_bank,
                        "build_bank",
                        return_value=mock_bank,
                    ):
                        with mock.patch.object(
                            inspiral_svd_bank.svd_bank, "write_bank"
                        ) as mock_write:
                            inspiral_svd_bank.main()

        # Verify write_bank was called
        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[0][0] == "output.xml"
        assert call_args[0][1] == [mock_bank]
        assert call_args[0][2] == mock_psd

    def test_main_with_template_bank_cache(self, mock_dependencies):
        """Test main function with template bank cache."""
        from sgnl.bin import inspiral_svd_bank

        # Use a real object instead of MagicMock
        mock_options = MockOptions(
            template_banks=[],
            template_bank_cache="cache.txt",
            reference_psd="psd.xml",
            write_svd_bank="output.xml",
            flow=40.0,
            max_duration=float("inf"),
            svd_tolerance=0.9999,
            padding=1.5,
            verbose=False,
            autocorrelation_length=201,
            samples_min=1024,
            samples_max_256=1024,
            samples_max_64=2048,
            samples_max=4096,
            sample_rate=None,
            instrument_override=None,
        )

        # Mock CacheEntry
        mock_cache_entry = mock.MagicMock()
        mock_cache_entry.url = "bank_from_cache.xml"

        # Mock extract_subbank_info
        mock_subbank_info = (["10"], ["20"], ["bank0"])

        # Mock read_psd
        mock_psd = mock.MagicMock()

        # Mock build_bank
        mock_bank = mock.MagicMock()

        with mock.patch.object(
            inspiral_svd_bank, "parse_command_line", return_value=mock_options
        ):
            with mock.patch("builtins.open", mock.mock_open(read_data="cache_line\n")):
                with mock.patch.object(
                    inspiral_svd_bank, "CacheEntry", return_value=mock_cache_entry
                ):
                    with mock.patch.object(
                        inspiral_svd_bank,
                        "extract_subbank_info",
                        return_value=mock_subbank_info,
                    ):
                        with mock.patch.object(
                            inspiral_svd_bank, "read_psd", return_value=mock_psd
                        ):
                            with mock.patch.object(
                                inspiral_svd_bank.svd_bank,
                                "build_bank",
                                return_value=mock_bank,
                            ):
                                with mock.patch.object(
                                    inspiral_svd_bank.svd_bank, "write_bank"
                                ):
                                    inspiral_svd_bank.main()

        # Verify template_banks was extended
        assert "bank_from_cache.xml" in mock_options.template_banks

    def test_main_with_multiple_banks(self, mock_dependencies):
        """Test main function with multiple template banks."""
        from sgnl.bin import inspiral_svd_bank

        # Use a real object instead of MagicMock
        mock_options = MockOptions(
            template_banks=["bank1.xml", "bank2.xml"],
            template_bank_cache=None,
            reference_psd="psd.xml",
            write_svd_bank="output.xml",
            flow=40.0,
            max_duration=float("inf"),
            svd_tolerance=0.9999,
            padding=1.5,
            verbose=True,
            autocorrelation_length=201,
            samples_min=1024,
            samples_max_256=1024,
            samples_max_64=2048,
            samples_max=4096,
            sample_rate=4096,
            instrument_override="H1",
        )

        # Mock extract_subbank_info
        mock_subbank_info = (["10", "15"], ["20", "25"], ["bank0", "bank1"])

        # Mock read_psd
        mock_psd = mock.MagicMock()

        # Mock build_bank
        mock_bank1 = mock.MagicMock()
        mock_bank2 = mock.MagicMock()

        with mock.patch.object(
            inspiral_svd_bank, "parse_command_line", return_value=mock_options
        ):
            with mock.patch.object(
                inspiral_svd_bank,
                "extract_subbank_info",
                return_value=mock_subbank_info,
            ):
                with mock.patch.object(
                    inspiral_svd_bank, "read_psd", return_value=mock_psd
                ):
                    with mock.patch.object(
                        inspiral_svd_bank.svd_bank,
                        "build_bank",
                        side_effect=[mock_bank1, mock_bank2],
                    ) as mock_build:
                        with mock.patch.object(
                            inspiral_svd_bank.svd_bank, "write_bank"
                        ) as mock_write:
                            inspiral_svd_bank.main()

        # Verify build_bank was called twice
        assert mock_build.call_count == 2

        # Verify write_bank received both banks
        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[0][1] == [mock_bank1, mock_bank2]
