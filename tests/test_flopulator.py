"""Tests for sgnl.bin.flopulator"""

import sys
from unittest import mock

import numpy as np
import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Clear any cached flopulator import first so it will be reimported with mocks
    sys.modules.pop("sgnl.bin.flopulator", None)

    # Store original modules locally within the fixture
    original_modules = {}
    modules_to_mock = ["h5py", "sgnl.svd_bank"]

    # Store original sgnl.svd_bank attribute if sgnl is already imported
    sgnl_mod = sys.modules.get("sgnl")
    original_sgnl_svd_bank = getattr(sgnl_mod, "svd_bank", None) if sgnl_mod else None

    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock.MagicMock()

    # Also patch the sgnl module's svd_bank attribute if sgnl is imported
    if sgnl_mod is not None:
        sgnl_mod.svd_bank = sys.modules["sgnl.svd_bank"]

    yield

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Restore sgnl.svd_bank attribute
    if sgnl_mod is not None and original_sgnl_svd_bank is not None:
        sgnl_mod.svd_bank = original_sgnl_svd_bank

    # Clear the cached import of flopulator so it can be reimported fresh
    sys.modules.pop("sgnl.bin.flopulator", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_args(self):
        """Test parsing with required arguments."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.svd_bank == ["bank.xml"]
            assert options.output_path == "output.h5"

    def test_multiple_svd_banks(self):
        """Test parsing with multiple SVD banks."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank1.xml",
                "bank2.xml",
                "bank3.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.svd_bank == ["bank1.xml", "bank2.xml", "bank3.xml"]

    def test_memory_flag(self):
        """Test --memory flag."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
                "--memory",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.memory is True

    def test_memory_default(self):
        """Test --memory default is False."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.memory is False

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
                "--verbose",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.verbose is True

    def test_verbose_default(self):
        """Test --verbose default is False."""
        from sgnl.bin import flopulator

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            options = flopulator.parse_command_line()
            assert options.verbose is False


class TestFlopulator:
    """Tests for flopulator function."""

    def test_basic_calculation(self):
        """Test basic MFLOPS calculation."""
        from sgnl.bin import flopulator

        r = np.array([256, 512, 1024, 2048])
        rT = np.array([256, 512, 1024, 2048])
        UT = np.array([10, 20, 30, 40])
        MT = 100
        NT = np.array([1000, 500, 250, 125])

        result = flopulator.flopulator(r, rT, UT, MT, NT, verbose=False)

        assert isinstance(result, (int, float, np.floating))
        assert result > 0

    def test_with_verbose(self, capsys):
        """Test flopulator with verbose output."""
        from sgnl.bin import flopulator

        r = np.array([256, 512, 1024, 2048])
        rT = np.array([256, 512, 1024, 2048])
        UT = np.array([10, 20, 30, 40])
        MT = 100
        NT = np.array([1000, 500, 250, 125])

        flopulator.flopulator(r, rT, UT, MT, NT, verbose=True)

        captured = capsys.readouterr()
        assert "MFLOPS from resampling" in captured.out
        assert "MFLOPS from filtering" in captured.out
        assert "MFLOPS from reconstruction" in captured.out
        assert "MFLOPS from addition" in captured.out
        assert "Total MFLOPS" in captured.out
        assert "MFLOPS per complex template" in captured.out
        assert "Ratio number SVD filters" in captured.out

    def test_single_rate(self):
        """Test with single sample rate."""
        from sgnl.bin import flopulator

        r = np.array([1024])
        rT = np.array([1024])
        UT = np.array([50])
        MT = 200
        NT = np.array([500])

        mflops = flopulator.flopulator(r, rT, UT, MT, NT, verbose=False)

        assert isinstance(mflops, (int, float, np.floating))


class TestMemulator:
    """Tests for memulator function."""

    def test_basic_calculation(self):
        """Test basic MB/s calculation."""
        from sgnl.bin import flopulator

        r = np.array([256, 512, 1024, 2048])
        rT = np.array([256, 512, 1024, 2048])
        UT = np.array([10, 20, 30, 40])
        MT = 100
        NT = np.array([1000, 500, 250, 125])

        result = flopulator.memulator(r, rT, UT, MT, NT, verbose=False)

        assert isinstance(result, (int, float, np.floating))
        assert result > 0

    def test_with_verbose(self, capsys):
        """Test memulator with verbose output."""
        from sgnl.bin import flopulator

        r = np.array([256, 512, 1024, 2048])
        rT = np.array([256, 512, 1024, 2048])
        UT = np.array([10, 20, 30, 40])
        MT = 100
        NT = np.array([1000, 500, 250, 125])

        flopulator.memulator(r, rT, UT, MT, NT, verbose=True)

        captured = capsys.readouterr()
        assert "MB/s from resampling" in captured.out
        assert "MB/s from filtering" in captured.out
        assert "MB/s from reconstruction" in captured.out
        assert "MB/s from addition" in captured.out
        assert "Total MB/s" in captured.out
        assert "MB/s per complex template" in captured.out
        assert "Ratio number SVD filters" in captured.out

    def test_single_rate(self):
        """Test with single sample rate."""
        from sgnl.bin import flopulator

        r = np.array([1024])
        rT = np.array([1024])
        UT = np.array([50])
        MT = 200
        NT = np.array([500])

        mbs = flopulator.memulator(r, rT, UT, MT, NT, verbose=False)

        assert isinstance(mbs, (int, float, np.floating))


class TestMain:
    """Tests for main function."""

    def _create_mock_bank_fragment(self, rate, start, end, mix_shape):
        """Create a mock bank fragment."""
        fragment = mock.MagicMock()
        fragment.rate = rate
        fragment.start = start
        fragment.end = end
        fragment.mix_matrix = mock.MagicMock()
        fragment.mix_matrix.shape = mix_shape
        return fragment

    def _create_mock_bank(self, bank_id, fragments):
        """Create a mock bank."""
        bank = mock.MagicMock()
        bank.bank_id = bank_id
        bank.bank_fragments = fragments
        return bank

    def test_main_basic(self, capsys):
        """Test main function with basic options."""
        from sgnl.bin import flopulator

        # Create mock bank fragments
        fragments = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
            self._create_mock_bank_fragment(512, 0.0, 0.5, (20, 100)),
        ]
        mock_bank = self._create_mock_bank("0_test", fragments)
        flopulator.svd_bank.read_banks.return_value = [mock_bank]

        # Create mock h5py file
        mock_hf = mock.MagicMock()
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            flopulator.main()

        flopulator.h5py.File.assert_called_once_with("output.h5", "a")
        flopulator.svd_bank.read_banks.assert_called_once()
        mock_hf.create_group.assert_called()
        mock_hf.close.assert_called_once()

        captured = capsys.readouterr()
        assert "SVD BIN: 0" in captured.out
        assert "Total MFLOPs" in captured.out

    def test_main_with_verbose(self, capsys):
        """Test main function with verbose output."""
        from sgnl.bin import flopulator

        fragments = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
            self._create_mock_bank_fragment(512, 0.0, 0.5, (20, 100)),
        ]
        mock_bank = self._create_mock_bank("0_test", fragments)
        flopulator.svd_bank.read_banks.return_value = [mock_bank]

        mock_hf = mock.MagicMock()
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
                "--verbose",
            ],
        ):
            flopulator.main()

        captured = capsys.readouterr()
        assert "SUB BANK 0" in captured.out
        assert "Unique sampling rates" in captured.out
        assert "Sampling rate for a given time slice" in captured.out
        assert "Total SVD filters" in captured.out
        assert "Number of SVD filter samples" in captured.out
        assert "Total real templates" in captured.out

    def test_main_with_memory(self, capsys):
        """Test main function with memory calculation."""
        from sgnl.bin import flopulator

        fragments = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
            self._create_mock_bank_fragment(512, 0.0, 0.5, (20, 100)),
        ]
        mock_bank = self._create_mock_bank("0_test", fragments)
        flopulator.svd_bank.read_banks.return_value = [mock_bank]

        mock_hf = mock.MagicMock()
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
                "--memory",
            ],
        ):
            flopulator.main()

        captured = capsys.readouterr()
        assert "Total MB/s" in captured.out

    def test_main_multiple_banks(self, capsys):
        """Test main function with multiple SVD banks."""
        from sgnl.bin import flopulator

        fragments1 = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
        ]
        mock_bank1 = self._create_mock_bank("0_test", fragments1)

        fragments2 = [
            self._create_mock_bank_fragment(512, 0.0, 0.5, (20, 100)),
        ]
        mock_bank2 = self._create_mock_bank("1_test", fragments2)

        # Reset mock call count and set up side_effect
        flopulator.svd_bank.read_banks.reset_mock()
        flopulator.svd_bank.read_banks.side_effect = [[mock_bank1], [mock_bank2]]

        mock_hf = mock.MagicMock()
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank1.xml",
                "bank2.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            flopulator.main()

        assert flopulator.svd_bank.read_banks.call_count == 2

    def test_main_multiple_subbanks(self, capsys):
        """Test main function with multiple sub-banks."""
        from sgnl.bin import flopulator

        fragments1 = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
        ]
        mock_bank1 = self._create_mock_bank("0_test", fragments1)

        fragments2 = [
            self._create_mock_bank_fragment(512, 0.0, 0.5, (20, 100)),
        ]
        mock_bank2 = self._create_mock_bank("0_test2", fragments2)

        # Return multiple banks for single file (reset side_effect first)
        flopulator.svd_bank.read_banks.side_effect = None
        flopulator.svd_bank.read_banks.return_value = [mock_bank1, mock_bank2]

        mock_hf = mock.MagicMock()
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            flopulator.main()

        # Verify multiple subbank groups were created
        # Should have: "0" group, "0/subbank-0", "0/subbank-1"
        assert mock_hf.create_group.call_count >= 3

    def test_main_creates_datasets(self):
        """Test that main creates expected datasets."""
        from sgnl.bin import flopulator

        fragments = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
        ]
        mock_bank = self._create_mock_bank("0_test", fragments)
        flopulator.svd_bank.read_banks.side_effect = None
        flopulator.svd_bank.read_banks.return_value = [mock_bank]

        mock_hf = mock.MagicMock()
        mock_group = mock.MagicMock()
        mock_hf.create_group.return_value = mock_group
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
            ],
        ):
            flopulator.main()

        # Check that datasets were created
        dataset_names = [
            call[0][0] for call in mock_group.create_dataset.call_args_list
        ]
        assert "sample-rates" in dataset_names
        assert "num-svd-filters" in dataset_names
        assert "num-real-templates" in dataset_names
        assert "total-mflops" in dataset_names

    def test_main_creates_memory_dataset_when_memory_flag(self):
        """Test that main creates memory dataset when --memory is set."""
        from sgnl.bin import flopulator

        fragments = [
            self._create_mock_bank_fragment(256, 0.0, 1.0, (10, 100)),
        ]
        mock_bank = self._create_mock_bank("0_test", fragments)
        flopulator.svd_bank.read_banks.side_effect = None
        flopulator.svd_bank.read_banks.return_value = [mock_bank]

        mock_hf = mock.MagicMock()
        mock_group = mock.MagicMock()
        mock_hf.create_group.return_value = mock_group
        flopulator.h5py.File.return_value = mock_hf

        with mock.patch(
            "sys.argv",
            [
                "flopulator",
                "--svd-bank",
                "bank.xml",
                "--output-path",
                "output.h5",
                "--memory",
            ],
        ):
            flopulator.main()

        # Check that total-mbs dataset was created
        dataset_names = [
            call[0][0] for call in mock_group.create_dataset.call_args_list
        ]
        assert "total-mbs" in dataset_names
