"""Tests for sgnl.bin.median_of_psds"""

import tempfile
from unittest import mock

import numpy as np

from sgnl.bin import median_of_psds


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_basic_options(self):
        """Test parsing with basic options."""
        with mock.patch(
            "sys.argv",
            [
                "median_of_psds",
                "--output-name",
                "output.xml.gz",
                "--input-files",
                "file1.xml",
            ],
        ):
            options, filenames = median_of_psds.parse_command_line()
            assert options.output_name == "output.xml.gz"
            assert filenames == ["file1.xml"]

    def test_multiple_input_files(self):
        """Test parsing with multiple input files."""
        with mock.patch(
            "sys.argv",
            [
                "median_of_psds",
                "--output-name",
                "output.xml.gz",
                "--input-files",
                "file1.xml",
                "--input-files",
                "file2.xml",
            ],
        ):
            options, filenames = median_of_psds.parse_command_line()
            assert filenames == ["file1.xml", "file2.xml"]

    def test_verbose_flag(self):
        """Test parsing with verbose flag."""
        with mock.patch(
            "sys.argv",
            [
                "median_of_psds",
                "--output-name",
                "output.xml.gz",
                "--verbose",
                "--input-files",
                "file1.xml",
            ],
        ):
            options, filenames = median_of_psds.parse_command_line()
            assert options.verbose is True

    def test_verbose_default_false(self):
        """Test verbose defaults to False."""
        with mock.patch(
            "sys.argv",
            [
                "median_of_psds",
                "--output-name",
                "output.xml.gz",
                "--input-files",
                "file1.xml",
            ],
        ):
            options, filenames = median_of_psds.parse_command_line()
            assert options.verbose is False

    def test_input_cache_option(self):
        """Test parsing with --input-cache option."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".cache", delete=False) as f:
            f.write("H H1_PSD 1000000000 100 file://localhost/path/to/file1.xml\n")
            f.write("L L1_PSD 1000000000 100 file://localhost/path/to/file2.xml\n")
            f.flush()

            with (
                mock.patch(
                    "sys.argv",
                    [
                        "median_of_psds",
                        "--output-name",
                        "output.xml.gz",
                        "--input-files",
                        "existing.xml",
                        "--input-cache",
                        f.name,
                    ],
                ),
                mock.patch("sgnl.bin.median_of_psds.CacheEntry") as mock_cache_entry,
            ):
                mock_entry1 = mock.MagicMock()
                mock_entry1.url = "file://localhost/path/to/file1.xml"
                mock_entry2 = mock.MagicMock()
                mock_entry2.url = "file://localhost/path/to/file2.xml"
                mock_cache_entry.side_effect = [mock_entry1, mock_entry2]

                options, filenames = median_of_psds.parse_command_line()

                assert options.input_cache == f.name
                assert "existing.xml" in filenames
                assert "file://localhost/path/to/file1.xml" in filenames
                assert "file://localhost/path/to/file2.xml" in filenames


class TestParseAndDumpPsdFiles:
    """Tests for parse_and_dump_psd_files function."""

    def test_parse_and_dump_psd_files(self):
        """Test parse_and_dump_psd_files function."""
        mock_psd1 = mock.MagicMock()
        mock_psd1.data.data = np.array([1.0, 2.0, 3.0])
        mock_psd2 = mock.MagicMock()
        mock_psd2.data.data = np.array([4.0, 5.0, 6.0])

        with mock.patch("sgnl.bin.median_of_psds.read_psd") as mock_read_psd:
            mock_read_psd.side_effect = [
                {"H1": mock_psd1},
                {"H1": mock_psd2},
            ]

            psd_dict, psd_out_dict = median_of_psds.parse_and_dump_psd_files(
                ["file1.xml", "file2.xml"], verbose=False
            )

            assert "H1" in psd_dict
            assert len(psd_dict["H1"]) == 2
            assert "H1" in psd_out_dict

    def test_parse_and_dump_psd_files_multiple_ifos(self):
        """Test parse_and_dump_psd_files with multiple IFOs."""
        mock_psd_h1 = mock.MagicMock()
        mock_psd_h1.data.data = np.array([1.0, 2.0, 3.0])
        mock_psd_l1 = mock.MagicMock()
        mock_psd_l1.data.data = np.array([4.0, 5.0, 6.0])

        with mock.patch("sgnl.bin.median_of_psds.read_psd") as mock_read_psd:
            mock_read_psd.return_value = {"H1": mock_psd_h1, "L1": mock_psd_l1}

            psd_dict, psd_out_dict = median_of_psds.parse_and_dump_psd_files(
                ["file1.xml"], verbose=True
            )

            assert "H1" in psd_dict
            assert "L1" in psd_dict
            mock_read_psd.assert_called_with("file1.xml", True)

    def test_parse_and_dump_psd_files_none_psd(self):
        """Test parse_and_dump_psd_files with None PSD."""
        mock_psd = mock.MagicMock()
        mock_psd.data.data = np.array([1.0, 2.0, 3.0])

        with mock.patch("sgnl.bin.median_of_psds.read_psd") as mock_read_psd:
            mock_read_psd.return_value = {"H1": mock_psd, "L1": None}

            psd_dict, psd_out_dict = median_of_psds.parse_and_dump_psd_files(
                ["file1.xml"], verbose=False
            )

            assert "H1" in psd_dict
            assert "L1" not in psd_dict


class TestComputeMedianPsd:
    """Tests for compute_median_psd function."""

    def test_compute_median_psd(self):
        """Test compute_median_psd function."""
        mock_psd1 = mock.MagicMock()
        mock_psd1.data.data = np.array([1.0, 2.0, 3.0])
        mock_psd2 = mock.MagicMock()
        mock_psd2.data.data = np.array([3.0, 4.0, 5.0])
        mock_psd3 = mock.MagicMock()
        mock_psd3.data.data = np.array([5.0, 6.0, 7.0])

        mock_out_psd = mock.MagicMock()
        mock_out_psd.data.data = np.zeros(3)

        psd_dict = {"H1": [mock_psd1, mock_psd2, mock_psd3]}
        psd_out_dict = {"H1": mock_out_psd}

        result = median_of_psds.compute_median_psd(psd_dict, psd_out_dict)

        # Median of [1,3,5], [2,4,6], [3,5,7] is [3,4,5]
        np.testing.assert_array_equal(result["H1"].data.data, np.array([3.0, 4.0, 5.0]))

    def test_compute_median_psd_multiple_ifos(self):
        """Test compute_median_psd with multiple IFOs."""
        mock_psd_h1_1 = mock.MagicMock()
        mock_psd_h1_1.data.data = np.array([1.0, 2.0])
        mock_psd_h1_2 = mock.MagicMock()
        mock_psd_h1_2.data.data = np.array([3.0, 4.0])

        mock_psd_l1_1 = mock.MagicMock()
        mock_psd_l1_1.data.data = np.array([10.0, 20.0])
        mock_psd_l1_2 = mock.MagicMock()
        mock_psd_l1_2.data.data = np.array([30.0, 40.0])

        mock_out_h1 = mock.MagicMock()
        mock_out_h1.data.data = np.zeros(2)
        mock_out_l1 = mock.MagicMock()
        mock_out_l1.data.data = np.zeros(2)

        psd_dict = {
            "H1": [mock_psd_h1_1, mock_psd_h1_2],
            "L1": [mock_psd_l1_1, mock_psd_l1_2],
        }
        psd_out_dict = {"H1": mock_out_h1, "L1": mock_out_l1}

        result = median_of_psds.compute_median_psd(psd_dict, psd_out_dict)

        # Median of [1,3] is 2, [2,4] is 3
        np.testing.assert_array_equal(result["H1"].data.data, np.array([2.0, 3.0]))
        # Median of [10,30] is 20, [20,40] is 30
        np.testing.assert_array_equal(result["L1"].data.data, np.array([20.0, 30.0]))


class TestMain:
    """Tests for main function."""

    def test_main(self):
        """Test main function."""
        mock_psd = mock.MagicMock()
        mock_psd.data.data = np.array([1.0, 2.0, 3.0])

        with (
            mock.patch(
                "sys.argv",
                [
                    "median_of_psds",
                    "--output-name",
                    "output.xml.gz",
                    "--input-files",
                    "file1.xml",
                ],
            ),
            mock.patch("sgnl.bin.median_of_psds.read_psd") as mock_read_psd,
            mock.patch("sgnl.bin.median_of_psds.write_psd") as mock_write_psd,
        ):
            mock_read_psd.return_value = {"H1": mock_psd}

            median_of_psds.main()

            mock_read_psd.assert_called_once()
            mock_write_psd.assert_called_once()
            # Verify output filename and verbose flag
            call_args = mock_write_psd.call_args
            assert call_args[0][0] == "output.xml.gz"
            assert call_args[1]["verbose"] is False

    def test_main_verbose(self):
        """Test main function with verbose flag."""
        mock_psd = mock.MagicMock()
        mock_psd.data.data = np.array([1.0, 2.0, 3.0])

        with (
            mock.patch(
                "sys.argv",
                [
                    "median_of_psds",
                    "--output-name",
                    "output.xml.gz",
                    "--verbose",
                    "--input-files",
                    "file1.xml",
                ],
            ),
            mock.patch("sgnl.bin.median_of_psds.read_psd") as mock_read_psd,
            mock.patch("sgnl.bin.median_of_psds.write_psd") as mock_write_psd,
        ):
            mock_read_psd.return_value = {"H1": mock_psd}

            median_of_psds.main()

            # Verify verbose flag is True
            call_args = mock_write_psd.call_args
            assert call_args[1]["verbose"] is True
