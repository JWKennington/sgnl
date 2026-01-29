"""Tests for sgnl.bin.plot_psd"""

from unittest import mock

import pytest

from sgnl.bin import plot_psd


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_single_filename(self):
        """Test parsing with a single filename."""
        with mock.patch("sys.argv", ["plot_psd", "file1.xml"]):
            options = plot_psd.parse_command_line()
            assert options.filenames == ["file1.xml"]
            assert options.output is None
            assert options.verbose is False

    def test_multiple_filenames(self):
        """Test parsing with multiple filenames."""
        with mock.patch(
            "sys.argv", ["plot_psd", "file1.xml", "file2.xml", "file3.xml"]
        ):
            options = plot_psd.parse_command_line()
            assert options.filenames == ["file1.xml", "file2.xml", "file3.xml"]

    def test_output_option(self):
        """Test parsing with --output option."""
        with mock.patch("sys.argv", ["plot_psd", "-o", "output.png", "file1.xml"]):
            options = plot_psd.parse_command_line()
            assert options.output == "output.png"

    def test_output_long_option(self):
        """Test parsing with --output long option."""
        with mock.patch(
            "sys.argv", ["plot_psd", "--output", "output.png", "file1.xml"]
        ):
            options = plot_psd.parse_command_line()
            assert options.output == "output.png"

    def test_verbose_flag(self):
        """Test parsing with -v flag."""
        with mock.patch("sys.argv", ["plot_psd", "-v", "file1.xml"]):
            options = plot_psd.parse_command_line()
            assert options.verbose is True

    def test_verbose_long_flag(self):
        """Test parsing with --verbose flag."""
        with mock.patch("sys.argv", ["plot_psd", "--verbose", "file1.xml"]):
            options = plot_psd.parse_command_line()
            assert options.verbose is True

    def test_no_filenames_raises_error(self):
        """Test that no filenames raises SystemExit from argparse."""
        with mock.patch("sys.argv", ["plot_psd"]):
            with pytest.raises(SystemExit):
                plot_psd.parse_command_line()

    def test_empty_filenames_raises_value_error(self):
        """Test that empty filenames raises ValueError."""
        mock_options = mock.MagicMock()
        mock_options.filenames = []
        mock_options.output = None
        mock_options.input_cache = None

        with mock.patch("sgnl.bin.plot_psd.ArgumentParser") as mock_parser_class:
            mock_parser = mock.MagicMock()
            mock_parser.parse_args.return_value = mock_options
            mock_parser_class.return_value = mock_parser

            with pytest.raises(ValueError) as exc_info:
                plot_psd.parse_command_line()

            assert "must supply at least one input filename" in str(exc_info.value)

    def test_multiple_files_with_output_raises_error(self):
        """Test that multiple files with --output raises ValueError."""
        with mock.patch(
            "sys.argv", ["plot_psd", "-o", "output.png", "file1.xml", "file2.xml"]
        ):
            with pytest.raises(ValueError) as exc_info:
                plot_psd.parse_command_line()
            assert "must supply only one input file" in str(exc_info.value)


class TestMain:
    """Tests for main function."""

    def test_main_single_file_with_output(self):
        """Test main function with single file and output option."""
        mock_fig = mock.MagicMock()
        mock_psd = mock.MagicMock()

        with (
            mock.patch("sys.argv", ["plot_psd", "-o", "output.png", "file1.xml"]),
            mock.patch(
                "sgnl.bin.plot_psd.read_psd", return_value=mock_psd
            ) as mock_read,
            mock.patch(
                "sgnl.bin.plot_psd.plot_psds", return_value=mock_fig
            ) as mock_plot,
        ):
            plot_psd.main()

            mock_read.assert_called_once_with("file1.xml", verbose=False)
            mock_plot.assert_called_once_with(mock_psd, plot_width=2400)
            mock_fig.savefig.assert_called_once_with("output.png")

    def test_main_single_file_without_output(self):
        """Test main function with single file and no output option."""
        mock_fig = mock.MagicMock()
        mock_psd = mock.MagicMock()

        with (
            mock.patch("sys.argv", ["plot_psd", "file1.xml"]),
            mock.patch("sgnl.bin.plot_psd.read_psd", return_value=mock_psd),
            mock.patch("sgnl.bin.plot_psd.plot_psds", return_value=mock_fig),
        ):
            plot_psd.main()

            # Check savefig was called (output path is constructed with os.path)
            mock_fig.savefig.assert_called_once()
            # Verify the output path ends with .png
            call_args = mock_fig.savefig.call_args[0][0]
            assert call_args.endswith(".png")

    def test_main_multiple_files(self):
        """Test main function with multiple files."""
        mock_fig = mock.MagicMock()
        mock_psd = mock.MagicMock()

        with (
            mock.patch("sys.argv", ["plot_psd", "file1.xml", "file2.xml"]),
            mock.patch("sgnl.bin.plot_psd.read_psd", return_value=mock_psd),
            mock.patch("sgnl.bin.plot_psd.plot_psds", return_value=mock_fig),
        ):
            plot_psd.main()

            assert mock_fig.savefig.call_count == 2

    def test_main_verbose(self):
        """Test main function with verbose flag."""
        mock_fig = mock.MagicMock()
        mock_psd = mock.MagicMock()

        with (
            mock.patch("sys.argv", ["plot_psd", "-v", "-o", "output.png", "file1.xml"]),
            mock.patch(
                "sgnl.bin.plot_psd.read_psd", return_value=mock_psd
            ) as mock_read,
            mock.patch("sgnl.bin.plot_psd.plot_psds", return_value=mock_fig),
        ):
            plot_psd.main()

            mock_read.assert_called_once_with("file1.xml", verbose=True)
