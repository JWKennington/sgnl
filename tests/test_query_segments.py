"""Tests for sgnl.bin.query_segments"""

from unittest import mock

import pytest

from sgnl.bin import query_segments


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_dqsegdb_source_with_flag(self):
        """Test parsing with dqsegdb source and valid flag."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.source == "dqsegdb"
            assert args.start == 1000000000
            assert args.end == 1000001000
            assert args.instruments == ["H1"]
            assert args.flags == {"H1": "H1:DMT-ANALYSIS_READY:1"}

    def test_dqsegdb_source_with_multiple_flags(self):
        """Test parsing with dqsegdb source and multiple flags."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
                "-f",
                "L1:DMT-ANALYSIS_READY:1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.instruments == ["H1", "L1"]
            assert args.flags == {
                "H1": "H1:DMT-ANALYSIS_READY:1",
                "L1": "L1:DMT-ANALYSIS_READY:1",
            }

    def test_dqsegdb_source_default(self):
        """Test that dqsegdb is the default source."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.source == "dqsegdb"

    def test_dqsegdb_without_flag_raises_error(self):
        """Test that dqsegdb source without --flag raises SystemExit."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
            ],
        ):
            with pytest.raises(SystemExit):
                query_segments.parse_command_line()

    def test_dqsegdb_invalid_flag_format(self):
        """Test that invalid flag format raises ValueError."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY",  # Missing version
            ],
        ):
            with pytest.raises(ValueError) as exc_info:
                query_segments.parse_command_line()
            assert "must be in format IFO:FLAG:VERSION" in str(exc_info.value)

    def test_dqsegdb_invalid_ifo(self):
        """Test that invalid IFO raises ValueError."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "XX:DMT-ANALYSIS_READY:1",  # Invalid IFO
            ],
        ):
            with pytest.raises(ValueError) as exc_info:
                query_segments.parse_command_line()
            assert "Invalid IFO" in str(exc_info.value)

    def test_dqsegdb_verbose(self, capsys):
        """Test dqsegdb verbose output."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "dqsegdb",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
                "-v",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.verbose is True
            captured = capsys.readouterr()
            assert "Using flags:" in captured.out

    def test_gwosc_source_with_instruments(self):
        """Test parsing with gwosc source and instruments."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "gwosc",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-i",
                "H1",
                "L1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.source == "gwosc"
            assert args.instruments == ["H1", "L1"]
            assert args.flags is None

    def test_gwosc_source_deduplicates_instruments(self):
        """Test that gwosc source deduplicates instruments."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "gwosc",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-i",
                "H1",
                "H1",
                "L1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.instruments == ["H1", "L1"]

    def test_gwosc_without_instruments_raises_error(self):
        """Test that gwosc source without --instruments raises SystemExit."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "gwosc",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
            ],
        ):
            with pytest.raises(SystemExit):
                query_segments.parse_command_line()

    def test_gwosc_with_flag_raises_error(self):
        """Test that gwosc source with --flag raises SystemExit."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "gwosc",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-i",
                "H1",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
            ],
        ):
            with pytest.raises(SystemExit):
                query_segments.parse_command_line()

    def test_server_option(self):
        """Test parsing with --server option."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
                "-u",
                "https://custom.server.com",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.server == "https://custom.server.com"

    def test_segment_name_option(self):
        """Test parsing with --segment-name option."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
                "-n",
                "custom_segments",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.segment_name == "custom_segments"

    def test_output_option(self):
        """Test parsing with --output option."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
                "-o",
                "output.xml.gz",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.output == "output.xml.gz"

    def test_no_cert_option(self):
        """Test parsing with --no-cert option."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "--source",
                "gwosc",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-i",
                "H1",
                "--no-cert",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.no_cert is True

    def test_verbose_default_false(self):
        """Test verbose defaults to False."""
        with mock.patch(
            "sys.argv",
            [
                "query_segments",
                "-s",
                "1000000000",
                "-e",
                "1000001000",
                "-f",
                "H1:DMT-ANALYSIS_READY:1",
            ],
        ):
            args = query_segments.parse_command_line()
            assert args.verbose is False


class TestMain:
    """Tests for main function."""

    def test_main_dqsegdb_source(self):
        """Test main function with dqsegdb source."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "--source",
                    "dqsegdb",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-f",
                    "H1:DMT-ANALYSIS_READY:1",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_dqsegdb_segments",
                return_value=mock_segments,
            ) as mock_query,
            mock.patch("sgnl.bin.query_segments.write_segments") as mock_write,
        ):
            query_segments.main()

            mock_query.assert_called_once_with(
                ["H1"],
                1000000000,
                1000001000,
                {"H1": "H1:DMT-ANALYSIS_READY:1"},
                server=mock.ANY,
            )
            mock_write.assert_called_once()

    def test_main_dqsegdb_source_verbose(self, capsys):
        """Test main function with dqsegdb source and verbose flag."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "--source",
                    "dqsegdb",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-f",
                    "H1:DMT-ANALYSIS_READY:1",
                    "-v",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch("sgnl.bin.query_segments.write_segments"),
        ):
            query_segments.main()

            captured = capsys.readouterr()
            assert "Querying DQSegDB segments" in captured.out
            assert "GPS time range:" in captured.out
            assert "Server:" in captured.out
            assert "Found the following segments:" in captured.out

    def test_main_gwosc_source(self):
        """Test main function with gwosc source."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "--source",
                    "gwosc",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-i",
                    "H1",
                    "L1",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_gwosc_segments",
                return_value=mock_segments,
            ) as mock_query,
            mock.patch("sgnl.bin.query_segments.write_segments") as mock_write,
        ):
            query_segments.main()

            mock_query.assert_called_once_with(
                ["H1", "L1"],
                1000000000,
                1000001000,
                verify_certs=True,
            )
            mock_write.assert_called_once()

    def test_main_gwosc_source_no_cert(self):
        """Test main function with gwosc source and --no-cert."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "--source",
                    "gwosc",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-i",
                    "H1",
                    "--no-cert",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_gwosc_segments",
                return_value=mock_segments,
            ) as mock_query,
            mock.patch("sgnl.bin.query_segments.write_segments"),
        ):
            query_segments.main()

            mock_query.assert_called_once_with(
                ["H1"],
                1000000000,
                1000001000,
                verify_certs=False,
            )

    def test_main_gwosc_source_verbose(self, capsys):
        """Test main function with gwosc source and verbose flag."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "--source",
                    "gwosc",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-i",
                    "H1",
                    "-v",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_gwosc_segments",
                return_value=mock_segments,
            ),
            mock.patch("sgnl.bin.query_segments.write_segments"),
        ):
            query_segments.main()

            captured = capsys.readouterr()
            assert "Querying GWOSC segments" in captured.out
            assert "GPS time range:" in captured.out
            assert "Found the following segments:" in captured.out

    def test_main_writes_segments_with_options(self):
        """Test that main passes correct options to write_segments."""
        mock_segments = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "query_segments",
                    "-s",
                    "1000000000",
                    "-e",
                    "1000001000",
                    "-f",
                    "H1:DMT-ANALYSIS_READY:1",
                    "-o",
                    "custom_output.xml.gz",
                    "-n",
                    "custom_name",
                    "-v",
                ],
            ),
            mock.patch(
                "sgnl.bin.query_segments.query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch("sgnl.bin.query_segments.write_segments") as mock_write,
        ):
            query_segments.main()

            mock_write.assert_called_once_with(
                mock_segments,
                "custom_output.xml.gz",
                segment_name="custom_name",
                verbose=True,
            )
