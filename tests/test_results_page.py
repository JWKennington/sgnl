"""Tests for sgnl.bin.results_page"""

import tempfile
from unittest import mock

import pytest

from sgnl.bin import results_page


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_valid_config_schema(self):
        """Test parsing with valid config schema."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                ],
            ):
                args = results_page.parse_command_line()
                assert args.config_schema == f.name

    def test_all_options(self):
        """Test parsing with all options."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                    "--input-db",
                    "test.db",
                    "--input-rank-stat-pdf",
                    "rank_stat.pdf",
                    "--input-likelihood-file",
                    "likelihood1.h5",
                    "--input-likelihood-file",
                    "likelihood2.h5",
                    "--output-html",
                    "output.html",
                    "-v",
                ],
            ):
                args = results_page.parse_command_line()
                assert args.config_schema == f.name
                assert args.input_db == "test.db"
                assert args.input_rank_stat_pdf == "rank_stat.pdf"
                assert args.input_likelihood_file == [
                    "likelihood1.h5",
                    "likelihood2.h5",
                ]
                assert args.output_html == "output.html"
                assert args.verbose is True

    def test_default_output_html(self):
        """Test default output-html value."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                ],
            ):
                args = results_page.parse_command_line()
                assert args.output_html == "plot-sim.html"

    def test_verbose_default_false(self):
        """Test verbose defaults to False."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            f.write(b"config: test")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    f.name,
                ],
            ):
                args = results_page.parse_command_line()
                assert args.verbose is False

    def test_config_schema_not_provided(self):
        """Test that missing config-schema raises AssertionError."""
        with mock.patch(
            "sys.argv",
            [
                "plot-sim",
            ],
        ):
            with pytest.raises(AssertionError):
                results_page.parse_command_line()

    def test_config_schema_file_not_exists(self):
        """Test that non-existent config-schema file raises AssertionError."""
        with mock.patch(
            "sys.argv",
            [
                "plot-sim",
                "-s",
                "/nonexistent/path/config.yaml",
            ],
        ):
            with pytest.raises(AssertionError):
                results_page.parse_command_line()


class TestProcessEvents:
    """Tests for process_events function."""

    def test_empty_events(self):
        """Test processing empty events list."""
        result = results_page.process_events([])
        assert result == []

    def test_default_cols_and_formats(self):
        """Test with default cols and formats (empty)."""
        events = [
            {"event": {"combined_far": 1e-6, "snr": 10.0}},
            {"event": {"combined_far": 1e-5, "snr": 8.0}},
        ]
        result = results_page.process_events(events)
        # With empty cols, no keys are included
        assert result == [{}, {}]

    def test_with_cols(self):
        """Test with specific cols."""
        events = [
            {"event": {"combined_far": 1e-5, "snr": 8.0, "time": 1000}},
            {"event": {"combined_far": 1e-6, "snr": 10.0, "time": 2000}},
        ]
        result = results_page.process_events(events, cols=["snr", "time"])
        # Should be sorted by combined_far
        assert result == [
            {"snr": 10.0, "time": 2000},
            {"snr": 8.0, "time": 1000},
        ]

    def test_with_formats(self):
        """Test with specific formats."""
        events = [
            {"event": {"combined_far": 1e-6, "snr": 10.12345}},
        ]
        result = results_page.process_events(
            events,
            cols=["snr"],
            formats={"snr": lambda x: "%.2f" % x},
        )
        assert result == [{"snr": "10.12"}]

    def test_sorting_by_far(self):
        """Test events are sorted by combined_far."""
        events = [
            {"event": {"combined_far": 1e-4, "id": 1}},
            {"event": {"combined_far": 1e-7, "id": 2}},
            {"event": {"combined_far": 1e-5, "id": 3}},
        ]
        result = results_page.process_events(events, cols=["id"])
        assert result == [{"id": 2}, {"id": 3}, {"id": 1}]

    def test_limit_n_results(self):
        """Test limiting to n results."""
        events = [
            {"event": {"combined_far": 1e-4, "id": 1}},
            {"event": {"combined_far": 1e-7, "id": 2}},
            {"event": {"combined_far": 1e-5, "id": 3}},
        ]
        result = results_page.process_events(events, n=2, cols=["id"])
        assert len(result) == 2
        assert result == [{"id": 2}, {"id": 3}]


class TestMain:
    """Tests for main function."""

    def test_main_without_likelihood_files(self, tmp_path):
        """Test main function without likelihood files."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("config: test")
        output_html = tmp_path / "output.html"

        mock_indb = mock.MagicMock()
        mock_indb.get_events.return_value = [
            {
                "event": {
                    "time": 1000000000000000000,
                    "network_snr": 10.5,
                    "network_chisq_weighted_snr": 9.8,
                    "likelihood": 15.2,
                    "combined_far": 1e-6,
                }
            }
        ]

        mock_pdf = mock.MagicMock()
        mock_pdf.create_plots.return_value = {
            "IFAR-plot": mock.MagicMock(),
            "LNLR-plot": mock.MagicMock(),
        }

        mock_section = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    str(config_file),
                    "--input-db",
                    "test.db",
                    "--input-rank-stat-pdf",
                    "rank_stat.pdf",
                    "--output-html",
                    str(output_html),
                ],
            ),
            mock.patch("sgnl.bin.results_page.sgnlio.SgnlDB", return_value=mock_indb),
            mock.patch(
                "sgnl.bin.results_page.far.RankingStatPDF.load", return_value=mock_pdf
            ),
            mock.patch("sgnl.bin.results_page.viz.Section", return_value=mock_section),
            mock.patch("sgnl.bin.results_page.viz.b64", return_value="base64_data"),
            mock.patch("sgnl.bin.results_page.viz.page", return_value="<html></html>"),
        ):
            results_page.main()

            # Verify HTML was written
            assert output_html.exists()
            assert output_html.read_text() == "<html></html>"

    def test_main_with_likelihood_files(self, tmp_path):
        """Test main function with likelihood files."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("config: test")
        output_html = tmp_path / "output.html"

        mock_indb = mock.MagicMock()
        mock_indb.get_events.return_value = [
            {
                "event": {
                    "time": 1000000000000000000,
                    "network_snr": 10.5,
                    "network_chisq_weighted_snr": 9.8,
                    "likelihood": 15.2,
                    "combined_far": 1e-6,
                }
            }
        ]

        mock_pdf = mock.MagicMock()
        mock_pdf.create_plots.return_value = {
            "IFAR-plot": mock.MagicMock(),
        }

        mock_lr = mock.MagicMock()
        mock_lr.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        mock_lr.terms["P_of_SNR_chisq"].create_plots.return_value = {
            "H1-SNRCHI2_BACKGROUND_PDF-plot": mock.MagicMock(),
            "L1-SNRCHI2_BACKGROUND_PDF-plot": mock.MagicMock(),
        }

        mock_section = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    str(config_file),
                    "--input-db",
                    "test.db",
                    "--input-rank-stat-pdf",
                    "rank_stat.pdf",
                    "--input-likelihood-file",
                    "likelihood.h5",
                    "--output-html",
                    str(output_html),
                ],
            ),
            mock.patch("sgnl.bin.results_page.sgnlio.SgnlDB", return_value=mock_indb),
            mock.patch(
                "sgnl.bin.results_page.far.RankingStatPDF.load", return_value=mock_pdf
            ),
            mock.patch("sgnl.bin.results_page.LR.load", return_value=mock_lr),
            mock.patch("sgnl.bin.results_page.viz.Section", return_value=mock_section),
            mock.patch("sgnl.bin.results_page.viz.b64", return_value="base64_data"),
            mock.patch("sgnl.bin.results_page.viz.page", return_value="<html></html>"),
        ):
            results_page.main()

            # Verify LR.load was called
            assert mock_lr.finish.called
            assert output_html.exists()

    def test_main_unknown_plot_raises_error(self, tmp_path):
        """Test main function raises ValueError for unknown plot type."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("config: test")
        output_html = tmp_path / "output.html"

        mock_indb = mock.MagicMock()
        mock_indb.get_events.return_value = [
            {
                "event": {
                    "time": 1000000000000000000,
                    "network_snr": 10.5,
                    "network_chisq_weighted_snr": 9.8,
                    "likelihood": 15.2,
                    "combined_far": 1e-6,
                }
            }
        ]

        mock_pdf = mock.MagicMock()
        # Return a plot with unknown type (neither IFAR nor LNLR)
        mock_pdf.create_plots.return_value = {
            "UNKNOWN-plot": mock.MagicMock(),
        }

        mock_section = mock.MagicMock()

        with (
            mock.patch(
                "sys.argv",
                [
                    "plot-sim",
                    "-s",
                    str(config_file),
                    "--input-db",
                    "test.db",
                    "--input-rank-stat-pdf",
                    "rank_stat.pdf",
                    "--output-html",
                    str(output_html),
                ],
            ),
            mock.patch("sgnl.bin.results_page.sgnlio.SgnlDB", return_value=mock_indb),
            mock.patch(
                "sgnl.bin.results_page.far.RankingStatPDF.load", return_value=mock_pdf
            ),
            mock.patch("sgnl.bin.results_page.viz.Section", return_value=mock_section),
            mock.patch("sgnl.bin.results_page.viz.b64", return_value="base64_data"),
        ):
            with pytest.raises(ValueError) as exc_info:
                results_page.main()
            assert "unknown plot" in str(exc_info.value)
