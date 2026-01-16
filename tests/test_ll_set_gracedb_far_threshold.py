"""Tests for sgnl.bin.ll_set_gracedb_far_threshold"""

import tempfile
from unittest import mock

import pytest

from sgnl.bin import ll_set_gracedb_far_threshold


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_set_far_threshold_option(self):
        """Test parsing with --set-far-threshold option."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--set-far-threshold",
                "1e-6",
                "--analysis-tag",
                "test",
                "file1.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert args.set_far_threshold == 1e-6
            assert args.analysis_tag == "test"
            assert filenames == ["file1.txt"]

    def test_query_far_threshold_option(self):
        """Test parsing with --query-far-threshold option."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--query-far-threshold",
                "--analysis-tag",
                "test",
                "file1.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert args.query_far_threshold is True
            assert args.analysis_tag == "test"
            assert filenames == ["file1.txt"]

    def test_multiple_filenames(self):
        """Test parsing with multiple filenames."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--query-far-threshold",
                "--analysis-tag",
                "test",
                "file1.txt",
                "file2.txt",
                "file3.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert filenames == ["file1.txt", "file2.txt", "file3.txt"]

    def test_verbose_flag(self):
        """Test parsing with verbose flag."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "-v",
                "--query-far-threshold",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert args.verbose is True

    def test_verbose_long_flag(self):
        """Test parsing with --verbose long flag."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--verbose",
                "--query-far-threshold",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert args.verbose is True

    def test_verbose_default_false(self):
        """Test verbose defaults to False."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--query-far-threshold",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_set_gracedb_far_threshold.parse_command_line()
            assert args.verbose is False

    def test_analysis_tag_required(self):
        """Test that --analysis-tag is required."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--query-far-threshold",
                "file.txt",
            ],
        ):
            with pytest.raises(SystemExit):
                ll_set_gracedb_far_threshold.parse_command_line()


class TestGetUrl:
    """Tests for get_url function."""

    def test_get_url_from_file(self):
        """Test reading URL from file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()
            url = ll_set_gracedb_far_threshold.get_url(f.name)
            assert url == "http://localhost:8080"

    def test_get_url_strips_whitespace(self):
        """Test that URL whitespace is stripped."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("  http://localhost:8080  \n")
            f.flush()
            url = ll_set_gracedb_far_threshold.get_url(f.name)
            assert url == "http://localhost:8080"

    def test_get_url_first_line_only(self):
        """Test that only first line is returned."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\nhttp://other:9090\n")
            f.flush()
            url = ll_set_gracedb_far_threshold.get_url(f.name)
            assert url == "http://localhost:8080"


class TestPostFarThreshold:
    """Tests for post_far_threshold function."""

    def test_post_far_threshold_makes_request(self):
        """Test post_far_threshold makes correct POST request."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sgnl.bin.ll_set_gracedb_far_threshold.requests.post"
            ) as mock_post:
                mock_post.return_value = mock.MagicMock(status_code=200)

                result = ll_set_gracedb_far_threshold.post_far_threshold(
                    f.name, 1e-6, "test_tag"
                )

                mock_post.assert_called_once_with(
                    "http://localhost:8080/test_tag/post/gracedb",
                    json={"far-threshold": 1e-6},
                    timeout=10,
                )
                assert result.status_code == 200


class TestQueryFarThreshold:
    """Tests for query_far_threshold function."""

    def test_query_far_threshold_makes_request(self, capsys):
        """Test query_far_threshold makes correct GET request."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sgnl.bin.ll_set_gracedb_far_threshold.requests.get"
            ) as mock_get:
                mock_response = mock.MagicMock()
                mock_response.json.return_value = 1e-6
                mock_get.return_value = mock_response

                ll_set_gracedb_far_threshold.query_far_threshold(f.name, "test_tag")

                mock_get.assert_called_once_with(
                    "http://localhost:8080/test_tag/get/gracedb/far-threshold",
                    timeout=10,
                )

                captured = capsys.readouterr()
                assert "far threshold:" in captured.out
                assert "1e-06" in captured.out


class TestMain:
    """Tests for main function."""

    def test_main_query_far_threshold(self, capsys):
        """Test main with --query-far-threshold option."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--query-far-threshold",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch(
                    "sgnl.bin.ll_set_gracedb_far_threshold.requests.get"
                ) as mock_get:
                    mock_response = mock.MagicMock()
                    mock_response.json.return_value = 1e-6
                    mock_get.return_value = mock_response

                    ll_set_gracedb_far_threshold.main()

                    mock_get.assert_called_once()
                    captured = capsys.readouterr()
                    assert "far threshold:" in captured.out

    def test_main_query_far_threshold_multiple_files(self, capsys):
        """Test main with --query-far-threshold and multiple files."""
        with (
            tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f1,
            tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f2,
        ):
            f1.write("http://localhost:8080\n")
            f1.flush()
            f2.write("http://localhost:9090\n")
            f2.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--query-far-threshold",
                    "--analysis-tag",
                    "test",
                    f1.name,
                    f2.name,
                ],
            ):
                with mock.patch(
                    "sgnl.bin.ll_set_gracedb_far_threshold.requests.get"
                ) as mock_get:
                    mock_response = mock.MagicMock()
                    mock_response.json.return_value = 1e-6
                    mock_get.return_value = mock_response

                    ll_set_gracedb_far_threshold.main()

                    assert mock_get.call_count == 2

    def test_main_set_far_threshold(self):
        """Test main with --set-far-threshold option."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--set-far-threshold",
                    "1e-6",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch(
                    "sgnl.bin.ll_set_gracedb_far_threshold.requests.post"
                ) as mock_post:
                    mock_post.return_value = mock.MagicMock(status_code=200)

                    ll_set_gracedb_far_threshold.main()

                    mock_post.assert_called_once()

    def test_main_set_far_threshold_multiple_files(self):
        """Test main with --set-far-threshold and multiple files."""
        with (
            tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f1,
            tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f2,
        ):
            f1.write("http://localhost:8080\n")
            f1.flush()
            f2.write("http://localhost:9090\n")
            f2.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--set-far-threshold",
                    "1e-6",
                    "--analysis-tag",
                    "test",
                    f1.name,
                    f2.name,
                ],
            ):
                with mock.patch(
                    "sgnl.bin.ll_set_gracedb_far_threshold.requests.post"
                ) as mock_post:
                    mock_post.return_value = mock.MagicMock(status_code=200)

                    ll_set_gracedb_far_threshold.main()

                    assert mock_post.call_count == 2

    def test_main_set_far_threshold_exception(self, capsys):
        """Test main with --set-far-threshold when exception occurs."""
        with mock.patch(
            "sys.argv",
            [
                "ll_set_gracedb_far_threshold",
                "--set-far-threshold",
                "1e-6",
                "--analysis-tag",
                "test",
                "nonexistent_file.txt",
            ],
        ):
            with mock.patch(
                "sgnl.bin.ll_set_gracedb_far_threshold.post_far_threshold",
                side_effect=Exception("Connection error"),
            ):
                ll_set_gracedb_far_threshold.main()

                captured = capsys.readouterr()
                assert "Error when posting far threshold" in captured.out
                assert "Connection error" in captured.out

    def test_main_both_options_raises_error(self):
        """Test main raises error when both options are specified."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--set-far-threshold",
                    "1e-6",
                    "--query-far-threshold",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with pytest.raises(ValueError) as exc_info:
                    ll_set_gracedb_far_threshold.main()

                assert "Improper combination of arguments" in str(exc_info.value)

    def test_main_neither_option_raises_error(self):
        """Test main raises error when neither option is specified."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_set_gracedb_far_threshold",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with pytest.raises(ValueError) as exc_info:
                    ll_set_gracedb_far_threshold.main()

                assert "Improper combination of arguments" in str(exc_info.value)
