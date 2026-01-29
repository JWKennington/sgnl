"""Tests for sgnl.bin.ll_remove_counts"""

import tempfile
from unittest import mock

from sgnl.bin import ll_remove_counts


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_required_args(self):
        """Test parsing with required arguments."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "remove",
                "--analysis-tag",
                "test",
                "file1.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.gps_time == 1000000000
            assert args.action == "remove"
            assert args.analysis_tag == "test"
            assert filenames == ["file1.txt"]

    def test_multiple_filenames(self):
        """Test parsing with multiple filenames."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "check",
                "--analysis-tag",
                "test",
                "file1.txt",
                "file2.txt",
                "file3.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert filenames == ["file1.txt", "file2.txt", "file3.txt"]

    def test_action_check(self):
        """Test parsing with check action."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "check",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.action == "check"

    def test_action_undo_remove(self):
        """Test parsing with undo-remove action."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "undo-remove",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.action == "undo-remove"

    def test_verbose_flag(self):
        """Test parsing with verbose flag."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "-v",
                "--gps-time",
                "1000000000",
                "--action",
                "remove",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.verbose is True

    def test_verbose_long_flag(self):
        """Test parsing with --verbose long flag."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--verbose",
                "--gps-time",
                "1000000000",
                "--action",
                "remove",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.verbose is True

    def test_verbose_default_false(self):
        """Test verbose defaults to False."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "remove",
                "--analysis-tag",
                "test",
                "file.txt",
            ],
        ):
            args, filenames = ll_remove_counts.parse_command_line()
            assert args.verbose is False


class TestGetUrl:
    """Tests for get_url function."""

    def test_get_url_from_file(self):
        """Test reading URL from file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()
            url = ll_remove_counts.get_url(f.name)
            assert url == "http://localhost:8080"

    def test_get_url_strips_whitespace(self):
        """Test that URL whitespace is stripped."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("  http://localhost:8080  \n")
            f.flush()
            url = ll_remove_counts.get_url(f.name)
            assert url == "http://localhost:8080"

    def test_get_url_first_line_only(self):
        """Test that only first line is returned."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\nhttp://other:9090\n")
            f.flush()
            url = ll_remove_counts.get_url(f.name)
            assert url == "http://localhost:8080"


class TestSubmit:
    """Tests for submit function."""

    def test_submit_makes_post_request(self):
        """Test submit makes correct POST request."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch("sgnl.bin.ll_remove_counts.requests.post") as mock_post:
                mock_post.return_value = mock.MagicMock(status_code=200)

                result = ll_remove_counts.submit(f.name, 1000000000, "test_tag")

                mock_post.assert_called_once_with(
                    "http://localhost:8080/test_tag/post/StrikeSnk",
                    json={"count_tracker": 1000000000},
                    timeout=10,
                )
                assert result.status_code == 200


class TestUndo:
    """Tests for undo function."""

    def test_undo_makes_post_request_with_negative_gps(self):
        """Test undo makes POST request with negative GPS time."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch("sgnl.bin.ll_remove_counts.requests.post") as mock_post:
                mock_post.return_value = mock.MagicMock(status_code=200)

                result = ll_remove_counts.undo(f.name, 1000000000, "test_tag")

                mock_post.assert_called_once_with(
                    "http://localhost:8080/test_tag/post/StrikeSnk",
                    json={"count_tracker": -1000000000},
                    timeout=10,
                )
                assert result.status_code == 200


class TestCheck:
    """Tests for check function."""

    def test_check_makes_get_request(self):
        """Test check makes correct GET request."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch("sgnl.bin.ll_remove_counts.requests.get") as mock_get:
                mock_get.return_value = mock.MagicMock(status_code=200)

                result = ll_remove_counts.check(f.name, 1000000000, "test_tag")

                mock_get.assert_called_once_with(
                    "http://localhost:8080/test_tag/get/StrikeSnk/count_removal_times",
                    timeout=10,
                )
                assert result.status_code == 200


class TestMain:
    """Tests for main function."""

    def test_main_remove_action(self):
        """Test main with remove action."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_remove_counts",
                    "--gps-time",
                    "1000000000",
                    "--action",
                    "remove",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch("sgnl.bin.ll_remove_counts.requests.post") as mock_post:
                    mock_post.return_value = mock.MagicMock(status_code=200)

                    ll_remove_counts.main()

                    mock_post.assert_called_once()

    def test_main_remove_action_multiple_files(self):
        """Test main with remove action and multiple files."""
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
                    "ll_remove_counts",
                    "--gps-time",
                    "1000000000",
                    "--action",
                    "remove",
                    "--analysis-tag",
                    "test",
                    f1.name,
                    f2.name,
                ],
            ):
                with mock.patch("sgnl.bin.ll_remove_counts.requests.post") as mock_post:
                    mock_post.return_value = mock.MagicMock(status_code=200)

                    ll_remove_counts.main()

                    assert mock_post.call_count == 2

    def test_main_remove_action_exception(self, capsys):
        """Test main with remove action when exception occurs."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "remove",
                "--analysis-tag",
                "test",
                "nonexistent_file.txt",
            ],
        ):
            with mock.patch(
                "sgnl.bin.ll_remove_counts.submit",
                side_effect=Exception("Connection error"),
            ):
                ll_remove_counts.main()

                captured = capsys.readouterr()
                assert "Error when removing from file" in captured.out
                assert "Connection error" in captured.out

    def test_main_undo_remove_action(self):
        """Test main with undo-remove action."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_remove_counts",
                    "--gps-time",
                    "1000000000",
                    "--action",
                    "undo-remove",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch("sgnl.bin.ll_remove_counts.requests.post") as mock_post:
                    mock_post.return_value = mock.MagicMock(status_code=200)

                    ll_remove_counts.main()

                    mock_post.assert_called_once()
                    # Verify negative GPS time was used
                    call_args = mock_post.call_args
                    assert call_args[1]["json"]["count_tracker"] == -1000000000

    def test_main_undo_remove_action_exception(self, capsys):
        """Test main with undo-remove action when exception occurs."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "undo-remove",
                "--analysis-tag",
                "test",
                "nonexistent_file.txt",
            ],
        ):
            with mock.patch(
                "sgnl.bin.ll_remove_counts.undo",
                side_effect=Exception("Connection error"),
            ):
                ll_remove_counts.main()

                captured = capsys.readouterr()
                assert "Error when undo-remove from file" in captured.out
                assert "Connection error" in captured.out

    def test_main_check_action_time_found(self, capsys):
        """Test main with check action when GPS time is found."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_remove_counts",
                    "--gps-time",
                    "1000000000",
                    "--action",
                    "check",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch("sgnl.bin.ll_remove_counts.requests.get") as mock_get:
                    mock_response = mock.MagicMock()
                    mock_response.json.return_value = [1000000000, 2000000000]
                    mock_get.return_value = mock_response

                    ll_remove_counts.main()

                    captured = capsys.readouterr()
                    assert "check successful" in captured.out

    def test_main_check_action_time_not_found(self, capsys):
        """Test main with check action when GPS time is not found."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("http://localhost:8080\n")
            f.flush()

            with mock.patch(
                "sys.argv",
                [
                    "ll_remove_counts",
                    "--gps-time",
                    "1000000000",
                    "--action",
                    "check",
                    "--analysis-tag",
                    "test",
                    f.name,
                ],
            ):
                with mock.patch("sgnl.bin.ll_remove_counts.requests.get") as mock_get:
                    mock_response = mock.MagicMock()
                    mock_response.json.return_value = [2000000000, 3000000000]
                    mock_get.return_value = mock_response

                    ll_remove_counts.main()

                    captured = capsys.readouterr()
                    assert "check unsuccessful" in captured.out

    def test_main_check_action_exception(self, capsys):
        """Test main with check action when exception occurs."""
        with mock.patch(
            "sys.argv",
            [
                "ll_remove_counts",
                "--gps-time",
                "1000000000",
                "--action",
                "check",
                "--analysis-tag",
                "test",
                "nonexistent_file.txt",
            ],
        ):
            with mock.patch(
                "sgnl.bin.ll_remove_counts.check",
                side_effect=Exception("Connection error"),
            ):
                ll_remove_counts.main()

                captured = capsys.readouterr()
                assert "Error when undo-remove from file" in captured.out
                assert "Connection error" in captured.out
