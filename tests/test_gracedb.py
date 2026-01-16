"""Tests for sgnl.gracedb"""

import http.client
from unittest import mock

import pytest

from sgnl import gracedb


class TestFakeGracedbResp:
    """Tests for FakeGracedbResp class."""

    def test_init(self):
        """Test initialization."""
        resp = gracedb.FakeGracedbResp()
        assert resp.status == http.client.CREATED

    def test_json(self):
        """Test json method."""
        resp = gracedb.FakeGracedbResp()
        result = resp.json()
        assert result == {"graceid": -1}


class TestFakeGracedbClient:
    """Tests for FakeGracedbClient class."""

    def test_init(self):
        """Test initialization."""
        client = gracedb.FakeGracedbClient("file:///tmp/gracedb")
        assert client.path == "/tmp/gracedb"

    def test_create_event(self, tmp_path):
        """Test createEvent method."""
        client = gracedb.FakeGracedbClient(f"file://{tmp_path}")

        response = client.createEvent(
            group="CBC",
            pipeline="gstlal",
            filename="test.xml",
            filecontents="<xml>test</xml>",
            search="AllSky",
        )

        assert response.status == http.client.CREATED
        assert (tmp_path / "test.xml").exists()
        assert (tmp_path / "test.xml").read_text() == "<xml>test</xml>"

    def test_write_log(self):
        """Test writeLog method."""
        client = gracedb.FakeGracedbClient("file:///tmp/gracedb")

        response = client.writeLog(
            gracedb_id="G123456",
            message="Test log message",
            filename="test.txt",
            filecontents=b"test content",
            tagname="test",
        )

        assert response.status == http.client.CREATED

    def test_write_label(self):
        """Test writeLabel method."""
        client = gracedb.FakeGracedbClient("file:///tmp/gracedb")

        response = client.writeLabel(
            gracedb_id="G123456",
            tagname="DQV",
        )

        assert response.status == http.client.CREATED


class TestUploadFig:
    """Tests for upload_fig function."""

    def test_upload_fig_success(self):
        """Test successful figure upload."""
        mock_fig = mock.MagicMock()
        mock_client = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.status = http.client.CREATED
        mock_client.writeLog.return_value = mock_response

        with mock.patch("logging.info"):
            gracedb.upload_fig(
                fig=mock_fig,
                gracedb_client=mock_client,
                graceid="G123456",
                filename="test.png",
                log_message="Test upload",
                tagname="psd",
            )

        mock_fig.savefig.assert_called_once()
        mock_client.writeLog.assert_called_once()

    def test_upload_fig_with_different_format(self):
        """Test figure upload with different file format."""
        mock_fig = mock.MagicMock()
        mock_client = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.status = http.client.CREATED
        mock_client.writeLog.return_value = mock_response

        with mock.patch("logging.info"):
            gracedb.upload_fig(
                fig=mock_fig,
                gracedb_client=mock_client,
                graceid="G123456",
                filename="test.pdf",
                log_message="Test upload",
            )

        # Should save with pdf format
        call_args = mock_fig.savefig.call_args
        assert call_args[1]["format"] == "pdf"

    def test_upload_fig_failure(self):
        """Test figure upload failure."""
        mock_fig = mock.MagicMock()
        mock_client = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.status = http.client.INTERNAL_SERVER_ERROR
        mock_response.__getitem__ = mock.MagicMock(return_value="Server error")
        mock_client.writeLog.return_value = mock_response

        with mock.patch("logging.info"):
            with pytest.raises(Exception) as exc_info:
                gracedb.upload_fig(
                    fig=mock_fig,
                    gracedb_client=mock_client,
                    graceid="G123456",
                    filename="test.png",
                    log_message="Test upload",
                )

        assert "upload of" in str(exc_info.value)
        assert "failed" in str(exc_info.value)
