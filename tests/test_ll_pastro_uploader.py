"""Tests for sgnl.bin.ll_pastro_uploader"""

import json
import sys
from collections import deque
from io import BytesIO
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Clear ll_pastro_uploader from sys.modules first to ensure fresh import
    sys.modules.pop("sgnl.bin.ll_pastro_uploader", None)

    original_modules = {}

    # Create a real-ish EventProcessor base class
    class MockEventProcessor:
        def __init__(self, **kwargs):
            self.num_messages = kwargs.get("num_messages", 10000)
            self.producer = mock.MagicMock()

        def start(self):
            pass

        def heartbeat(self):
            pass

    modules_to_mock = [
        "igwn_ligolw",
        "igwn_ligolw.ligolw",
        "igwn_ligolw.lsctables",
        "igwn_ligolw.utils",
        "igwn_ligolw.array",
        "igwn_ligolw.param",
        "ligo",
        "ligo.gracedb",
        "ligo.gracedb.rest",
        "ligo.scald",
        "ligo.scald.utils",
        "pastro",
        "pastro.pastro",
        "sgnl.events",
        "sgnl.gracedb",
    ]

    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        sys.modules[mod] = mock.MagicMock()

    # Set up DEFAULT_GRACEDB_URL
    sys.modules["ligo.gracedb.rest"].DEFAULT_SERVICE_URL = (
        "https://gracedb.ligo.org/api/"
    )

    # Create a proper HTTPError exception class that can be caught
    class MockHTTPError(Exception):
        """Mock HTTPError that can be raised and caught."""

        pass

    sys.modules["ligo.gracedb.rest"].HTTPError = MockHTTPError

    # Set up EventProcessor base class
    sys.modules["sgnl.events"].EventProcessor = MockEventProcessor

    # Also set the sgnl module attributes for the mocked submodules
    sgnl_orig_events = None
    sgnl_orig_gracedb = None
    if "sgnl" in sys.modules:
        sgnl_orig_events = getattr(sys.modules["sgnl"], "events", None)
        sgnl_orig_gracedb = getattr(sys.modules["sgnl"], "gracedb", None)
        sys.modules["sgnl"].events = sys.modules["sgnl.events"]
        sys.modules["sgnl"].gracedb = sys.modules["sgnl.gracedb"]

    yield

    # Restore the sgnl attributes
    if "sgnl" in sys.modules:
        if sgnl_orig_events is not None:
            sys.modules["sgnl"].events = sgnl_orig_events
        if sgnl_orig_gracedb is not None:
            sys.modules["sgnl"].gracedb = sgnl_orig_gracedb

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Clear cached imports
    sys.modules.pop("sgnl.bin.ll_pastro_uploader", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test parsing with default values."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch("sys.argv", ["ll_pastro_uploader"]):
            options = ll_pastro_uploader.parse_command_line()
            assert options.verbose is False
            assert options.num_messages == 10000
            assert options.tag == "test"
            assert options.processing_cadence == 0.1
            assert options.request_timeout == 0.2
            assert options.pastro_filename == "p_astro.json"

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch("sys.argv", ["ll_pastro_uploader", "-v"]):
            options = ll_pastro_uploader.parse_command_line()
            assert options.verbose is True

    def test_num_messages_option(self):
        """Test --num-messages option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--num-messages", "5000"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.num_messages == 5000

    def test_tag_option(self):
        """Test --tag option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--tag", "production"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.tag == "production"

    def test_processing_cadence_option(self):
        """Test --processing-cadence option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--processing-cadence", "0.5"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.processing_cadence == 0.5

    def test_request_timeout_option(self):
        """Test --request-timeout option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--request-timeout", "1.0"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.request_timeout == 1.0

    def test_kafka_server_option(self):
        """Test --kafka-server option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--kafka-server", "kafka:9092"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.kafka_server == "kafka:9092"

    def test_input_topic_option(self):
        """Test --input-topic option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--input-topic", "uploads"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.input_topic == ["uploads"]

    def test_multiple_input_topics(self):
        """Test multiple --input-topic options."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_pastro_uploader",
                "--input-topic",
                "uploads",
                "--input-topic",
                "inj_uploads",
            ],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.input_topic == ["uploads", "inj_uploads"]

    def test_gracedb_service_url_option(self):
        """Test --gracedb-service-url option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_pastro_uploader",
                "--gracedb-service-url",
                "https://test.gracedb.ligo.org/api/",
            ],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.gracedb_service_url == "https://test.gracedb.ligo.org/api/"

    def test_pastro_filename_option(self):
        """Test --pastro-filename option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--pastro-filename", "custom_pastro.json"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.pastro_filename == "custom_pastro.json"

    def test_model_name_option(self):
        """Test --model-name option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--model-name", "FGMC"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.model_name == "FGMC"

    def test_pastro_model_file_option(self):
        """Test --pastro-model-file option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--pastro-model-file", "/path/to/model.h5"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.pastro_model_file == "/path/to/model.h5"

    def test_rank_stat_option(self):
        """Test --rank-stat option."""
        from sgnl.bin import ll_pastro_uploader

        with mock.patch(
            "sys.argv",
            ["ll_pastro_uploader", "--rank-stat", "/path/to/rankstat.xml.gz"],
        ):
            options = ll_pastro_uploader.parse_command_line()
            assert options.rank_stat == "/path/to/rankstat.xml.gz"


class TestPAstroUploader:
    """Tests for PAstroUploader class."""

    def _create_pastro_uploader(self, ll_pastro_uploader, **kwargs):
        """Helper to create PAstroUploader instance with mocked dependencies."""
        defaults = {
            "gracedb_service_url": "file:///tmp/gracedb",
            "input_topic": ["uploads"],
            "kafka_server": "kafka:9092",
            "logger": mock.MagicMock(),
            "model_name": "FGMC",
            "pastro_model_file": "/tmp/model.h5",
            "rank_stat": "/tmp/rankstat.xml.gz",
            "num_messages": 10000,
            "pastro_filename": "p_astro.json",
            "processing_cadence": 0.1,
            "request_timeout": 0.2,
            "tag": "test",
        }
        defaults.update(kwargs)

        # Mock the pastro.load function
        mock_model = mock.MagicMock()
        mock_model.prior.return_value = {}
        ll_pastro_uploader.pastro.load.return_value = mock_model

        return ll_pastro_uploader.PAstroUploader(**defaults)

    def test_init_basic(self):
        """Test PAstroUploader initialization."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        assert uploader.tag == "test"
        assert uploader.model_name == "FGMC"
        assert uploader.max_retries == 3
        assert uploader.filename == "p_astro.json"
        assert uploader.pastro_model_file == "/tmp/model.h5"
        assert uploader.p_astro_topic == "sgnl.test.p_astro"
        assert uploader.rank_stat == "/tmp/rankstat.xml.gz"
        assert uploader.update_rankstat_cadence == 4.0 * 3600.0
        assert uploader.is_injection_job is False
        assert isinstance(uploader.events, deque)

    def test_init_with_gracedb_url(self):
        """Test PAstroUploader uses GraceDb client for non-file URLs."""
        from sgnl.bin import ll_pastro_uploader

        ll_pastro_uploader.GraceDb.reset_mock()
        self._create_pastro_uploader(
            ll_pastro_uploader,
            gracedb_service_url="https://gracedb.ligo.org/api/",
        )

        ll_pastro_uploader.GraceDb.assert_called_once_with(
            "https://gracedb.ligo.org/api/"
        )

    def test_init_with_file_gracedb_url(self):
        """Test PAstroUploader uses FakeGracedbClient for file URLs."""
        from sgnl.bin import ll_pastro_uploader

        ll_pastro_uploader.FakeGracedbClient.reset_mock()
        self._create_pastro_uploader(
            ll_pastro_uploader,
            gracedb_service_url="file:///tmp/gracedb",
        )

        ll_pastro_uploader.FakeGracedbClient.assert_called_once_with(
            "file:///tmp/gracedb"
        )

    def test_init_with_injection_topic(self):
        """Test PAstroUploader with injection topic."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(
            ll_pastro_uploader, input_topic=["inj_uploads"]
        )

        assert uploader.is_injection_job is True

    def test_ingest_message(self):
        """Test ingest processes messages."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.add_rankstat_to_model = mock.MagicMock()
        uploader.load_xmlobj = mock.MagicMock(return_value=mock.MagicMock())
        # Explicitly set to a recent float value so update check uses proper types
        uploader.last_rankstat_update = 1000000000.0

        mock_message = mock.MagicMock()
        mock_message.value.return_value = json.dumps(
            {
                "gid": "G12345",
                "time": 1000000000,
                "time_ns": 500000000,
                "coinc": "<xml>test</xml>",
                "snr_optimized": False,
            }
        )

        # Mock utils.gps_now for update_rankstat_cadence check
        ll_pastro_uploader.utils.gps_now.return_value = 1000000100.0

        uploader.ingest(mock_message)

        assert len(uploader.events) == 1
        assert uploader.events[0]["gid"] == "G12345"
        assert uploader.events[0]["snr_optimized"] is False
        assert uploader.events[0]["upload_attempts"] == 0
        assert uploader.events[0]["upload_success"] is False

    def test_ingest_triggers_rankstat_update(self):
        """Test ingest triggers ranking stat update when cadence is met."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.add_rankstat_to_model = mock.MagicMock()
        uploader.load_xmlobj = mock.MagicMock(return_value=mock.MagicMock())
        uploader.last_rankstat_update = None  # Not yet updated

        mock_message = mock.MagicMock()
        mock_message.value.return_value = json.dumps(
            {
                "gid": "G12345",
                "time": 1000000000,
                "time_ns": 0,
                "coinc": "<xml>test</xml>",
                "snr_optimized": False,
            }
        )

        ll_pastro_uploader.utils.gps_now.return_value = 1000000000.0

        uploader.ingest(mock_message)

        uploader.add_rankstat_to_model.assert_called_once()

    def test_ingest_skips_rankstat_update_within_cadence(self):
        """Test ingest skips ranking stat update if within cadence."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.add_rankstat_to_model = mock.MagicMock()
        uploader.load_xmlobj = mock.MagicMock(return_value=mock.MagicMock())
        uploader.last_rankstat_update = 1000000000.0  # Already updated

        mock_message = mock.MagicMock()
        mock_message.value.return_value = json.dumps(
            {
                "gid": "G12345",
                "time": 1000000000,
                "time_ns": 0,
                "coinc": "<xml>test</xml>",
                "snr_optimized": False,
            }
        )

        # Within update_rankstat_cadence (4 hours)
        ll_pastro_uploader.utils.gps_now.return_value = 1000001000.0

        uploader.ingest(mock_message)

        uploader.add_rankstat_to_model.assert_not_called()

    def test_handle_returns_early_without_rankstat_update(self):
        """Test handle returns early if ranking stat not yet updated."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = None
        uploader.calculate_pastro = mock.MagicMock()

        # Add an event
        uploader.events.append(
            {
                "gid": "G12345",
                "time": 1000000000.0,
                "coinc": mock.MagicMock(),
                "pastro": None,
                "snr_optimized": False,
                "upload_attempts": 0,
                "upload_success": False,
            }
        )

        uploader.handle()

        # Should return early, not process events
        uploader.calculate_pastro.assert_not_called()

    def test_handle_processes_events(self):
        """Test handle processes events after rankstat is updated."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock(return_value='{"BNS": 0.9}')
        uploader.upload_file = mock.MagicMock(return_value=mock.MagicMock())
        uploader.send_p_astro = mock.MagicMock()

        # Add an event
        uploader.events.append(
            {
                "gid": "G12345",
                "time": 1000000000.0,
                "coinc": mock.MagicMock(),
                "pastro": None,
                "snr_optimized": False,
                "upload_attempts": 0,
                "upload_success": False,
            }
        )

        uploader.handle()

        uploader.calculate_pastro.assert_called_once()
        uploader.upload_file.assert_called_once()

    def test_handle_upload_success(self):
        """Test handle marks upload_success after successful upload."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock(return_value='{"BNS": 0.9}')

        mock_response = mock.MagicMock()
        uploader.upload_file = mock.MagicMock(return_value=mock_response)
        uploader.send_p_astro = mock.MagicMock()

        # Add an event
        event = {
            "gid": "G12345",
            "time": 1000000000.0,
            "coinc": mock.MagicMock(),
            "pastro": None,
            "snr_optimized": False,
            "upload_attempts": 0,
            "upload_success": False,
        }
        uploader.events.append(event)

        uploader.handle()

        assert event["upload_success"] is True
        uploader.send_p_astro.assert_called_once()

    def test_handle_upload_failure(self):
        """Test handle increments upload_attempts on failure."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock(return_value='{"BNS": 0.9}')
        uploader.upload_file = mock.MagicMock(return_value=False)

        # Add an event
        event = {
            "gid": "G12345",
            "time": 1000000000.0,
            "coinc": mock.MagicMock(),
            "pastro": None,
            "snr_optimized": False,
            "upload_attempts": 0,
            "upload_success": False,
        }
        uploader.events.append(event)

        uploader.handle()

        assert event["upload_attempts"] == 1
        assert event["upload_success"] is False

    def test_handle_skips_successful_uploads(self):
        """Test handle skips already successful uploads."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock()

        # Add an already successful event
        uploader.events.append(
            {
                "gid": "G12345",
                "time": 1000000000.0,
                "coinc": mock.MagicMock(),
                "pastro": '{"BNS": 0.9}',
                "snr_optimized": False,
                "upload_attempts": 1,
                "upload_success": True,
            }
        )

        uploader.handle()

        uploader.calculate_pastro.assert_not_called()

    def test_handle_skips_max_retries(self):
        """Test handle skips events that have hit max retries."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock()

        # Add an event with max retries hit
        uploader.events.append(
            {
                "gid": "G12345",
                "time": 1000000000.0,
                "coinc": mock.MagicMock(),
                "pastro": None,
                "snr_optimized": False,
                "upload_attempts": 3,  # max_retries
                "upload_success": False,
            }
        )

        uploader.handle()

        uploader.calculate_pastro.assert_not_called()

    def test_handle_http_error_on_write_label(self):
        """Test handle catches HTTPError when writing label."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = 1000000000.0
        uploader.calculate_pastro = mock.MagicMock(return_value='{"BNS": 0.9}')

        mock_response = mock.MagicMock()
        uploader.upload_file = mock.MagicMock(return_value=mock_response)
        uploader.client.writeLabel.side_effect = ll_pastro_uploader.HTTPError(
            "test error"
        )

        # Add an event
        event = {
            "gid": "G12345",
            "time": 1000000000.0,
            "coinc": mock.MagicMock(),
            "pastro": None,
            "snr_optimized": False,
            "upload_attempts": 0,
            "upload_success": False,
        }
        uploader.events.append(event)

        # Should not raise
        uploader.handle()

        # upload_success should not be set since writeLabel failed
        assert event["upload_success"] is False

    def test_load_xmlobj_string(self):
        """Test load_xmlobj with string input."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        uploader.load_xmlobj("<xml>test</xml>")

        ll_pastro_uploader.ligolw_utils.load_fileobj.assert_called()

    def test_load_xmlobj_bytes(self):
        """Test load_xmlobj with BytesIO input."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        uploader.load_xmlobj(BytesIO(b"<xml>test</xml>"))

        ll_pastro_uploader.ligolw_utils.load_fileobj.assert_called()

    def test_load_model(self):
        """Test load_model loads and finalizes model."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        # Reset mock to test load_model independently
        ll_pastro_uploader.pastro.load.reset_mock()
        mock_model = mock.MagicMock()
        mock_model.prior.return_value = {}
        ll_pastro_uploader.pastro.load.return_value = mock_model

        result = uploader.load_model("/path/to/model.h5")

        ll_pastro_uploader.pastro.load.assert_called_once_with("/path/to/model.h5")
        mock_model.finalize.assert_called_once()
        assert result is mock_model

    def test_add_rankstat_to_model_success(self):
        """Test add_rankstat_to_model updates model successfully."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = None

        # Reset the mocks since they were called during __init__
        uploader.model.update_rankstatpdf.reset_mock()
        uploader.model.to_h5.reset_mock()

        ll_pastro_uploader.utils.gps_now.return_value = 1000000000.0

        with (
            mock.patch("tempfile.mkstemp") as mock_mkstemp,
            mock.patch("os.close"),
            mock.patch("shutil.move"),
        ):
            mock_mkstemp.return_value = (123, "/tmp/test_model.h5")

            uploader.add_rankstat_to_model()

        assert uploader.last_rankstat_update == 1000000000.0
        uploader.model.update_rankstatpdf.assert_called_once()
        uploader.model.to_h5.assert_called_once()

    def test_add_rankstat_to_model_os_error(self):
        """Test add_rankstat_to_model handles OSError."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = None

        uploader.model.update_rankstatpdf.side_effect = OSError("file not found")

        # Should not raise
        uploader.add_rankstat_to_model()

        # last_rankstat_update should remain None
        assert uploader.last_rankstat_update is None

    def test_add_rankstat_to_model_value_error(self):
        """Test add_rankstat_to_model handles ValueError."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.last_rankstat_update = None

        uploader.model.update_rankstatpdf.side_effect = ValueError("invalid value")

        # Should not raise
        uploader.add_rankstat_to_model()

        # last_rankstat_update should remain None
        assert uploader.last_rankstat_update is None

    def test_calculate_pastro_non_optimized(self):
        """Test calculate_pastro for non-SNR-optimized event."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.parse_data_from_coinc = mock.MagicMock(
            return_value={"mchirp": 1.2, "likelihood": 100.0}
        )
        uploader.model.return_value = '{"BNS": 0.9}'

        event = {
            "snr_optimized": False,
            "coinc": mock.MagicMock(),
            "time": 1000000000.0,
        }

        result = uploader.calculate_pastro(event)

        assert result == '{"BNS": 0.9}'
        uploader.model.assert_called_once()

    def test_calculate_pastro_snr_optimized(self):
        """Test calculate_pastro for SNR-optimized event copies from non-optimized."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        # Add a non-optimized event with pastro calculated
        non_optimized_event = {
            "snr_optimized": False,
            "coinc": mock.MagicMock(),
            "time": 1000000000.0,
            "pastro": '{"BNS": 0.9}',
        }
        uploader.events.append(non_optimized_event)

        # SNR-optimized event with same time
        optimized_event = {
            "snr_optimized": True,
            "coinc": mock.MagicMock(),
            "time": 1000000000.0,
        }

        result = uploader.calculate_pastro(optimized_event)

        assert result == '{"BNS": 0.9}'

    def test_calculate_pastro_snr_optimized_no_match(self):
        """Test calculate_pastro for SNR-optimized event with no matching event."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        # SNR-optimized event with no matching non-optimized event
        optimized_event = {
            "snr_optimized": True,
            "coinc": mock.MagicMock(),
            "time": 1000000000.0,
        }

        result = uploader.calculate_pastro(optimized_event)

        assert result is None

    def test_calculate_pastro_parse_failure(self):
        """Test calculate_pastro handles parse failure."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)
        uploader.parse_data_from_coinc = mock.MagicMock(return_value=None)

        event = {
            "snr_optimized": False,
            "coinc": mock.MagicMock(),
            "time": 1000000000.0,
        }

        result = uploader.calculate_pastro(event)

        assert result is None

    def test_parse_data_from_coinc_success(self):
        """Test parse_data_from_coinc extracts data correctly."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        # Mock lsctables
        mock_coinc_inspiral_table = mock.MagicMock()
        mock_coinc_inspiral_table.getColumnByName.side_effect = lambda col: (
            [1.2] if col == "mchirp" else [10.0]
        )

        mock_coinc_table = mock.MagicMock()
        mock_coinc_table.getColumnByName.return_value = [100.0]

        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.getColumnByName.return_value = [42]

        ll_pastro_uploader.lsctables.CoincInspiralTable.get_table.return_value = (
            mock_coinc_inspiral_table
        )
        ll_pastro_uploader.lsctables.CoincTable.get_table.return_value = (
            mock_coinc_table
        )
        ll_pastro_uploader.lsctables.SnglInspiralTable.get_table.return_value = (
            mock_sngl_table
        )

        mock_coinc = mock.MagicMock()
        result = uploader.parse_data_from_coinc(mock_coinc)

        assert result is not None
        assert result["mchirp"] == 1.2
        assert result["likelihood"] == 100.0
        assert result["template_id"] == 42

    def test_parse_data_from_coinc_exception(self):
        """Test parse_data_from_coinc handles exceptions."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        ll_pastro_uploader.lsctables.CoincInspiralTable.get_table.side_effect = (
            Exception("parse error")
        )

        mock_coinc = mock.MagicMock()
        result = uploader.parse_data_from_coinc(mock_coinc)

        assert result is None

    def test_upload_file_success(self):
        """Test upload_file successfully uploads."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        import http.client

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        uploader.client.writeLog.return_value = mock_resp

        result = uploader.upload_file(
            "G12345", "test message", "p_astro.json", '{"BNS": 0.9}', "p_astro"
        )

        uploader.client.writeLog.assert_called_once()
        assert result == mock_resp

    def test_upload_file_http_error(self):
        """Test upload_file handles HTTPError."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        import http.client

        # Create a fresh mock for writeLog that raises HTTPError then succeeds
        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED

        # Create list of responses for 3 retries
        def write_log_side_effect(*args, **kwargs):
            if write_log_side_effect.call_count == 0:
                write_log_side_effect.call_count += 1
                raise ll_pastro_uploader.HTTPError("test error")
            else:
                return mock_resp

        write_log_side_effect.call_count = 0
        uploader.client = mock.MagicMock()
        uploader.client.writeLog.side_effect = write_log_side_effect

        with mock.patch("time.sleep"):
            result = uploader.upload_file(
                "G12345", "test message", "p_astro.json", '{"BNS": 0.9}', "p_astro"
            )

        assert result == mock_resp

    def test_upload_file_all_retries_exhausted(self):
        """Test upload_file returns False after all retries fail."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        import http.client

        # Always return non-CREATED status
        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.INTERNAL_SERVER_ERROR
        uploader.client = mock.MagicMock()
        uploader.client.writeLog.return_value = mock_resp

        with mock.patch("time.sleep"):
            result = uploader.upload_file(
                "G12345", "test message", "p_astro.json", '{"BNS": 0.9}', "p_astro"
            )

        assert result is False
        assert uploader.client.writeLog.call_count == 3

    def test_send_p_astro(self):
        """Test send_p_astro sends via Kafka."""
        from sgnl.bin import ll_pastro_uploader

        uploader = self._create_pastro_uploader(ll_pastro_uploader)

        event = {"time": 1000000000.0}
        pastro = '{"BNS": 0.9}'

        uploader.send_p_astro(event, pastro)

        uploader.producer.produce.assert_called_once()
        call_kwargs = uploader.producer.produce.call_args
        assert call_kwargs[1]["topic"] == "sgnl.test.p_astro"
        uploader.producer.poll.assert_called_once_with(0)


class TestMain:
    """Tests for main function."""

    def test_main_basic(self):
        """Test main function with basic options."""
        from sgnl.bin import ll_pastro_uploader

        mock_model = mock.MagicMock()
        mock_model.prior.return_value = {}
        ll_pastro_uploader.pastro.load.return_value = mock_model

        mock_uploader = mock.MagicMock()
        with mock.patch.object(
            ll_pastro_uploader, "PAstroUploader", return_value=mock_uploader
        ):
            with mock.patch(
                "sys.argv",
                [
                    "ll_pastro_uploader",
                    "--kafka-server",
                    "kafka:9092",
                    "--input-topic",
                    "uploads",
                    "--gracedb-service-url",
                    "file:///tmp/gracedb",
                    "--model-name",
                    "FGMC",
                    "--pastro-model-file",
                    "/tmp/model.h5",
                    "--rank-stat",
                    "/tmp/rankstat.xml.gz",
                ],
            ):
                ll_pastro_uploader.main()

        mock_uploader.start.assert_called_once()

    def test_main_verbose(self):
        """Test main function with verbose flag."""
        from sgnl.bin import ll_pastro_uploader

        mock_model = mock.MagicMock()
        mock_model.prior.return_value = {}
        ll_pastro_uploader.pastro.load.return_value = mock_model

        mock_uploader = mock.MagicMock()
        with mock.patch.object(
            ll_pastro_uploader, "PAstroUploader", return_value=mock_uploader
        ):
            with mock.patch(
                "sys.argv",
                [
                    "ll_pastro_uploader",
                    "-v",
                    "--kafka-server",
                    "kafka:9092",
                    "--input-topic",
                    "uploads",
                    "--gracedb-service-url",
                    "file:///tmp/gracedb",
                    "--model-name",
                    "FGMC",
                    "--pastro-model-file",
                    "/tmp/model.h5",
                    "--rank-stat",
                    "/tmp/rankstat.xml.gz",
                ],
            ):
                ll_pastro_uploader.main()

        mock_uploader.start.assert_called_once()

    def test_main_with_all_options(self):
        """Test main function with all options."""
        from sgnl.bin import ll_pastro_uploader

        mock_model = mock.MagicMock()
        mock_model.prior.return_value = {}
        ll_pastro_uploader.pastro.load.return_value = mock_model

        mock_uploader = mock.MagicMock()
        with mock.patch.object(
            ll_pastro_uploader, "PAstroUploader", return_value=mock_uploader
        ):
            with mock.patch(
                "sys.argv",
                [
                    "ll_pastro_uploader",
                    "-v",
                    "--num-messages",
                    "5000",
                    "--tag",
                    "production",
                    "--processing-cadence",
                    "0.5",
                    "--request-timeout",
                    "1.0",
                    "--kafka-server",
                    "kafka:9092",
                    "--input-topic",
                    "uploads",
                    "--gracedb-service-url",
                    "https://gracedb.ligo.org/api/",
                    "--pastro-filename",
                    "custom_pastro.json",
                    "--model-name",
                    "FGMC",
                    "--pastro-model-file",
                    "/path/to/model.h5",
                    "--rank-stat",
                    "/path/to/rankstat.xml.gz",
                ],
            ):
                ll_pastro_uploader.main()

        mock_uploader.start.assert_called_once()
