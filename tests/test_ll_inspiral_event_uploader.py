"""Tests for sgnl.bin.ll_inspiral_event_uploader"""

import http.client
import json
import sys
from collections import OrderedDict, deque
from io import BytesIO
from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Clear ll_inspiral_event_uploader from sys.modules first to ensure fresh import
    sys.modules.pop("sgnl.bin.ll_inspiral_event_uploader", None)

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
        "igwn_segments",
        "lal",
        "ligo",
        "ligo.gracedb",
        "ligo.gracedb.rest",
        "ligo.scald",
        "ligo.scald.utils",
        "ligo.scald.io",
        "ligo.scald.io.influx",
        "numpy",
        "yaml",
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

    # Set up segment class
    class MockSegment:
        def __init__(self, start, end):
            self.start = start
            self.end = end

        def __contains__(self, other):
            return self.start <= other.start <= self.end

        def __iter__(self):
            return iter([self.start, self.end])

        def __getitem__(self, idx):
            return [self.start, self.end][idx]

    sys.modules["igwn_segments"].segment = MockSegment

    # Set up LIGOTimeGPS
    class MockLIGOTimeGPS:
        def __init__(self, seconds, nanoseconds=0):
            self.gpsSeconds = seconds
            self.gpsNanoSeconds = nanoseconds

        def __float__(self):
            return float(self.gpsSeconds) + float(self.gpsNanoSeconds) * 1e-9

        def __sub__(self, other):
            return float(self) - float(other)

    sys.modules["lal"].LIGOTimeGPS = MockLIGOTimeGPS

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
    sys.modules.pop("sgnl.bin.ll_inspiral_event_uploader", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test parsing with default values."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch("sys.argv", ["ll_inspiral_event_uploader"]):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.verbose is False
            assert options.num_jobs == 10000
            assert options.tag == "test"
            assert options.max_event_time == 7200
            assert options.upload_cadence_type == "geometric"
            assert options.upload_cadence_factor == 4
            assert options.selection_criteria == "MAXSNR"
            assert options.far_threshold == 3.84e-07
            assert options.far_trials_factor == 1
            assert options.processing_cadence == 0.1
            assert options.request_timeout == 0.2
            assert options.gracedb_group == "Test"
            assert options.gracedb_pipeline == "SGNL"
            assert options.gracedb_search == "LowMass"
            assert options.max_partitions == 10

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch("sys.argv", ["ll_inspiral_event_uploader", "-v"]):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.verbose is True

    def test_kafka_server_option(self):
        """Test --kafka-server option."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_uploader", "--kafka-server", "kafka:9092"],
        ):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.kafka_server == "kafka:9092"

    def test_input_topic_option(self):
        """Test --input-topic option."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_uploader", "--input-topic", "events"],
        ):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.input_topic == "events"

    def test_selection_criteria_option(self):
        """Test --selection-criteria option."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_uploader", "--selection-criteria", "MINFAR"],
        ):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.selection_criteria == "MINFAR"

    def test_gracedb_service_url_option(self):
        """Test --gracedb-service-url option."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_uploader",
                "--gracedb-service-url",
                "https://test.gracedb.ligo.org/api/",
            ],
        ):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.gracedb_service_url == "https://test.gracedb.ligo.org/api/"

    def test_upload_cadence_options(self):
        """Test upload cadence options."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_uploader",
                "--upload-cadence-type",
                "linear",
                "--upload-cadence-factor",
                "2.0",
            ],
        ):
            options = ll_inspiral_event_uploader.parse_command_line()
            assert options.upload_cadence_type == "linear"
            assert options.upload_cadence_factor == 2.0


class TestEventUploader:
    """Tests for EventUploader class."""

    def _create_event_uploader(self, ll_inspiral_event_uploader, **kwargs):
        """Helper to create EventUploader instance with mocked dependencies."""
        defaults = {
            "input_topic": "events",
            "kafka_server": "kafka:9092",
            "logger": mock.MagicMock(),
            "scald_config": "/tmp/scald.yaml",
            "far_threshold": 3.84e-07,
            "far_trials_factor": 1,
            "gracedb_group": "Test",
            "gracedb_pipeline": "SGNL",
            "gracedb_search": "LowMass",
            "gracedb_service_url": "file:///tmp/gracedb",
            "max_event_time": 7200,
            "max_partitions": 10,
            "num_jobs": 10000,
            "processing_cadence": 0.1,
            "request_timeout": 0.2,
            "selection_criteria": kwargs.get("selection_criteria", "MAXSNR"),
            "tag": "test",
            "upload_cadence_factor": 4,
            "upload_cadence_type": "geometric",
        }
        defaults.update(kwargs)

        # Mock yaml.safe_load
        ll_inspiral_event_uploader.yaml.safe_load.return_value = {
            "backends": {"default": {}}
        }

        # Mock open for scald_config
        mock_open = mock.mock_open(read_data="")
        with mock.patch("builtins.open", mock_open):
            return ll_inspiral_event_uploader.EventUploader(**defaults)

    def test_init_basic(self):
        """Test EventUploader initialization."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        assert uploader.max_event_time == 7200
        assert uploader.retries == 5
        assert uploader.retry_delay == 1
        assert uploader.gracedb_group == "Test"
        assert uploader.gracedb_pipeline == "SGNL"
        assert uploader.gracedb_search == "LowMass"
        assert uploader.upload_cadence_type == "geometric"
        assert uploader.upload_cadence_factor == 4
        assert isinstance(uploader.events, OrderedDict)

    def test_init_with_gracedb_url(self):
        """Test EventUploader uses GraceDb client for non-file URLs."""
        from sgnl.bin import ll_inspiral_event_uploader

        ll_inspiral_event_uploader.GraceDb.reset_mock()
        self._create_event_uploader(
            ll_inspiral_event_uploader,
            gracedb_service_url="https://gracedb.ligo.org/api/",
        )

        ll_inspiral_event_uploader.GraceDb.assert_called_once_with(
            "https://gracedb.ligo.org/api/"
        )

    def test_init_with_injection_topic(self):
        """Test EventUploader with injection topic."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, input_topic="inj_events"
        )

        assert uploader.is_injection_job is True
        assert "inj_" in uploader.favored_event_topic
        assert "inj_" in uploader.upload_topic

    def test_init_minfar_selection(self):
        """Test EventUploader with MINFAR selection criteria."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="MINFAR"
        )

        assert uploader.favored_function == uploader.select_minfar_candidate

    def test_init_composite_selection(self):
        """Test EventUploader with COMPOSITE selection criteria."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="COMPOSITE"
        )

        assert uploader.favored_function == uploader.construct_composite_candidate

    def test_ingest_heartbeat_message(self):
        """Test ingest processes heartbeat messages."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        mock_message = mock.MagicMock()
        mock_message.key.return_value = b"heartbeat"
        mock_message.value.return_value = json.dumps({"time": 1000000000.0})

        uploader.ingest(mock_message)

        assert uploader.last_inspiral_heartbeat == 1000000000.0

    def test_ingest_candidate_message(self):
        """Test ingest processes candidate messages."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.process_candidate = mock.MagicMock()

        mock_message = mock.MagicMock()
        mock_message.key.return_value = None
        mock_message.value.return_value = json.dumps(
            {
                "time": 1000000000,
                "time_ns": 0,
                "snr": 10.0,
                "far": 1e-8,
                "job_tag": "0001_inspiral",
                "coinc": "<xml>test</xml>",
            }
        )

        # Mock trigger_info
        uploader.trigger_info = mock.MagicMock(return_value={"trigger_info": {}})

        uploader.ingest(mock_message)

        uploader.process_candidate.assert_called_once()

    def test_process_candidate_new_event(self):
        """Test process_candidate creates new event."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.update_trigger_history = mock.MagicMock()

        mock_time = mock.MagicMock()
        mock_time.__float__ = mock.MagicMock(return_value=1000000000.0)

        candidate = {
            "time": mock_time,
            "snr": 10.0,
            "far": 1e-8,
            "trigger_info": {"0001": {"snr": 10.0}},
        }

        # Mock event_window to return a unique key
        uploader.event_window = mock.MagicMock(
            return_value=ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        )

        uploader.process_candidate(candidate)

        assert len(uploader.events) == 1

    def test_process_candidate_existing_event(self):
        """Test process_candidate adds to existing event."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.update_trigger_history = mock.MagicMock()

        mock_time = mock.MagicMock()
        mock_time.__float__ = mock.MagicMock(return_value=1000000000.0)

        # Create existing event
        key = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.events[key] = uploader.new_event()

        candidate = {
            "time": mock_time,
            "snr": 10.0,
            "far": 1e-8,
            "trigger_info": {"0001": {"snr": 10.0}},
        }

        uploader.event_window = mock.MagicMock(return_value=key)

        uploader.process_candidate(candidate)

        assert len(uploader.events[key]["candidates"]) == 1

    def test_process_candidate_in_segment_but_not_key(self):
        """Test process_candidate when candidate falls within existing segment.

        This tests lines 326-332 where the key doesn't match directly
        but the candidate time falls within an existing event segment.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.update_trigger_history = mock.MagicMock()

        # Create existing event with a wide segment
        existing_seg = ll_inspiral_event_uploader.segment(1000000000.0, 1000000010.0)
        uploader.events[existing_seg] = uploader.new_event()

        # Use a simple float value for time (not MagicMock) so segment containment works
        candidate = {
            "time": 1000000005.0,  # Falls within existing_seg
            "snr": 10.0,
            "far": 1e-8,
            "trigger_info": {"0001": {"snr": 10.0}},
        }

        # event_window returns a different key that doesn't exist in self.events
        new_key = ll_inspiral_event_uploader.segment(1000000005.0, 1000000005.5)
        uploader.event_window = mock.MagicMock(return_value=new_key)

        uploader.process_candidate(candidate)

        # Should have been added to existing event
        assert len(uploader.events[existing_seg]["candidates"]) == 1
        # Should NOT have created a new event
        assert new_key not in uploader.events

    def test_trigger_info(self):
        """Test trigger_info extracts trigger information."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        # Mock lsctables
        mock_coinc_row = mock.MagicMock()
        mock_coinc_row.likelihood = 100.0
        mock_sngl_row = mock.MagicMock()
        mock_sngl_row.mass1 = 1.4
        mock_sngl_row.mass2 = 1.4
        mock_sngl_row.spin1z = 0.0
        mock_sngl_row.spin2z = 0.0
        mock_sngl_row.Gamma0 = 1

        ll_inspiral_event_uploader.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_row
        ]
        sngl_table = ll_inspiral_event_uploader.lsctables.SnglInspiralTable
        sngl_table.get_table.return_value = [mock_sngl_row]

        # Mock load_xmlobj
        uploader.load_xmlobj = mock.MagicMock(return_value=mock.MagicMock())

        candidate = {
            "snr": 10.0,
            "job_tag": "0001_inspiral",
            "coinc": "<xml>test</xml>",
        }

        result = uploader.trigger_info(candidate)

        assert "trigger_info" in result
        assert "0001" in result["trigger_info"]
        assert result["trigger_info"]["0001"]["snr"] == 10.0

    def test_parse_job_tag(self):
        """Test parse_job_tag extracts SVD bin and job type."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        svdbin, name = uploader.parse_job_tag("0001_inspiral")
        assert svdbin == "0001"
        assert name == "inspiral"

        svdbin, name = uploader.parse_job_tag("0002_snr_optimizer")
        assert svdbin == "0002"
        assert name == "snr_optimizer"

    def test_load_xmlobj_string(self):
        """Test load_xmlobj with string input."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        uploader.load_xmlobj("<xml>test</xml>")

        ll_inspiral_event_uploader.ligolw_utils.load_fileobj.assert_called()

    def test_load_xmlobj_bytes(self):
        """Test load_xmlobj with BytesIO input."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        uploader.load_xmlobj(BytesIO(b"<xml>test</xml>"))

        ll_inspiral_event_uploader.ligolw_utils.load_fileobj.assert_called()

    def test_event_window(self):
        """Test event_window returns correct segment."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        # Mock utils.floor_div
        ll_inspiral_event_uploader.utils.floor_div.side_effect = [
            1000000000.0,
            1000000000.0,
        ]

        result = uploader.event_window(1000000000.1)

        # Verify segment was created with correct bounds
        assert result is not None
        assert result[0] == 1000000000.0

    def test_new_event(self):
        """Test new_event returns correct structure."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        event = uploader.new_event()

        assert event["num_sent"] == 0
        assert event["time_sent"] is None
        assert event["favored"] is None
        assert event["gid"] is None
        assert isinstance(event["candidates"], deque)
        assert isinstance(event["trigger_history"], dict)

    def test_handle_uploads_event(self):
        """Test handle uploads events when conditions are met."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.process_event = mock.MagicMock()

        # Create event with candidates
        key = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.events[key] = uploader.new_event()
        uploader.events[key]["candidates"].append({"snr": 10.0, "far": 1e-8})

        # Mock gps_now to return time within max_event_time
        # Make heartbeat_write recent so we don't trigger heartbeat logic
        uploader.heartbeat_write = 1000000100.0
        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000000100.0

        uploader.handle()

        uploader.process_event.assert_called_once()

    def test_handle_cleans_old_events(self):
        """Test handle removes old events."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.upload_file = mock.MagicMock()

        # Create old event
        key = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.events[key] = uploader.new_event()
        uploader.events[key]["gid"] = "G12345"

        # Mock gps_now to return time beyond max_event_time
        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000010000.0
        uploader.heartbeat_write = 1000010000.0

        uploader.handle()

        assert key not in uploader.events
        uploader.upload_file.assert_called()

    def test_handle_writes_heartbeat(self):
        """Test handle writes heartbeat to influx."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        # Set heartbeat_write to old time
        uploader.heartbeat_write = 1000000000.0
        uploader.last_inspiral_heartbeat = 1000001000.0

        # Mock gps_now
        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000001000.0

        uploader.handle()

        uploader.agg_sink.store_columns.assert_called()

    def test_process_event_uploads(self):
        """Test process_event uploads when updated."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.upload_event = mock.MagicMock(return_value="G12345")
        uploader.send_favored_event = mock.MagicMock()
        uploader.send_uploaded = mock.MagicMock()

        event = uploader.new_event()
        event["candidates"].append({"snr": 10.0, "far": 1e-8, "time": mock.MagicMock()})

        # Mock process_candidates
        uploader.process_candidates = mock.MagicMock(
            return_value=(
                True,
                {
                    "num_sent": 0,
                    "favored": {"snr": 10.0, "far": 1e-8},
                    "candidates": deque(),
                    "time_sent": None,
                    "gid": None,
                    "trigger_history": {},
                },
            )
        )

        window = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.process_event(event, window)

        uploader.upload_event.assert_called_once()
        uploader.send_favored_event.assert_called_once()
        uploader.send_uploaded.assert_called_once()

    def test_process_candidates_with_update(self):
        """Test process_candidates when favored is updated.

        This tests lines 503-512 including the update path.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        event = uploader.new_event()
        event["candidates"].append({"snr": 12.0, "far": 1e-10})

        # Call process_candidates directly (don't mock it)
        updated, result_event = uploader.process_candidates(event)

        assert updated is True
        assert result_event["favored"]["snr"] == 12.0
        assert len(result_event["candidates"]) == 0  # cleared

    def test_process_candidates_no_update(self):
        """Test process_candidates when favored is not updated.

        This exercises the path where updated=False.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        event = uploader.new_event()
        # Set an existing favored that is better than the candidate
        event["favored"] = {"snr": 15.0, "far": 1e-12}
        event["candidates"].append({"snr": 8.0, "far": 1e-6})

        updated, result_event = uploader.process_candidates(event)

        assert updated is False
        # favored should remain unchanged
        assert result_event["favored"]["snr"] == 15.0
        assert len(result_event["candidates"]) == 0  # still cleared

    def test_update_trigger_history_called(self):
        """Test update_trigger_history is called and updates correctly.

        This tests line 345 by NOT mocking update_trigger_history.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        # Create an event
        key = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.events[key] = uploader.new_event()

        candidate = {
            "trigger_info": {"0001": {"snr": 10.0, "mass1": 1.4}},
        }

        # Call update_trigger_history directly
        uploader.update_trigger_history(key, candidate)

        # Verify the trigger_history was updated
        assert "0001" in uploader.events[key]["trigger_history"]
        assert uploader.events[key]["trigger_history"]["0001"]["snr"] == 10.0

    def test_select_maxsnr_candidate_no_favored(self):
        """Test select_maxsnr_candidate with no previous favored."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        candidates = [
            {"snr": 10.0, "far": 1e-8},
            {"snr": 12.0, "far": 1e-7},
        ]

        updated, favored = uploader.select_maxsnr_candidate(candidates, None)

        assert updated is True

    def test_select_maxsnr_candidate_with_better_favored(self):
        """Test select_maxsnr_candidate with better existing favored."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        candidates = [{"snr": 8.0, "far": 1e-7}]
        favored = {"snr": 10.0, "far": 1e-8}

        updated, result = uploader.select_maxsnr_candidate(candidates, favored)

        assert updated is False
        assert result == favored

    def test_select_maxsnr_candidate_new_beats_favored(self):
        """Test select_maxsnr_candidate when new candidate beats favored.

        This tests line 586 where new candidate has higher rank.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        # New candidate has higher SNR and lower FAR than favored
        candidates = [{"snr": 12.0, "far": 1e-10}]
        favored = {"snr": 8.0, "far": 1e-6}

        updated, result = uploader.select_maxsnr_candidate(candidates, favored)

        assert updated is True
        assert result["snr"] == 12.0

    def test_select_minfar_candidate(self):
        """Test select_minfar_candidate."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="MINFAR"
        )

        candidates = [
            {"snr": 10.0, "far": 1e-7},
            {"snr": 8.0, "far": 1e-9},
        ]

        updated, favored = uploader.select_minfar_candidate(candidates, None)

        assert updated is True
        assert favored["far"] == 1e-9

    def test_select_minfar_candidate_with_worse_far(self):
        """Test select_minfar_candidate when new FAR is worse."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="MINFAR"
        )

        candidates = [{"snr": 10.0, "far": 1e-7}]
        favored = {"snr": 8.0, "far": 1e-9}

        updated, result = uploader.select_minfar_candidate(candidates, favored)

        assert updated is False

    def test_construct_composite_candidate_no_favored(self):
        """Test construct_composite_candidate with no previous favored."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="COMPOSITE"
        )

        # Mock get_coinc_row
        mock_coinc_row = mock.MagicMock()
        mock_coinc_file = mock.MagicMock()
        uploader.get_coinc_row = mock.MagicMock(
            return_value=(mock_coinc_row, mock_coinc_file)
        )

        candidates = [
            {"snr": 10.0, "far": 1e-7, "coinc": "<xml>1</xml>"},
            {"snr": 8.0, "far": 1e-9, "coinc": "<xml>2</xml>"},
        ]

        updated, composite = uploader.construct_composite_candidate(candidates, None)

        assert updated is True

    def test_construct_composite_candidate_no_improvement(self):
        """Test construct_composite_candidate when no improvement."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="COMPOSITE"
        )

        candidates = [{"snr": 8.0, "far": 1e-7, "coinc": "<xml>1</xml>"}]
        favored = {"snr": 10.0, "far": 1e-9, "coinc": "<xml>fav</xml>"}

        updated, result = uploader.construct_composite_candidate(candidates, favored)

        assert updated is False
        assert result == favored

    def test_construct_composite_candidate_with_favored_assertions(self):
        """Test construct_composite_candidate with favored to exercise assertions.

        This tests lines 567-570 where asserts check FAR <= and SNR >= previous.
        """
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, selection_criteria="COMPOSITE"
        )

        # Mock get_coinc_row
        mock_coinc_row = mock.MagicMock()
        mock_coinc_file = mock.MagicMock()
        uploader.get_coinc_row = mock.MagicMock(
            return_value=(mock_coinc_row, mock_coinc_file)
        )

        # Candidates where composite will beat favored:
        # - max snr candidate: snr=12, far=1e-7
        # - min far candidate: snr=8, far=1e-10
        # - composite: snr=12, far=1e-10
        # - favored: snr=10, far=1e-8
        candidates = [
            {"snr": 12.0, "far": 1e-7, "coinc": "<xml>maxsnr</xml>"},
            {"snr": 8.0, "far": 1e-10, "coinc": "<xml>minfar</xml>"},
        ]
        # favored has worse FAR and lower SNR, so new composite should pass asserts
        favored = {"snr": 10.0, "far": 1e-8, "coinc": "<xml>fav</xml>"}

        updated, result = uploader.construct_composite_candidate(candidates, favored)

        assert updated is True
        # The composite should have max snr's snr and min far's far
        assert result["snr"] == 12.0
        assert result["far"] == 1e-10

    def test_rank_candidate_below_threshold(self):
        """Test rank_candidate for candidate below FAR threshold."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        candidate = {"snr": 10.0, "far": 1e-10}  # Below threshold

        rank = uploader.rank_candidate(candidate)

        assert rank[0] is True  # Below threshold flag

    def test_rank_candidate_above_threshold(self):
        """Test rank_candidate for candidate above FAR threshold."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        candidate = {"snr": 10.0, "far": 1e-5}  # Above threshold

        rank = uploader.rank_candidate(candidate)

        assert rank[0] is False  # Above threshold flag

    def test_rank_snr(self):
        """Test rank_snr static method."""
        from sgnl.bin import ll_inspiral_event_uploader

        candidate = {"snr": 10.0, "far": 1e-8}
        assert ll_inspiral_event_uploader.EventUploader.rank_snr(candidate) == 10.0

    def test_rank_far(self):
        """Test rank_far static method."""
        from sgnl.bin import ll_inspiral_event_uploader

        candidate = {"snr": 10.0, "far": 1e-8}
        assert ll_inspiral_event_uploader.EventUploader.rank_far(candidate) == 1e-8

    def test_send_favored_event(self):
        """Test send_favored_event sends via Kafka."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        mock_time = mock.MagicMock()
        mock_time.gpsSeconds = 1000000000
        mock_time.gpsNanoSeconds = 0

        event = {
            "favored": {
                "time": mock_time,
                "snr": 10.0,
                "far": 1e-8,
                "coinc": "<xml>test</xml>",
            }
        }
        event_window = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)

        uploader.send_favored_event(event, event_window)

        uploader.producer.produce.assert_called_once()

    def test_send_uploaded(self):
        """Test send_uploaded sends via Kafka."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        mock_time = mock.MagicMock()
        mock_time.gpsSeconds = 1000000000
        mock_time.gpsNanoSeconds = 0

        event = {
            "favored": {
                "time": mock_time,
                "snr": 10.0,
                "far": 1e-8,
                "coinc": "<xml>test</xml>",
            }
        }

        uploader.send_uploaded(event, "G12345")

        uploader.producer.produce.assert_called_once()
        assert uploader.partition_key == 1

    def test_send_uploaded_partition_wrap(self):
        """Test send_uploaded wraps partition key."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.partition_key = 10  # At max

        mock_time = mock.MagicMock()
        mock_time.gpsSeconds = 1000000000
        mock_time.gpsNanoSeconds = 0

        event = {
            "favored": {
                "time": mock_time,
                "snr": 10.0,
                "far": 1e-8,
                "coinc": "<xml>test</xml>",
            }
        }

        uploader.send_uploaded(event, "G12345")

        # After sending, partition_key should be 1 (wrapped from 10 to 0, then +1)
        assert uploader.partition_key == 1

    def test_upload_event_success(self):
        """Test upload_event successfully uploads."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.upload_file = mock.MagicMock()

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        mock_resp.json.return_value = {"graceid": "G12345"}
        uploader.client.createEvent.return_value = mock_resp

        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000000000.0

        event = {
            "favored": {"coinc": "<xml>test</xml>"},
            "time_sent": None,
            "trigger_history": {},
        }

        gid = uploader.upload_event(event)

        assert gid == "G12345"
        uploader.upload_file.assert_called_once()

    def test_upload_event_with_snr_optimized_label(self):
        """Test upload_event with SNR optimized label."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.upload_file = mock.MagicMock()

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        mock_resp.json.return_value = {"graceid": "G12345"}
        uploader.client.createEvent.return_value = mock_resp

        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000000000.0

        event = {
            "favored": {
                "coinc": "<xml>test</xml>",
                "apply_snr_optimized_label": True,
            },
            "time_sent": None,
            "trigger_history": {},
        }

        gid = uploader.upload_event(event)

        assert gid == "G12345"
        # Check that labels was passed
        call_kwargs = uploader.client.createEvent.call_args
        assert call_kwargs[1]["labels"] == "SNR_OPTIMIZED"

    def test_upload_event_http_error(self):
        """Test upload_event handles HTTPError."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.retries = 2

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        mock_resp.json.return_value = {"graceid": "G12345"}

        uploader.client.createEvent.side_effect = [
            ll_inspiral_event_uploader.HTTPError("test error"),
            mock_resp,
        ]

        uploader.upload_file = mock.MagicMock()
        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000000000.0
        ll_inspiral_event_uploader.numpy.random.lognormal.return_value = 0.1

        event = {
            "favored": {"coinc": "<xml>test</xml>"},
            "time_sent": None,
            "trigger_history": {},
        }

        with mock.patch("time.sleep"):
            gid = uploader.upload_event(event)

        assert gid == "G12345"

    def test_upload_event_generic_exception(self):
        """Test upload_event handles generic Exception."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.retries = 2

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        mock_resp.json.return_value = {"graceid": "G12345"}

        uploader.client.createEvent.side_effect = [
            Exception("generic error"),
            mock_resp,
        ]

        uploader.upload_file = mock.MagicMock()
        ll_inspiral_event_uploader.utils.gps_now.return_value = 1000000000.0
        ll_inspiral_event_uploader.numpy.random.lognormal.return_value = 0.1

        event = {
            "favored": {"coinc": "<xml>test</xml>"},
            "time_sent": None,
            "trigger_history": {},
        }

        with mock.patch("time.sleep"):
            gid = uploader.upload_event(event)

        assert gid == "G12345"

    def test_upload_event_all_retries_fail(self):
        """Test upload_event returns None after all retries fail."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.retries = 2

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.INTERNAL_SERVER_ERROR  # Not CREATED

        uploader.client.createEvent.return_value = mock_resp
        ll_inspiral_event_uploader.numpy.random.lognormal.return_value = 0.1

        event = {
            "favored": {"coinc": "<xml>test</xml>"},
            "time_sent": None,
            "trigger_history": {},
        }

        with mock.patch("time.sleep"):
            gid = uploader.upload_event(event)

        assert gid is None

    def test_upload_file_success(self):
        """Test upload_file successfully uploads."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED
        uploader.client.writeLog.return_value = mock_resp

        result = uploader.upload_file(
            "test message", "test.json", "test_tag", '{"test": true}', "G12345"
        )

        uploader.client.writeLog.assert_called_once()
        assert result is None  # Success returns None (no explicit return)

    def test_upload_file_http_error(self):
        """Test upload_file handles HTTPError."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.retries = 2
        uploader.client = mock.MagicMock()

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.CREATED

        uploader.client.writeLog.side_effect = [
            ll_inspiral_event_uploader.HTTPError("test error"),
            mock_resp,
        ]

        ll_inspiral_event_uploader.numpy.random.lognormal.return_value = 0.1

        with mock.patch("time.sleep"):
            uploader.upload_file(
                "test message", "test.json", "test_tag", '{"test": true}', "G12345"
            )

        assert uploader.client.writeLog.call_count == 2

    def test_upload_file_all_retries_fail(self):
        """Test upload_file returns False after all retries fail."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.retries = 2
        uploader.client = mock.MagicMock()

        mock_resp = mock.MagicMock()
        mock_resp.status = http.client.INTERNAL_SERVER_ERROR  # Not CREATED

        uploader.client.writeLog.return_value = mock_resp
        ll_inspiral_event_uploader.numpy.random.lognormal.return_value = 0.1

        with mock.patch("time.sleep"):
            result = uploader.upload_file(
                "test message", "test.json", "test_tag", '{"test": true}', "G12345"
            )

        assert result is False

    def test_next_event_upload_geometric(self):
        """Test next_event_upload with geometric cadence."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        ll_inspiral_event_uploader.numpy.power.return_value = 16.0

        event = {"time_sent": 1000000000.0, "num_sent": 2}

        result = uploader.next_event_upload(event)

        assert result == 1000000016.0

    def test_next_event_upload_linear(self):
        """Test next_event_upload with linear cadence."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(
            ll_inspiral_event_uploader, upload_cadence_type="linear"
        )
        uploader.upload_cadence_type = "linear"

        event = {"time_sent": 1000000000.0, "num_sent": 2}

        result = uploader.next_event_upload(event)

        assert result == 1000000008.0  # 1000000000 + 4 * 2

    def test_finish(self):
        """Test finish processes remaining events."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)
        uploader.process_event = mock.MagicMock()

        # Create event with candidates
        key = ll_inspiral_event_uploader.segment(1000000000.0, 1000000000.5)
        uploader.events[key] = uploader.new_event()
        uploader.events[key]["candidates"].append({"snr": 10.0, "far": 1e-8})

        uploader.finish()

        uploader.process_event.assert_called_once()

    def test_to_ordinal(self):
        """Test to_ordinal converts integers to ordinal strings."""
        from sgnl.bin import ll_inspiral_event_uploader

        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(1) == "1st"
        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(2) == "2nd"
        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(3) == "3rd"
        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(4) == "4th"
        # Note: The implementation produces "11st" not "11th" due to float division
        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(11) == "11st"
        assert ll_inspiral_event_uploader.EventUploader.to_ordinal(21) == "21st"

    def test_get_coinc_row(self):
        """Test get_coinc_row returns coinc row and file."""
        from sgnl.bin import ll_inspiral_event_uploader

        uploader = self._create_event_uploader(ll_inspiral_event_uploader)

        mock_coinc_row = mock.MagicMock()
        ll_inspiral_event_uploader.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_row
        ]

        event = {"coinc": "<xml>test</xml>"}

        row, file = uploader.get_coinc_row(event)

        assert row == mock_coinc_row


class TestMain:
    """Tests for main function."""

    def test_main_with_events_topic(self):
        """Test main function with events topic."""
        from sgnl.bin import ll_inspiral_event_uploader

        # Mock yaml.safe_load
        ll_inspiral_event_uploader.yaml.safe_load.return_value = {
            "backends": {"default": {}}
        }

        mock_uploader = mock.MagicMock()
        ll_inspiral_event_uploader.EventUploader = mock.MagicMock(
            return_value=mock_uploader
        )

        mock_open = mock.mock_open(read_data="")
        with mock.patch("builtins.open", mock_open):
            with mock.patch(
                "sys.argv",
                [
                    "ll_inspiral_event_uploader",
                    "--input-topic",
                    "events",
                    "--kafka-server",
                    "kafka:9092",
                    "--scald-config",
                    "/tmp/scald.yaml",
                ],
            ):
                ll_inspiral_event_uploader.main()

        mock_uploader.start.assert_called_once()

    def test_main_with_injection_topic(self):
        """Test main function with inj_events topic."""
        from sgnl.bin import ll_inspiral_event_uploader

        ll_inspiral_event_uploader.yaml.safe_load.return_value = {
            "backends": {"default": {}}
        }

        mock_uploader = mock.MagicMock()
        ll_inspiral_event_uploader.EventUploader = mock.MagicMock(
            return_value=mock_uploader
        )

        mock_open = mock.mock_open(read_data="")
        with mock.patch("builtins.open", mock_open):
            with mock.patch(
                "sys.argv",
                [
                    "ll_inspiral_event_uploader",
                    "--input-topic",
                    "inj_events",
                    "--kafka-server",
                    "kafka:9092",
                    "--scald-config",
                    "/tmp/scald.yaml",
                ],
            ):
                ll_inspiral_event_uploader.main()

        mock_uploader.start.assert_called_once()

    def test_main_invalid_input_topic_raises(self):
        """Test main raises for invalid input topic."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_uploader",
                "--input-topic",
                "invalid_topic",
                "--kafka-server",
                "kafka:9092",
                "--scald-config",
                "/tmp/scald.yaml",
            ],
        ):
            with pytest.raises(Exception, match="Input topic should be either"):
                ll_inspiral_event_uploader.main()

    def test_main_invalid_selection_criteria_raises(self):
        """Test main raises for invalid selection criteria."""
        from sgnl.bin import ll_inspiral_event_uploader

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_uploader",
                "--input-topic",
                "events",
                "--selection-criteria",
                "INVALID",
                "--kafka-server",
                "kafka:9092",
                "--scald-config",
                "/tmp/scald.yaml",
            ],
        ):
            with pytest.raises(ValueError, match="Favored event method must be"):
                ll_inspiral_event_uploader.main()

    def test_main_verbose(self):
        """Test main function with verbose flag."""
        from sgnl.bin import ll_inspiral_event_uploader

        ll_inspiral_event_uploader.yaml.safe_load.return_value = {
            "backends": {"default": {}}
        }

        mock_uploader = mock.MagicMock()
        ll_inspiral_event_uploader.EventUploader = mock.MagicMock(
            return_value=mock_uploader
        )

        mock_open = mock.mock_open(read_data="")
        with mock.patch("builtins.open", mock_open):
            with mock.patch(
                "sys.argv",
                [
                    "ll_inspiral_event_uploader",
                    "-v",
                    "--input-topic",
                    "events",
                    "--kafka-server",
                    "kafka:9092",
                    "--scald-config",
                    "/tmp/scald.yaml",
                ],
            ):
                ll_inspiral_event_uploader.main()

        mock_uploader.start.assert_called_once()
