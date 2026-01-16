"""Tests for sgnl.bin.ll_inspiral_event_plotter"""

import http.client
import sys
from collections import OrderedDict
from unittest import mock
from xml.sax import SAXParseException

import pytest


@pytest.fixture(autouse=True)
def mock_dependencies():
    """Mock external dependencies and clean up after tests."""
    # Clear ll_inspiral_event_plotter from sys.modules first to ensure fresh import
    sys.modules.pop("sgnl.bin.ll_inspiral_event_plotter", None)

    original_modules = {}

    # Create mock for matplotlib with proper structure
    matplotlib_mock = mock.MagicMock()
    matplotlib_mock.rcParams = {}
    matplotlib_mock.use = mock.MagicMock()

    # Create a real-ish EventProcessor base class
    class MockEventProcessor:
        def __init__(self, **kwargs):
            pass

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
        "lal",
        "ligo",
        "ligo.gracedb",
        "ligo.gracedb.rest",
        "ligo.scald",
        "ligo.scald.utils",
        "strike",
        "strike.stats",
        "strike.stats.likelihood_ratio",
        "strike.plots",
        "strike.plots.stats",
        "matplotlib",
        "matplotlib.figure",
        "matplotlib.backends",
        "matplotlib.backends.backend_agg",
        "matplotlib.pyplot",
        "numpy",
        "sgnl.events",
        "sgnl.svd_bank",
        "sgnl.gracedb",
        "sgnl.plots",
        "sgnl.plots.util",
        "sgnl.plots.psd",
    ]

    for mod in modules_to_mock:
        original_modules[mod] = sys.modules.get(mod)
        if mod == "matplotlib":
            sys.modules[mod] = matplotlib_mock
        else:
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

    # Also set the svd_bank attribute on the sgnl parent module to our mock
    # This is needed because `from sgnl import svd_bank` looks up the attribute
    # on the parent module, not just in sys.modules
    sgnl_orig_svd_bank = None
    if "sgnl" in sys.modules:
        sgnl_orig_svd_bank = getattr(sys.modules["sgnl"], "svd_bank", None)
        sys.modules["sgnl"].svd_bank = sys.modules["sgnl.svd_bank"]

    yield

    # Restore the sgnl.svd_bank attribute
    if "sgnl" in sys.modules and sgnl_orig_svd_bank is not None:
        sys.modules["sgnl"].svd_bank = sgnl_orig_svd_bank

    # Restore originals
    for mod in modules_to_mock:
        if original_modules[mod] is None:
            sys.modules.pop(mod, None)
        else:
            sys.modules[mod] = original_modules[mod]

    # Clear cached imports
    sys.modules.pop("sgnl.bin.ll_inspiral_event_plotter", None)


class TestParseCommandLine:
    """Tests for parse_command_line function."""

    def test_default_values(self):
        """Test parsing with default values."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_plotter", "--output-path", "/tmp/plots"],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.verbose is False
            assert options.tag == "test"
            assert options.max_event_time == 7200
            assert options.processing_cadence == 0.1
            assert options.request_timeout == 0.2
            assert options.max_snr == 200.0
            assert options.format == "png"
            assert options.no_upload is False

    def test_verbose_flag(self):
        """Test --verbose flag."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_plotter", "-v", "--output-path", "/tmp"],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.verbose is True

    def test_tag_option(self):
        """Test --tag option."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_plotter", "--tag", "prod", "--output-path", "/tmp"],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.tag == "prod"

    def test_max_event_time_option(self):
        """Test --max-event-time option."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--max-event-time",
                "3600",
                "--output-path",
                "/tmp",
            ],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.max_event_time == 3600

    def test_kafka_server_option(self):
        """Test --kafka-server option."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--kafka-server",
                "kafka:9092",
                "--output-path",
                "/tmp",
            ],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.kafka_server == "kafka:9092"

    def test_plot_option(self):
        """Test --plot option."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--plot",
                "RANKING_DATA",
                "--plot",
                "PSD_PLOTS",
                "--output-path",
                "/tmp",
            ],
        ):
            options = ll_inspiral_event_plotter.parse_command_line()
            assert options.plot == ["RANKING_DATA", "PSD_PLOTS"]

    def test_no_upload_without_output_path_raises(self):
        """Test --no-upload without --output-path raises ValueError."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            ["ll_inspiral_event_plotter", "--no-upload"],
        ):
            with pytest.raises(ValueError, match="--no-upload without setting"):
                ll_inspiral_event_plotter.parse_command_line()


class TestLoadRankingstatXmlWithRetries:
    """Tests for load_rankingstat_xml_with_retries function."""

    def test_load_success_first_try(self):
        """Test successful load on first try."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.ligolw_utils.load_filename.reset_mock()
        mock_xmldoc = mock.MagicMock()
        ll_inspiral_event_plotter.ligolw_utils.load_filename.return_value = mock_xmldoc

        mock_logger = mock.MagicMock()
        result = ll_inspiral_event_plotter.load_rankingstat_xml_with_retries(
            "/path/to/file.xml", mock_logger
        )

        assert result == mock_xmldoc

    def test_load_success_after_retries(self):
        """Test successful load after retries."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.ligolw_utils.load_filename.reset_mock()
        mock_xmldoc = mock.MagicMock()
        mock_locator = mock.MagicMock()
        mock_locator.getLineNumber.return_value = 1
        mock_locator.getColumnNumber.return_value = 1
        ll_inspiral_event_plotter.ligolw_utils.load_filename.side_effect = [
            SAXParseException("error", None, mock_locator),
            OSError("error"),
            mock_xmldoc,
        ]

        mock_logger = mock.MagicMock()

        with mock.patch("time.sleep"):
            result = ll_inspiral_event_plotter.load_rankingstat_xml_with_retries(
                "/path/to/file.xml", mock_logger
            )

        assert result == mock_xmldoc

    def test_load_failure_after_all_retries(self):
        """Test IOError raised after all retries fail."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.ligolw_utils.load_filename.reset_mock()
        ll_inspiral_event_plotter.ligolw_utils.load_filename.side_effect = OSError(
            "persistent error"
        )

        mock_logger = mock.MagicMock()

        with mock.patch("time.sleep"):
            with pytest.raises(IOError, match="Could not load rankingstat"):
                ll_inspiral_event_plotter.load_rankingstat_xml_with_retries(
                    "/path/to/file.xml", mock_logger
                )


class TestPlots:
    """Tests for Plots enum."""

    def test_plots_enum_values(self):
        """Test Plots enum has expected values."""
        from sgnl.bin import ll_inspiral_event_plotter

        assert ll_inspiral_event_plotter.Plots.RANKING_DATA.value == 1
        assert ll_inspiral_event_plotter.Plots.RANKING_PLOTS.value == 2
        assert ll_inspiral_event_plotter.Plots.SNR_PLOTS.value == 3
        assert ll_inspiral_event_plotter.Plots.PSD_PLOTS.value == 4
        assert ll_inspiral_event_plotter.Plots.DTDPHI_PLOTS.value == 5


class TestEventPlotter:
    """Tests for EventPlotter class."""

    def _create_event_plotter(self, ll_inspiral_event_plotter, **kwargs):
        """Helper to create EventPlotter with common defaults."""
        defaults = {
            "kafka_server": "kafka:9092",
            "logger": mock.MagicMock(),
            "output_path": "/tmp/plots",
            "plot": ["RANKING_DATA"],
            "upload_topic": "uploads",
            "ranking_stat_topic": "ranking",
            "format": "png",
            "gracedb_service_url": "file:///tmp/gracedb",
            "max_event_time": 7200,
            "max_snr": 200.0,
            "no_upload": False,
            "processing_cadence": 0.1,
            "request_timeout": 0.2,
            "tag": "test",
        }
        defaults.update(kwargs)
        return ll_inspiral_event_plotter.EventPlotter(**defaults)

    def test_init_basic(self):
        """Test EventPlotter initialization."""
        from sgnl.bin import ll_inspiral_event_plotter

        mock_logger = mock.MagicMock()
        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, logger=mock_logger
        )

        assert plotter.upload_topic == "sgnl.test.uploads"
        assert plotter.ranking_stat_topic == "sgnl.test.ranking"
        assert plotter.max_event_time == 7200
        assert plotter.max_snr == 200.0
        assert plotter.format == "png"
        assert plotter.output_path == "/tmp/plots"
        assert plotter.no_upload is False

    def test_init_with_gracedb_url(self):
        """Test EventPlotter uses GraceDb client for non-file URLs."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.GraceDb.reset_mock()
        self._create_event_plotter(
            ll_inspiral_event_plotter,
            gracedb_service_url="https://gracedb.ligo.org/api/",
        )

        ll_inspiral_event_plotter.GraceDb.assert_called_once_with(
            "https://gracedb.ligo.org/api/"
        )

    def test_init_with_injection_topic(self):
        """Test EventPlotter with injection upload topic."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, upload_topic="inj_uploads"
        )

        assert plotter.upload_topic == "sgnl.test.inj_uploads"

    def test_new_event(self):
        """Test new_event creates correct structure."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_time = mock.MagicMock()
        event = plotter.new_event(mock_time, "0001")

        assert event["time"] == mock_time
        assert event["bin"] == "0001"
        assert event["coinc"] is None
        assert event["gid"] is None
        assert event["psd"] is None
        assert event["ranking_data_path"] is None
        assert "RANKING_DATA" in event["uploaded"]

    def test_ingest_ranking_stat_message(self):
        """Test ingest handles ranking stat messages."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_message = mock.MagicMock()
        mock_message.topic.return_value = plotter.ranking_stat_topic
        mock_message.value.return_value = (
            '{"time": 1000000000, "time_ns": 0, '
            '"coinc": "<xml></xml>", "ranking_data_path": "/path/to/ranking.xml"}'
        )

        mock_sngl = mock.MagicMock()
        mock_sngl.Gamma1 = 1
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        plotter.ingest(mock_message)

        assert "1000000000_0001" in plotter.events
        assert (
            plotter.events["1000000000_0001"]["ranking_data_path"]
            == "/path/to/ranking.xml"
        )

    def test_ingest_upload_message(self):
        """Test ingest handles upload messages."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_message = mock.MagicMock()
        mock_message.topic.return_value = plotter.upload_topic
        mock_message.value.return_value = (
            '{"time": 1000000000, "time_ns": 0, '
            '"coinc": "<xml></xml>", "gid": "G12345", "snr_optimized": true}'
        )

        mock_sngl = mock.MagicMock()
        mock_sngl.Gamma1 = 1
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        plotter.ingest(mock_message)

        assert "1000000000_0001" in plotter.events
        assert plotter.events["1000000000_0001"]["gid"] == "G12345"
        assert plotter.events["1000000000_0001"]["snr_optimized"] is True

    def test_ingest_upload_message_no_snr_optimized(self):
        """Test ingest handles upload messages without snr_optimized."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_message = mock.MagicMock()
        mock_message.topic.return_value = plotter.upload_topic
        mock_message.value.return_value = (
            '{"time": 1000000000, "time_ns": 0, '
            '"coinc": "<xml></xml>", "gid": "G12345"}'
        )

        mock_sngl = mock.MagicMock()
        mock_sngl.Gamma1 = 1
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        plotter.ingest(mock_message)

        assert plotter.events["1000000000_0001"]["snr_optimized"] is False

    def test_handle_uploads_ranking_data(self):
        """Test handle uploads ranking data when available."""
        from sgnl.bin import ll_inspiral_event_plotter

        # Use multiple upload types so event doesn't get cleaned up immediately
        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, plot=["RANKING_DATA", "PSD_PLOTS"]
        )
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)
        plotter.events["test_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": "G12345",
            "psd": None,  # PSD is None so PSD_PLOTS won't upload
            "ranking_data_path": "/path/to/ranking.xml",
            "uploaded": {"RANKING_DATA": False, "PSD_PLOTS": False},
        }

        ll_inspiral_event_plotter.utils.gps_now.return_value = mock_time
        plotter.upload_ranking_data = mock.MagicMock()

        plotter.handle()

        plotter.upload_ranking_data.assert_called_once()
        assert plotter.events["test_event"]["uploaded"]["RANKING_DATA"] is True

    def test_handle_uploads_ranking_plots(self):
        """Test handle uploads ranking plots when available."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, plot=["RANKING_PLOTS"]
        )
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)
        plotter.events["test_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": "G12345",
            "psd": None,
            "ranking_data_path": "/path/to/ranking.xml",
            "uploaded": {"RANKING_PLOTS": False},
        }

        ll_inspiral_event_plotter.utils.gps_now.return_value = mock_time
        plotter.upload_ranking_plots = mock.MagicMock()

        plotter.handle()

        plotter.upload_ranking_plots.assert_called_once()

    def test_handle_uploads_psd_plots(self):
        """Test handle uploads PSD plots when available."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, plot=["PSD_PLOTS"]
        )
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)
        mock_psd = mock.MagicMock()
        plotter.events["test_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": "G12345",
            "psd": mock_psd,
            "ranking_data_path": None,
            "uploaded": {"PSD_PLOTS": False},
        }

        ll_inspiral_event_plotter.utils.gps_now.return_value = mock_time
        plotter.upload_psd_plots = mock.MagicMock()

        plotter.handle()

        plotter.upload_psd_plots.assert_called_once()

    def test_handle_uploads_snr_plots(self):
        """Test handle uploads SNR plots when available."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, plot=["SNR_PLOTS"]
        )
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)
        plotter.events["test_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": "G12345",
            "psd": None,
            "ranking_data_path": None,
            "uploaded": {"SNR_PLOTS": False},
        }

        ll_inspiral_event_plotter.utils.gps_now.return_value = mock_time
        plotter.upload_snr_plots = mock.MagicMock()

        plotter.handle()

        plotter.upload_snr_plots.assert_called_once()

    def test_handle_uploads_dtdphi_plots(self):
        """Test handle uploads dtdphi plots when available."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, plot=["DTDPHI_PLOTS"]
        )
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)
        plotter.events["test_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": "G12345",
            "psd": None,
            "ranking_data_path": "/path/to/ranking.xml",
            "uploaded": {"DTDPHI_PLOTS": False},
        }

        ll_inspiral_event_plotter.utils.gps_now.return_value = mock_time
        plotter.upload_dtdphi_plots = mock.MagicMock()

        plotter.handle()

        plotter.upload_dtdphi_plots.assert_called_once()

    def test_handle_cleans_old_events(self):
        """Test handle removes old events."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.events = OrderedDict()

        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=10000)
        plotter.events["old_event"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": None,
            "gid": None,
            "psd": None,
            "ranking_data_path": None,
            "uploaded": {"RANKING_DATA": False},
        }

        current_time = mock.MagicMock()
        current_time.__sub__ = mock.MagicMock(return_value=10000)
        ll_inspiral_event_plotter.utils.gps_now.return_value = current_time

        plotter.handle()

        assert "old_event" not in plotter.events

    def test_handle_unlinks_coinc_on_removal(self):
        """Test handle unlinks coinc document when removing event."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.events = OrderedDict()

        mock_coinc = mock.MagicMock()
        mock_psd = mock.MagicMock()
        mock_time = mock.MagicMock()
        mock_time.__sub__ = mock.MagicMock(return_value=100)

        plotter.events["event_with_coinc"] = {
            "time": mock_time,
            "bin": "0001",
            "coinc": mock_coinc,
            "gid": None,
            "psd": mock_psd,
            "ranking_data_path": None,
            "uploaded": {"RANKING_DATA": True},
        }

        current_time = mock.MagicMock()
        current_time.__sub__ = mock.MagicMock(return_value=100)
        ll_inspiral_event_plotter.utils.gps_now.return_value = current_time

        plotter.handle()

        mock_coinc.unlink.assert_called_once()
        mock_psd.unlink.assert_called_once()

    def test_upload_file_success(self):
        """Test upload_file succeeds on first try."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_response = mock.MagicMock()
        mock_response.status = http.client.CREATED
        plotter.client.writeLog.return_value = mock_response

        plotter.upload_file(
            "test message", "test.xml", "test_tag", b"content", "G12345"
        )

        plotter.client.writeLog.assert_called_once()

    def test_upload_file_retries_on_failure(self):
        """Test upload_file retries on HTTP error."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.client.writeLog.reset_mock()

        mock_response_fail = mock.MagicMock()
        mock_response_fail.status = http.client.BAD_REQUEST
        mock_response_success = mock.MagicMock()
        mock_response_success.status = http.client.CREATED

        plotter.client.writeLog.side_effect = [
            mock_response_fail,
            mock_response_fail,
            mock_response_success,
        ]

        with mock.patch("time.sleep"):
            ll_inspiral_event_plotter.numpy.random.lognormal.return_value = 1.0
            plotter.upload_file(
                "test message", "test.xml", "test_tag", b"content", "G12345"
            )

        assert plotter.client.writeLog.call_count == 3

    def test_upload_file_handles_http_error(self):
        """Test upload_file handles HTTPError exception."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.client.writeLog.reset_mock()

        mock_response = mock.MagicMock()
        mock_response.status = http.client.CREATED
        plotter.client.writeLog.side_effect = [
            ll_inspiral_event_plotter.HTTPError("mock http error"),
            mock_response,
        ]

        with mock.patch("time.sleep"):
            ll_inspiral_event_plotter.numpy.random.lognormal.return_value = 1.0
            plotter.upload_file(
                "test message", "test.xml", "test_tag", b"content", "G12345"
            )

        assert plotter.client.writeLog.call_count == 2

    def test_upload_file_returns_false_after_all_retries(self):
        """Test upload_file returns False after all retries fail."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.retries = 2
        plotter.client = mock.MagicMock()

        mock_response = mock.MagicMock()
        mock_response.status = http.client.BAD_REQUEST
        plotter.client.writeLog.return_value = mock_response

        with mock.patch("time.sleep"):
            ll_inspiral_event_plotter.numpy.random.lognormal.return_value = 1.0
            result = plotter.upload_file(
                "test message", "test.xml", "test_tag", b"content", "G12345"
            )

        assert result is False

    def test_upload_ranking_data(self):
        """Test upload_ranking_data uploads file."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.upload_file = mock.MagicMock()

        event = {
            "gid": "G12345",
            "ranking_data_path": "/path/to/ranking.xml.gz",
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_data(event)

        plotter.upload_file.assert_called_once()
        call_args = plotter.upload_file.call_args
        assert call_args[0][1] == "ranking_data.xml.gz"
        assert call_args[0][4] == "G12345"

    def test_upload_ranking_data_uncompressed(self):
        """Test upload_ranking_data handles uncompressed file."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.upload_file = mock.MagicMock()

        event = {
            "gid": "G12345",
            "ranking_data_path": "/path/to/ranking.xml",
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_data(event)

        plotter.upload_file.assert_called_once()

    def test_upload_psd_plots(self):
        """Test upload_psd_plots generates and uploads PSD plots."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter, no_upload=True)

        mock_psd_doc = mock.MagicMock()
        mock_coinc = mock.MagicMock()
        mock_psds = {"H1": mock.MagicMock(), "L1": mock.MagicMock()}
        ll_inspiral_event_plotter.series.read_psd_xmldoc.return_value = mock_psds

        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plotpsd.plot_psds.return_value = mock_fig
        ll_inspiral_event_plotter.plotpsd.plot_cumulative_snrs.return_value = mock_fig

        event = {
            "gid": "G12345",
            "psd": mock_psd_doc,
            "coinc": mock_coinc,
        }

        plotter.upload_psd_plots(event)

        ll_inspiral_event_plotter.plotpsd.plot_psds.assert_called_once()
        ll_inspiral_event_plotter.plotpsd.plot_cumulative_snrs.assert_called_once()

    def test_upload_psd_plots_with_upload(self):
        """Test upload_psd_plots uploads when no_upload is False."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=False, output_path=None
        )

        mock_psds = {"H1": mock.MagicMock()}
        ll_inspiral_event_plotter.series.read_psd_xmldoc.return_value = mock_psds

        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plotpsd.plot_psds.return_value = mock_fig
        ll_inspiral_event_plotter.plotpsd.plot_cumulative_snrs.return_value = mock_fig

        event = {
            "gid": "G12345",
            "psd": mock.MagicMock(),
            "coinc": mock.MagicMock(),
        }

        plotter.upload_psd_plots(event)

        ll_inspiral_event_plotter.upload_fig.assert_called()

    def test_upload_psd_plots_returns_early_if_no_psds(self):
        """Test upload_psd_plots returns early if psds is None."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        ll_inspiral_event_plotter.series.read_psd_xmldoc.reset_mock()
        ll_inspiral_event_plotter.series.read_psd_xmldoc.return_value = None

        ll_inspiral_event_plotter.plotpsd.plot_psds.reset_mock()

        event = {
            "gid": "G12345",
            "psd": mock.MagicMock(),
            "coinc": mock.MagicMock(),
        }

        plotter.upload_psd_plots(event)

        ll_inspiral_event_plotter.plotpsd.plot_psds.assert_not_called()

    def test_finish_calls_handle(self):
        """Test finish calls handle."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)
        plotter.handle = mock.MagicMock()

        plotter.finish()

        plotter.handle.assert_called_once()

    def test_upload_ranking_plots(self):
        """Test upload_ranking_plots generates and uploads ranking plots."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=False, output_path="/tmp"
        )

        # Mock sngl_inspirals
        mock_sngl = mock.MagicMock()
        mock_sngl.ifo = "H1"
        mock_sngl.snr = 10.0
        mock_sngl.chisq = 1.0
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        # Mock coinc_event
        mock_coinc_event = mock.MagicMock()
        mock_coinc_event.likelihood = 20.0
        ll_inspiral_event_plotter.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_event
        ]

        # Mock rankingstat
        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.instruments = ["H1"]
        mock_rankingstat.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        # Mock plot functions
        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plot_snr_chi_pdf.return_value = mock_fig
        ll_inspiral_event_plotter.plot_horizon_distance_vs_time.return_value = mock_fig
        ll_inspiral_event_plotter.plot_rates.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
            "time": mock.MagicMock(),
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_plots(event)

        ll_inspiral_event_plotter.plot_snr_chi_pdf.assert_called()
        ll_inspiral_event_plotter.plot_horizon_distance_vs_time.assert_called()
        ll_inspiral_event_plotter.plot_rates.assert_called()

    def test_upload_ranking_plots_snr_below_threshold(self):
        """Test upload_ranking_plots with SNR below 4.0 threshold."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=True, output_path="/tmp"
        )

        # Mock sngl_inspirals with low SNR
        mock_sngl = mock.MagicMock()
        mock_sngl.ifo = "H1"
        mock_sngl.snr = 3.0  # Below threshold
        mock_sngl.chisq = 1.0
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        mock_coinc_event = mock.MagicMock()
        ll_inspiral_event_plotter.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_event
        ]

        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.instruments = ["H1"]
        mock_rankingstat.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plot_snr_chi_pdf.return_value = mock_fig
        ll_inspiral_event_plotter.plot_horizon_distance_vs_time.return_value = mock_fig
        ll_inspiral_event_plotter.plot_rates.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
            "time": mock.MagicMock(),
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_plots(event)

    def test_upload_ranking_plots_multiple_coinc_events_raises(self):
        """Test upload_ranking_plots raises with multiple coinc events."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = (
            []
        )
        ll_inspiral_event_plotter.lsctables.CoincTable.get_table.return_value = [
            mock.MagicMock(),
            mock.MagicMock(),
        ]

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
            "time": mock.MagicMock(),
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            with pytest.raises(ValueError, match="exactly one candidate"):
                plotter.upload_ranking_plots(event)

    def test_upload_ranking_plots_no_sngl_for_instrument(self):
        """Test upload_ranking_plots when instrument not in sngl_inspirals."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=True, output_path="/tmp"
        )

        # No sngl_inspirals for the instrument
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = (
            []
        )

        mock_coinc_event = mock.MagicMock()
        ll_inspiral_event_plotter.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_event
        ]

        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.instruments = ["H1"]  # But no sngl for H1
        mock_rankingstat.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plot_snr_chi_pdf.return_value = mock_fig
        ll_inspiral_event_plotter.plot_horizon_distance_vs_time.return_value = mock_fig
        ll_inspiral_event_plotter.plot_rates.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
            "time": mock.MagicMock(),
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_plots(event)

    def test_upload_ranking_plots_fig_is_none(self):
        """Test upload_ranking_plots when plot_snr_chi_pdf returns None."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=True, output_path="/tmp"
        )

        mock_sngl = mock.MagicMock()
        mock_sngl.ifo = "H1"
        mock_sngl.snr = 10.0
        mock_sngl.chisq = 1.0
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        mock_coinc_event = mock.MagicMock()
        ll_inspiral_event_plotter.lsctables.CoincTable.get_table.return_value = [
            mock_coinc_event
        ]

        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.instruments = ["H1"]
        mock_rankingstat.terms = {"P_of_SNR_chisq": mock.MagicMock()}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        # Return None for some plots
        ll_inspiral_event_plotter.plot_snr_chi_pdf.return_value = None
        ll_inspiral_event_plotter.plot_horizon_distance_vs_time.return_value = (
            mock.MagicMock()
        )
        ll_inspiral_event_plotter.plot_rates.return_value = mock.MagicMock()

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
            "time": mock.MagicMock(),
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_ranking_plots(event)

    def test_upload_snr_plots(self):
        """Test upload_snr_plots generates and uploads SNR plots."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=False, output_path="/tmp"
        )

        # Mock COMPLEX8TimeSeries
        mock_ts = mock.MagicMock()
        mock_ts.data.length = 100
        mock_ts.data.data = ll_inspiral_event_plotter.numpy.zeros(100)
        mock_ts.epoch = 1000000000.0
        mock_ts.deltaT = 1.0 / 16384

        # Mock getElementsByTagName
        mock_elem = mock.MagicMock()
        mock_elem.hasAttribute.return_value = True
        mock_elem.Name = "COMPLEX8TimeSeries"
        mock_coinc = mock.MagicMock()
        mock_coinc.getElementsByTagName.return_value = [mock_elem]

        # Mock ligolw_param.get_param
        ll_inspiral_event_plotter.ligolw_param.get_param.return_value.value = 1

        # Mock series.parse_COMPLEX8TimeSeries
        ll_inspiral_event_plotter.series.parse_COMPLEX8TimeSeries.return_value = mock_ts

        # Mock sngl_inspiral
        mock_sngl = mock.MagicMock()
        mock_sngl.event_id = 1
        mock_sngl.ifo = "H1"
        mock_sngl.end = 1000000000.5
        mock_sngl.Gamma0 = 1
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        # Mock process params for SVD bank
        mock_process_param = mock.MagicMock()
        mock_process_param.param = "--svd-bank"
        mock_process_param.value = "/path/to/H1-0001-bank.h5"
        process_params_table = ll_inspiral_event_plotter.lsctables.ProcessParamsTable
        process_params_table.get_table.return_value = [mock_process_param]

        # Mock svd_bank
        mock_bank = mock.MagicMock()
        mock_bank_row = mock.MagicMock()
        mock_bank_row.Gamma0 = 1
        mock_bank.sngl_inspiral_table = [mock_bank_row]
        mock_bank.autocorrelation_bank = [ll_inspiral_event_plotter.numpy.zeros(100)]
        ll_inspiral_event_plotter.svd_bank.parse_svdbank_string.return_value = {}
        ll_inspiral_event_plotter.svd_bank.parse_bank_files.return_value = {
            "H1": [mock_bank]
        }

        # Mock numpy functions
        ll_inspiral_event_plotter.numpy.linspace.return_value = (
            ll_inspiral_event_plotter.numpy.arange(108)
        )
        ll_inspiral_event_plotter.numpy.concatenate.return_value = (
            ll_inspiral_event_plotter.numpy.zeros(108)
        )
        ll_inspiral_event_plotter.numpy.argmin.return_value = 54
        ll_inspiral_event_plotter.numpy.angle.return_value = 0.0
        ll_inspiral_event_plotter.numpy.exp.return_value = 1.0
        ll_inspiral_event_plotter.numpy.sqrt.return_value = 1.414

        # Mock figure
        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.figure.Figure.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock_coinc,
            "bin": "0001",
            "snr_optimized": False,
        }

        plotter.upload_snr_plots(event)

    def test_upload_snr_plots_snr_optimized(self):
        """Test upload_snr_plots with snr_optimized event."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=True, output_path="/tmp"
        )

        # Mock COMPLEX8TimeSeries
        mock_ts = mock.MagicMock()
        mock_ts.data.length = 100
        mock_ts.data.data = ll_inspiral_event_plotter.numpy.zeros(100)
        mock_ts.epoch = 1000000000.0
        mock_ts.deltaT = 1.0 / 16384

        mock_elem = mock.MagicMock()
        mock_elem.hasAttribute.return_value = True
        mock_elem.Name = "COMPLEX8TimeSeries"
        mock_coinc = mock.MagicMock()
        mock_coinc.getElementsByTagName.return_value = [mock_elem]

        ll_inspiral_event_plotter.ligolw_param.get_param.return_value.value = 1
        ll_inspiral_event_plotter.series.parse_COMPLEX8TimeSeries.return_value = mock_ts

        mock_sngl = mock.MagicMock()
        mock_sngl.event_id = 1
        mock_sngl.ifo = "H1"
        mock_sngl.end = 1000000000.5
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        ll_inspiral_event_plotter.numpy.linspace.return_value = (
            ll_inspiral_event_plotter.numpy.arange(108)
        )
        ll_inspiral_event_plotter.numpy.concatenate.return_value = (
            ll_inspiral_event_plotter.numpy.zeros(108)
        )
        ll_inspiral_event_plotter.numpy.argmin.return_value = 54
        ll_inspiral_event_plotter.numpy.angle.return_value = 0.0
        ll_inspiral_event_plotter.numpy.exp.return_value = 1.0
        ll_inspiral_event_plotter.numpy.sqrt.return_value = 1.414

        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.figure.Figure.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock_coinc,
            "bin": "0001",
            "snr_optimized": True,  # Skip autocorrelation
        }

        plotter.upload_snr_plots(event)

    def test_upload_snr_plots_template_not_found_raises(self):
        """Test upload_snr_plots raises when template not found in banks."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(ll_inspiral_event_plotter)

        mock_ts = mock.MagicMock()
        mock_ts.data.length = 100

        mock_elem = mock.MagicMock()
        mock_elem.hasAttribute.return_value = True
        mock_elem.Name = "COMPLEX8TimeSeries"
        mock_coinc = mock.MagicMock()
        mock_coinc.getElementsByTagName.return_value = [mock_elem]

        ll_inspiral_event_plotter.ligolw_param.get_param.return_value.value = 1
        ll_inspiral_event_plotter.series.parse_COMPLEX8TimeSeries.return_value = mock_ts

        mock_sngl = mock.MagicMock()
        mock_sngl.event_id = 1
        mock_sngl.ifo = "H1"
        mock_sngl.Gamma0 = 999  # Different from bank template
        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl
        ]

        mock_process_param = mock.MagicMock()
        mock_process_param.param = "--svd-bank"
        mock_process_param.value = "/path/to/H1-0001-bank.h5"
        process_params_table = ll_inspiral_event_plotter.lsctables.ProcessParamsTable
        process_params_table.get_table.return_value = [mock_process_param]

        mock_bank = mock.MagicMock()
        mock_bank_row = mock.MagicMock()
        mock_bank_row.Gamma0 = 1  # Different from trigger
        mock_bank.sngl_inspiral_table = [mock_bank_row]
        ll_inspiral_event_plotter.svd_bank.parse_bank_files.return_value = {
            "H1": [mock_bank]
        }

        event = {
            "gid": "G12345",
            "coinc": mock_coinc,
            "bin": "0001",
            "snr_optimized": False,
        }

        with pytest.raises(ValueError, match="do not contain the template"):
            plotter.upload_snr_plots(event)

    def test_upload_dtdphi_plots(self):
        """Test upload_dtdphi_plots generates and uploads dtdphi plots."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=False, output_path="/tmp"
        )

        # Mock sngl_inspiral_table
        mock_sngl1 = mock.MagicMock()
        mock_sngl1.event_id = 1
        mock_sngl1.template_id = 1
        mock_sngl1.ifo = "H1"
        mock_sngl1.end = 1000000000.0
        mock_sngl1.snr = 10.0
        mock_sngl1.chisq = 1.0
        mock_sngl1.coa_phase = 0.0
        mock_sngl1.Gamma2 = 1.0
        mock_sngl1.template_duration = 1.0

        mock_sngl2 = mock.MagicMock()
        mock_sngl2.event_id = 2
        mock_sngl2.template_id = 1
        mock_sngl2.ifo = "L1"
        mock_sngl2.end = 1000000000.01
        mock_sngl2.snr = 9.0
        mock_sngl2.chisq = 1.1
        mock_sngl2.coa_phase = 0.1
        mock_sngl2.Gamma2 = 1.0
        mock_sngl2.template_duration = 1.0

        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl1,
            mock_sngl2,
        ]

        # Mock offset_vectors
        mock_timeslide = mock.MagicMock()
        mock_timeslide.as_dict.return_value = {0: {"H1": 0.0, "L1": 0.0}}
        ll_inspiral_event_plotter.lsctables.TimeSlideTable.get_table.return_value = (
            mock_timeslide
        )

        # Mock rankingstat
        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.kwargs_from_triggers.return_value = {
            "snrs": {"H1": 10.0, "L1": 9.0},
            "segments": {"H1": mock.MagicMock(), "L1": mock.MagicMock()},
            "template_id": 1,
            "dt": {"H1": 0.0, "L1": 0.01},
            "dphi": {"H1": 0.0, "L1": 0.1},
        }
        mock_rankingstat.terms = {
            "P_of_tref_Dh": mock.MagicMock(),
            "P_of_dt_dphi": mock.MagicMock(),
        }
        mock_rankingstat.terms["P_of_tref_Dh"].horizon_history = {
            "H1": [1.0],
            "L1": [1.0],
        }
        mock_rankingstat.terms[
            "P_of_tref_Dh"
        ].local_mean_horizon_distance.return_value = {"H1": 100.0, "L1": 100.0}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        # Mock plot function
        mock_fig = mock.MagicMock()
        ll_inspiral_event_plotter.plot_dtdphi.return_value = mock_fig

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_dtdphi_plots(event)

        ll_inspiral_event_plotter.plot_dtdphi.assert_called()

    def test_upload_dtdphi_plots_trigger_with_none_chisq(self):
        """Test upload_dtdphi_plots skips triggers with None chisq."""
        from sgnl.bin import ll_inspiral_event_plotter

        plotter = self._create_event_plotter(
            ll_inspiral_event_plotter, no_upload=True, output_path="/tmp"
        )

        # Trigger with None chisq should be skipped
        mock_sngl1 = mock.MagicMock()
        mock_sngl1.chisq = None  # This should be skipped

        mock_sngl2 = mock.MagicMock()
        mock_sngl2.event_id = 2
        mock_sngl2.template_id = 1
        mock_sngl2.ifo = "H1"
        mock_sngl2.end = 1000000000.0
        mock_sngl2.snr = 10.0
        mock_sngl2.chisq = 1.0
        mock_sngl2.coa_phase = 0.0
        mock_sngl2.Gamma2 = 1.0
        mock_sngl2.template_duration = 1.0

        ll_inspiral_event_plotter.lsctables.SnglInspiralTable.get_table.return_value = [
            mock_sngl1,
            mock_sngl2,
        ]

        mock_timeslide = mock.MagicMock()
        mock_timeslide.as_dict.return_value = {0: {"H1": 0.0}}
        ll_inspiral_event_plotter.lsctables.TimeSlideTable.get_table.return_value = (
            mock_timeslide
        )

        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.kwargs_from_triggers.return_value = {
            "snrs": {"H1": 10.0},
            "segments": {"H1": mock.MagicMock()},
            "template_id": 1,
            "dt": {"H1": 0.0},
            "dphi": {"H1": 0.0},
        }
        mock_rankingstat.terms = {
            "P_of_tref_Dh": mock.MagicMock(),
            "P_of_dt_dphi": mock.MagicMock(),
        }
        mock_rankingstat.terms["P_of_tref_Dh"].horizon_history = {"H1": [1.0]}
        mock_rankingstat.terms[
            "P_of_tref_Dh"
        ].local_mean_horizon_distance.return_value = {"H1": 100.0}
        ll_inspiral_event_plotter.LnLikelihoodRatio.from_xml.return_value = (
            mock_rankingstat
        )

        # With only one ifo, no dtdphi plots should be generated
        ll_inspiral_event_plotter.plot_dtdphi.reset_mock()

        event = {
            "gid": "G12345",
            "coinc": mock.MagicMock(),
            "ranking_data_path": "/path/to/ranking.xml",
        }

        with mock.patch.object(
            ll_inspiral_event_plotter,
            "load_rankingstat_xml_with_retries",
            return_value=mock.MagicMock(),
        ):
            plotter.upload_dtdphi_plots(event)


class TestMain:
    """Tests for main function."""

    def test_main_default_plots(self):
        """Test main uses all plots by default."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.EventPlotter = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--kafka-server",
                "kafka:9092",
                "--upload-topic",
                "uploads",
                "--ranking-stat-topic",
                "ranking",
                "--output-path",
                "/tmp",
            ],
        ):
            ll_inspiral_event_plotter.main()

        call_kwargs = ll_inspiral_event_plotter.EventPlotter.call_args[1]
        assert "RANKING_DATA" in call_kwargs["plot"]
        assert "RANKING_PLOTS" in call_kwargs["plot"]
        assert "SNR_PLOTS" in call_kwargs["plot"]
        assert "PSD_PLOTS" in call_kwargs["plot"]
        assert "DTDPHI_PLOTS" in call_kwargs["plot"]

    def test_main_specific_plots(self):
        """Test main with specific plots."""
        from sgnl.bin import ll_inspiral_event_plotter

        ll_inspiral_event_plotter.EventPlotter = mock.MagicMock()

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--kafka-server",
                "kafka:9092",
                "--upload-topic",
                "uploads",
                "--ranking-stat-topic",
                "ranking",
                "--output-path",
                "/tmp",
                "--plot",
                "PSD_PLOTS",
            ],
        ):
            ll_inspiral_event_plotter.main()

        call_kwargs = ll_inspiral_event_plotter.EventPlotter.call_args[1]
        assert call_kwargs["plot"] == ["PSD_PLOTS"]

    def test_main_invalid_plot_raises(self):
        """Test main raises ValueError for invalid plot."""
        from sgnl.bin import ll_inspiral_event_plotter

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--kafka-server",
                "kafka:9092",
                "--upload-topic",
                "uploads",
                "--ranking-stat-topic",
                "ranking",
                "--output-path",
                "/tmp",
                "--plot",
                "INVALID_PLOT",
            ],
        ):
            with pytest.raises(ValueError, match="Unsupported option"):
                ll_inspiral_event_plotter.main()

    def test_main_starts_event_plotter(self):
        """Test main starts the event plotter."""
        from sgnl.bin import ll_inspiral_event_plotter

        mock_plotter = mock.MagicMock()
        ll_inspiral_event_plotter.EventPlotter.return_value = mock_plotter

        with mock.patch(
            "sys.argv",
            [
                "ll_inspiral_event_plotter",
                "--kafka-server",
                "kafka:9092",
                "--upload-topic",
                "uploads",
                "--ranking-stat-topic",
                "ranking",
                "--output-path",
                "/tmp",
            ],
        ):
            ll_inspiral_event_plotter.main()

        mock_plotter.start.assert_called_once()
