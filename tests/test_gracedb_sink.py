"""Tests for sgnl.sinks.gracedb_sink module."""

from unittest import mock

import lal
import numpy
import pytest
from igwn_ligolw import ligolw, lsctables
from lal import LIGOTimeGPS

from sgnl.sinks import gracedb_sink


class TestCeilPow2:
    """Tests for ceil_pow_2 function."""

    def test_power_of_2_unchanged(self):
        """Test that powers of 2 are unchanged."""
        assert gracedb_sink.ceil_pow_2(2) == 2
        assert gracedb_sink.ceil_pow_2(4) == 4
        assert gracedb_sink.ceil_pow_2(8) == 8
        assert gracedb_sink.ceil_pow_2(16) == 16

    def test_round_up_to_power_of_2(self):
        """Test that values are rounded up to nearest power of 2."""
        assert gracedb_sink.ceil_pow_2(3) == 4
        assert gracedb_sink.ceil_pow_2(5) == 8
        assert gracedb_sink.ceil_pow_2(9) == 16
        assert gracedb_sink.ceil_pow_2(100) == 128

    def test_ceil_pow_2_one(self):
        """Test with value 1."""
        assert gracedb_sink.ceil_pow_2(1) == 1

    def test_ceil_pow_2_float(self):
        """Test with float values."""
        assert gracedb_sink.ceil_pow_2(3.5) == 4
        assert gracedb_sink.ceil_pow_2(100.1) == 128


class TestNsToGps:
    """Tests for ns_to_gps function."""

    def test_basic_conversion(self):
        """Test basic nanosecond to GPS conversion."""
        result = gracedb_sink.ns_to_gps(1000000000_000000000)
        assert isinstance(result, LIGOTimeGPS)
        assert int(result) == 1000000000

    def test_with_nanoseconds(self):
        """Test conversion with nanosecond component."""
        result = gracedb_sink.ns_to_gps(1000000000_500000000)
        assert int(result) == 1000000000
        assert result.gpsNanoSeconds == 500000000


class TestColMap:
    """Tests for col_map function."""

    def test_direct_mapping(self):
        """Test direct column mapping without renaming."""

        class MockRow:
            pass

        row = MockRow()
        datadict = {"col1": 100, "col2": "test"}
        gracedb_sink.col_map(row, datadict, {}, {})
        assert row.col1 == 100
        assert row.col2 == "test"

    def test_with_rename_mapping(self):
        """Test column mapping with rename."""

        class MockRow:
            pass

        row = MockRow()
        datadict = {"old_name": 42}
        mapdict = {"old_name": "new_name"}
        gracedb_sink.col_map(row, datadict, mapdict, {})
        assert row.new_name == 42
        assert not hasattr(row, "old_name")

    def test_exclude_column(self):
        """Test that columns mapped to None are excluded."""

        class MockRow:
            pass

        row = MockRow()
        datadict = {"keep_col": 1, "skip_col": 2}
        mapdict = {"skip_col": None}
        gracedb_sink.col_map(row, datadict, mapdict, {})
        assert row.keep_col == 1
        assert not hasattr(row, "skip_col")

    def test_with_function_mapping(self):
        """Test column mapping with function transform."""

        class MockRow:
            pass

        row = MockRow()
        datadict = {"value": 10}
        funcdict = {"value": lambda x: x * 2}
        gracedb_sink.col_map(row, datadict, {}, funcdict)
        assert row.value == 20


class TestAddTableWithNRows:
    """Tests for add_table_with_n_rows function."""

    def test_add_empty_table(self):
        """Test adding a table with 0 rows."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        table = gracedb_sink.add_table_with_n_rows(xmldoc, lsctables.ProcessTable, 0)
        assert len(table) == 0

    def test_add_table_with_one_row(self):
        """Test adding a table with 1 row."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        table = gracedb_sink.add_table_with_n_rows(xmldoc, lsctables.ProcessTable, 1)
        assert len(table) == 1

    def test_add_table_with_multiple_rows(self):
        """Test adding a table with multiple rows."""
        xmldoc = ligolw.Document()
        xmldoc.appendChild(ligolw.LIGO_LW())
        table = gracedb_sink.add_table_with_n_rows(xmldoc, lsctables.TimeSlideTable, 3)
        assert len(table) == 3


class TestLIGOLWContentHandler:
    """Tests for LIGOLWContentHandler class."""

    def test_content_handler_exists(self):
        """Test that LIGOLWContentHandler is properly defined."""
        assert gracedb_sink.LIGOLWContentHandler is not None
        assert issubclass(
            gracedb_sink.LIGOLWContentHandler, ligolw.LIGOLWContentHandler
        )


class TestBestEvent:
    """Tests for best_event function."""

    def test_empty_events(self):
        """Test with empty events list."""
        result = gracedb_sink.best_event([], [], [], 1.0, 1e-6)
        assert result == (None, None, None)

    def test_no_events_below_threshold(self):
        """Test when no events are below threshold."""
        events = [
            {"combined_far": 10.0, "network_snr": 8.0},
            {"combined_far": 20.0, "network_snr": 9.0},
        ]
        triggers = [[{"ifo": "H1"}], [{"ifo": "L1"}]]
        snr_ts = [None, None]
        result = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-6)
        assert result == (None, None, None)

    def test_event_below_opa_threshold(self):
        """Test selecting event below OPA threshold by max SNR."""
        events = [
            {"combined_far": 1e-7, "network_snr": 8.0},
            {"combined_far": 1e-8, "network_snr": 12.0},  # Higher SNR
        ]
        triggers = [[{"ifo": "H1"}], [{"ifo": "L1"}]]
        snr_ts = ["ts1", "ts2"]
        event, trigs, snr = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-6)
        assert event["network_snr"] == 12.0

    def test_event_below_regular_threshold(self):
        """Test selecting event below regular threshold by min FAR."""
        events = [
            {"combined_far": 0.5, "network_snr": 8.0},
            {"combined_far": 0.1, "network_snr": 6.0},  # Lower FAR
        ]
        triggers = [[{"ifo": "H1"}], [{"ifo": "L1"}]]
        snr_ts = ["ts1", "ts2"]
        event, trigs, snr = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-3)
        assert event["combined_far"] == 0.1

    def test_filters_none_combined_far(self):
        """Test that events with None combined_far are filtered out."""
        events = [
            {"combined_far": None, "network_snr": 100.0},
            {"combined_far": 0.5, "network_snr": 8.0},
        ]
        triggers = [[{"ifo": "H1"}], [{"ifo": "L1"}]]
        snr_ts = ["ts1", "ts2"]
        event, trigs, snr = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-3)
        assert event["combined_far"] == 0.5

    def test_filters_zero_combined_far(self):
        """Test that events with 0 combined_far are filtered out."""
        events = [
            {"combined_far": 0, "network_snr": 100.0},
            {"combined_far": 0.5, "network_snr": 8.0},
        ]
        triggers = [[{"ifo": "H1"}], [{"ifo": "L1"}]]
        snr_ts = ["ts1", "ts2"]
        event, trigs, snr = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-3)
        assert event["combined_far"] == 0.5

    def test_filters_none_triggers(self):
        """Test that None triggers are filtered from result."""
        events = [{"combined_far": 0.1, "network_snr": 8.0}]
        triggers = [[{"ifo": "H1"}, None, {"ifo": "L1"}]]
        snr_ts = ["ts1"]
        event, trigs, snr = gracedb_sink.best_event(events, triggers, snr_ts, 1.0, 1e-3)
        assert len(trigs) == 2


class TestSnglMap:
    """Tests for sngl_map function."""

    def test_basic_mapping(self):
        """Test basic sngl_map functionality."""

        class MockRow:
            pass

        class MockSngl:
            pass

        mock_sngl = MockSngl()
        for col in lsctables.SnglInspiralTable.validcolumns:
            col_name = col.split(":")[-1]
            setattr(mock_sngl, col_name, 0)
        mock_sngl.mass1 = 1.4
        mock_sngl.mass2 = 1.4

        sngls_dict = {1: mock_sngl}
        row = MockRow()

        gracedb_sink.sngl_map(row, sngls_dict, 1, exclude=())
        assert row.mass1 == 1.4
        assert row.mass2 == 1.4

    def test_mapping_with_exclude(self):
        """Test sngl_map with excluded columns."""

        class MockRow:
            pass

        class MockSngl:
            pass

        mock_sngl = MockSngl()
        for col in lsctables.SnglInspiralTable.validcolumns:
            col_name = col.split(":")[-1]
            setattr(mock_sngl, col_name, 0)
        mock_sngl.mass1 = 1.4

        sngls_dict = {1: mock_sngl}
        row = MockRow()

        gracedb_sink.sngl_map(row, sngls_dict, 1, exclude=("mass1",))
        assert not hasattr(row, "mass1")


class TestDownsampleSnr:
    """Tests for downsample_snr function."""

    def test_no_downsampling_needed(self):
        """Test when no downsampling is needed."""
        snr_ts = lal.CreateCOMPLEX8TimeSeries(
            name="snr",
            epoch=LIGOTimeGPS(1000000000),
            f0=0.0,
            deltaT=1.0 / 256,  # 256 Hz
            sampleUnits=lal.DimensionlessUnit,
            length=101,
        )
        snr_ts.data.data = numpy.ones(101, dtype=numpy.complex64)

        class MockSngl:
            f_final = 100.0  # ceil_pow_2(2*100) = 256

        result = gracedb_sink.downsample_snr(snr_ts, [MockSngl()])
        # Should return the same series since 256 == 256
        assert result is snr_ts

    def test_downsampling(self):
        """Test downsampling of SNR time series."""
        snr_ts = lal.CreateCOMPLEX8TimeSeries(
            name="snr",
            epoch=LIGOTimeGPS(1000000000),
            f0=0.0,
            deltaT=1.0 / 512,  # 512 Hz
            sampleUnits=lal.DimensionlessUnit,
            length=101,
        )
        snr_ts.data.data = numpy.arange(101, dtype=numpy.complex64)

        class MockSngl:
            f_final = 100.0  # ceil_pow_2(2*100) = 256

        result = gracedb_sink.downsample_snr(snr_ts, [MockSngl()])
        # Should downsample from 512 to 256 (factor of 2)
        assert result.deltaT == 1.0 / 256
        assert len(result.data.data) % 2 == 1


class TestWriteRankingstatXmldocGracedb:
    """Tests for write_rankingstat_xmldoc_gracedb function."""

    def test_empty_horizon_history(self, tmp_path):
        """Test with empty horizon history."""
        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.copy.return_value = mock_rankingstat
        mock_rankingstat.terms = {"P_of_tref_Dh": mock.MagicMock()}
        mock_rankingstat.terms["P_of_tref_Dh"].horizon_history.maxkey.side_effect = (
            ValueError
        )

        output_file = str(tmp_path / "ranking.xml")
        gracedb_sink.write_rankingstat_xmldoc_gracedb(mock_rankingstat, output_file)

        mock_rankingstat.save.assert_called_once_with(output_file)

    def test_with_horizon_history(self, tmp_path):
        """Test with horizon history data."""
        mock_rankingstat = mock.MagicMock()
        mock_rankingstat.copy.return_value = mock_rankingstat
        mock_history = mock.MagicMock()
        mock_rankingstat.terms = {"P_of_tref_Dh": mock.MagicMock()}
        mock_rankingstat.terms["P_of_tref_Dh"].horizon_history.maxkey.return_value = (
            10000.0
        )
        mock_rankingstat.terms["P_of_tref_Dh"].horizon_history.values.return_value = [
            mock_history
        ]

        output_file = str(tmp_path / "ranking.xml")
        gracedb_sink.write_rankingstat_xmldoc_gracedb(mock_rankingstat, output_file)

        # Should trim history and save
        mock_history.__delitem__.assert_called()
        mock_rankingstat.save.assert_called_once_with(output_file)


class TestPublishGracedb:
    """Tests for publish_gracedb function."""

    @mock.patch("sgnl.sinks.gracedb_sink.ligolw_utils.write_fileobj")
    @mock.patch("sgnl.sinks.gracedb_sink.logo_data")
    def test_publish_gracedb(self, mock_logo, mock_write):
        """Test publishing to GraceDB."""
        mock_client = mock.MagicMock()
        mock_client.createEvent.return_value.json.return_value = {"graceid": "G123456"}
        mock_client.writeLog.return_value = {"status": "ok"}
        mock_logo.return_value = "dGVzdA=="  # base64 encoded "test"

        xmldoc = mock.MagicMock()

        gracedb_sink.publish_gracedb(
            mock_client,
            xmldoc,
            group="Test",
            pipeline="SGNL",
            search="AllSky",
            labels=["TEST"],
        )

        mock_client.createEvent.assert_called_once()
        mock_client.writeLog.assert_called_once()


class TestPublishKafka:
    """Tests for publish_kafka function."""

    @mock.patch("sgnl.sinks.gracedb_sink.ligolw_utils.write_fileobj")
    @mock.patch("sgnl.sinks.gracedb_sink.ligolw_utils.write_filename")
    @mock.patch("sgnl.sinks.gracedb_sink.write_rankingstat_xmldoc_gracedb")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    def test_publish_kafka_noninj(
        self,
        mock_exists,
        mock_makedirs,
        mock_write_ranking,
        mock_write_file,
        mock_write_obj,
    ):
        """Test publishing to Kafka for non-injection job."""
        mock_exists.return_value = False
        mock_client = mock.MagicMock()
        mock_strike_object = mock.MagicMock()
        mock_strike_object.likelihood_ratio_uploads = {"0001": mock.MagicMock()}

        # Create mock coinc_inspiral row
        mock_coinc_inspiral = mock.MagicMock()
        mock_coinc_inspiral.combined_far = 1e-10
        mock_coinc_inspiral.snr = 10.0
        mock_coinc_inspiral.end_time = 1000000000
        mock_coinc_inspiral.end_time_ns = 0
        mock_coinc_inspiral.mass = 2.8
        mock_coinc_inspiral.end = 1000000000.0

        # Create mock sngl_inspiral row
        mock_sngl = mock.MagicMock()
        mock_sngl.Gamma1 = 1.0

        # Mock the xmldoc and tables
        mock_xmldoc = mock.MagicMock()
        mock_coinc_table = mock.MagicMock()
        mock_coinc_table.__getitem__ = mock.MagicMock(return_value=mock_coinc_inspiral)
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.__getitem__ = mock.MagicMock(return_value=mock_sngl)

        with (
            mock.patch.object(
                lsctables.CoincInspiralTable, "get_table", return_value=mock_coinc_table
            ),
            mock.patch.object(
                lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
            ),
        ):
            gracedb_sink.publish_kafka(
                mock_client,
                mock_xmldoc,
                job_type="noninj_test",
                analysis_tag="test",
                instruments="H1L1",
                group="Test",
                search="AllSky",
                strike_object=mock_strike_object,
            )

        assert mock_client.produce.call_count == 2
        assert mock_client.poll.call_count == 2

    @mock.patch("sgnl.sinks.gracedb_sink.ligolw_utils.write_fileobj")
    @mock.patch("sgnl.sinks.gracedb_sink.ligolw_utils.write_filename")
    @mock.patch("sgnl.sinks.gracedb_sink.write_rankingstat_xmldoc_gracedb")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    def test_publish_kafka_inj(
        self,
        mock_exists,
        mock_makedirs,
        mock_write_ranking,
        mock_write_file,
        mock_write_obj,
    ):
        """Test publishing to Kafka for injection job."""
        mock_exists.return_value = True  # Directory already exists
        mock_client = mock.MagicMock()
        mock_strike_object = mock.MagicMock()
        mock_strike_object.likelihood_ratio_uploads = {"0001": mock.MagicMock()}

        mock_coinc_inspiral = mock.MagicMock()
        mock_coinc_inspiral.combined_far = 1e-10
        mock_coinc_inspiral.snr = 10.0
        mock_coinc_inspiral.end_time = 1000000000
        mock_coinc_inspiral.end_time_ns = 0
        mock_coinc_inspiral.mass = 2.8
        mock_coinc_inspiral.end = 1000000000.0

        mock_sngl = mock.MagicMock()
        mock_sngl.Gamma1 = 1.0

        mock_xmldoc = mock.MagicMock()
        mock_coinc_table = mock.MagicMock()
        mock_coinc_table.__getitem__ = mock.MagicMock(return_value=mock_coinc_inspiral)
        mock_sngl_table = mock.MagicMock()
        mock_sngl_table.__getitem__ = mock.MagicMock(return_value=mock_sngl)

        with (
            mock.patch.object(
                lsctables.CoincInspiralTable, "get_table", return_value=mock_coinc_table
            ),
            mock.patch.object(
                lsctables.SnglInspiralTable, "get_table", return_value=mock_sngl_table
            ),
        ):
            gracedb_sink.publish_kafka(
                mock_client,
                mock_xmldoc,
                job_type="inj_test",
                analysis_tag="test",
                instruments="H1L1",
                group="Test",
                search="AllSky",
                strike_object=mock_strike_object,
            )

        # Check that inj prefix is used
        call_args = mock_client.produce.call_args_list[0]
        assert "inj_events" in call_args[1]["topic"]
        # makedirs should not be called since dir exists
        mock_makedirs.assert_not_called()


class TestGraceDBSink:
    """Tests for GraceDBSink class."""

    @mock.patch("sgnl.sinks.gracedb_sink.Producer")
    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.__post_init__")
    def test_init_with_kafka(self, mock_super_init, mock_producer):
        """Test initialization with Kafka client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.strike_object = mock.MagicMock()
        sink.event_pad = "events"
        sink.spectrum_pads = ("psd_H1", "psd_L1")
        sink.far_thresh = 1.0
        sink.aggregator_far_thresh = 3.84e-07
        sink.aggregator_far_trials_factor = 1
        sink.output_kafka_server = "localhost:9092"
        sink.gracedb_service_url = None
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = None
        sink.gracedb_cred_reload = True
        sink.gracedb_reload_buffer = 300
        sink.template_sngls = None
        sink.analysis_tag = "test"
        sink.job_type = "noninj"
        sink.analysis_ifos = None
        sink.process_params = None
        sink.delta_t = 0.005
        sink.channel_dict = None
        sink.autocorrelation_lengths = None
        sink.name = "test_sink"

        # Call __post_init__ manually
        gracedb_sink.GraceDBSink.__post_init__(sink)

        mock_producer.assert_called_once()

    @mock.patch("sgnl.sinks.gracedb_sink.GraceDb")
    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.__post_init__")
    def test_init_with_gracedb(self, mock_super_init, mock_gracedb):
        """Test initialization with GraceDB client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.strike_object = mock.MagicMock()
        sink.event_pad = "events"
        sink.spectrum_pads = ("psd_H1", "psd_L1")
        sink.far_thresh = 1.0
        sink.aggregator_far_thresh = 3.84e-07
        sink.aggregator_far_trials_factor = 1
        sink.output_kafka_server = None
        sink.gracedb_service_url = "https://gracedb.ligo.org/api/"
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = None
        sink.gracedb_cred_reload = True
        sink.gracedb_reload_buffer = 300
        sink.template_sngls = None
        sink.analysis_tag = "test"
        sink.job_type = "noninj"
        sink.analysis_ifos = None
        sink.process_params = None
        sink.delta_t = 0.005
        sink.channel_dict = None
        sink.autocorrelation_lengths = None
        sink.name = "test_sink"

        gracedb_sink.GraceDBSink.__post_init__(sink)

        mock_gracedb.assert_called_once()

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.__post_init__")
    def test_init_no_client(self, mock_super_init):
        """Test initialization without any client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.strike_object = mock.MagicMock()
        sink.event_pad = "events"
        sink.spectrum_pads = ("psd_H1", "psd_L1")
        sink.far_thresh = 1.0
        sink.aggregator_far_thresh = 3.84e-07
        sink.aggregator_far_trials_factor = 1
        sink.output_kafka_server = None
        sink.gracedb_service_url = None
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = None
        sink.gracedb_cred_reload = True
        sink.gracedb_reload_buffer = 300
        sink.template_sngls = None
        sink.analysis_tag = "test"
        sink.job_type = "noninj"
        sink.analysis_ifos = None
        sink.process_params = None
        sink.delta_t = 0.005
        sink.channel_dict = None
        sink.autocorrelation_lengths = None
        sink.name = "test_sink"

        gracedb_sink.GraceDBSink.__post_init__(sink)

        assert sink.client is None

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.__post_init__")
    def test_init_assertion_both_clients(self, mock_super_init):
        """Test that assertion fails if both kafka server and gracedb url are truthy."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.strike_object = mock.MagicMock()
        sink.event_pad = "events"
        sink.spectrum_pads = ("psd_H1",)
        sink.far_thresh = 1.0
        sink.aggregator_far_thresh = 3.84e-07
        sink.aggregator_far_trials_factor = 1
        # Both are truthy strings - should fail assertion
        sink.output_kafka_server = "localhost:9092"
        sink.gracedb_service_url = "https://gracedb.ligo.org/api/"
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = None
        sink.gracedb_cred_reload = True
        sink.gracedb_reload_buffer = 300
        sink.template_sngls = None
        sink.analysis_tag = "test"
        sink.job_type = "noninj"
        sink.analysis_ifos = None
        sink.process_params = None
        sink.delta_t = 0.005
        sink.channel_dict = None
        sink.autocorrelation_lengths = None
        sink.name = "test_sink"

        with pytest.raises(AssertionError):
            gracedb_sink.GraceDBSink.__post_init__(sink)

    def test_pull_eos(self):
        """Test pull method with EOS frame."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.event_pad = "events"
        sink.rsnks = {"pad1": "events"}
        sink.mark_eos = mock.MagicMock()
        sink.events = None

        frame = mock.MagicMock()
        frame.EOS = True
        frame.metadata = {"psd": None}

        sink.pull("pad1", frame)

        sink.mark_eos.assert_called_once_with("pad1")
        assert sink.events == frame

    def test_pull_psd(self):
        """Test pull method with PSD frame."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.event_pad = "events"
        sink.rsnks = {"psd_pad": "psd_H1"}
        sink.psds = {}
        sink.mark_eos = mock.MagicMock()

        mock_psd = mock.MagicMock()
        frame = mock.MagicMock()
        frame.EOS = False
        frame.metadata = {"psd": mock_psd}

        sink.pull("psd_pad", frame)

        assert sink.psds["psd_H1"] == mock_psd

    def test_pull_psd_none(self):
        """Test pull method with None PSD in frame."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.event_pad = "events"
        sink.rsnks = {"psd_pad": "psd_H1"}
        sink.psds = {}
        sink.mark_eos = mock.MagicMock()

        frame = mock.MagicMock()
        frame.EOS = False
        frame.metadata = {"psd": None}

        sink.pull("psd_pad", frame)

        # PSD should not be added since it's None
        assert "psd_H1" not in sink.psds

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    @mock.patch("sgnl.sinks.gracedb_sink.event_trigs_to_coinc_xmldoc")
    @mock.patch("sgnl.sinks.gracedb_sink.best_event")
    @mock.patch("sgnl.sinks.gracedb_sink.publish_gracedb")
    @mock.patch("lal.series.make_psd_xmldoc")
    def test_internal_with_gracedb_client(
        self,
        mock_make_psd,
        mock_publish,
        mock_best_event,
        mock_event_to_xmldoc,
        mock_exchange_state,
    ):
        """Test internal method with GraceDB client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": 1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = ""
        sink.events = mock.MagicMock()
        sink.events.events = [
            {
                "event": mock.MagicMock(),
                "trigger": mock.MagicMock(),
                "snr_ts": mock.MagicMock(),
            }
        ]
        sink.public_far_threshold = 1e-6
        sink.sngls_dict = {}
        sink.analysis_ifos = ["H1", "L1"]
        sink.process_params = {}
        sink.delta_t = 0.005
        sink.channel_dict = {"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"}
        sink.autocorrelation_lengths = {}
        sink.psds = {"H1": mock.MagicMock(), "L1": mock.MagicMock()}
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = []
        sink.strike_object = mock.MagicMock()

        mock_event = {"time_subthresh": 1000000000_000000000}
        mock_best_event.return_value = (
            mock_event,
            [{"ifo": "H1"}],
            {"H1": mock.MagicMock()},
        )

        mock_xmldoc = mock.MagicMock()
        mock_event_to_xmldoc.return_value = mock_xmldoc

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            sink.internal()

        mock_publish.assert_called_once()

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    @mock.patch("sgnl.sinks.gracedb_sink.event_trigs_to_coinc_xmldoc")
    @mock.patch("sgnl.sinks.gracedb_sink.best_event")
    @mock.patch("sgnl.sinks.gracedb_sink.publish_kafka")
    @mock.patch("lal.series.make_psd_xmldoc")
    def test_internal_with_kafka_client(
        self,
        mock_make_psd,
        mock_publish,
        mock_best_event,
        mock_event_to_xmldoc,
        mock_exchange_state,
    ):
        """Test internal method with Kafka client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": 1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = "localhost:9092"
        sink.events = mock.MagicMock()
        sink.events.events = [
            {
                "event": mock.MagicMock(),
                "trigger": mock.MagicMock(),
                "snr_ts": mock.MagicMock(),
            }
        ]
        sink.public_far_threshold = 1e-6
        sink.sngls_dict = {}
        sink.analysis_ifos = ["H1", "L1"]
        sink.process_params = {}
        sink.delta_t = 0.005
        sink.channel_dict = {"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"}
        sink.autocorrelation_lengths = {}
        sink.psds = {"H1": mock.MagicMock(), "L1": mock.MagicMock()}
        sink.gracedb_group = "Test"
        sink.gracedb_search = "AllSky"
        sink.job_type = "noninj"
        sink.analysis_tag = "test"
        sink.strike_object = mock.MagicMock()

        mock_event = {"time_subthresh": 1000000000_000000000}
        mock_best_event.return_value = (
            mock_event,
            [{"ifo": "H1"}],
            {"H1": mock.MagicMock()},
        )

        mock_xmldoc = mock.MagicMock()
        mock_event_to_xmldoc.return_value = mock_xmldoc

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            sink.internal()

        mock_publish.assert_called_once()

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    @mock.patch("sgnl.sinks.gracedb_sink.best_event")
    def test_internal_no_event(self, mock_best_event, mock_exchange_state):
        """Test internal method when no event passes threshold."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": 1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = "localhost:9092"
        sink.events = mock.MagicMock()
        sink.events.events = [
            {
                "event": mock.MagicMock(),
                "trigger": mock.MagicMock(),
                "snr_ts": mock.MagicMock(),
            }
        ]
        sink.public_far_threshold = 1e-6

        mock_best_event.return_value = (None, None, None)

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            sink.internal()

        # Should not attempt to publish
        # Just check no errors

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    def test_internal_skips_none_buffer(self, mock_exchange_state):
        """Test internal method skips None buffers and buffers with None event."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": 1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = "localhost:9092"
        sink.events = mock.MagicMock()
        # Include None buffer and buffer with None event
        sink.events.events = [
            None,  # Should be skipped
            {"event": None, "trigger": mock.MagicMock(), "snr_ts": mock.MagicMock()},
        ]
        sink.public_far_threshold = 1e-6

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            # Should not raise any errors - just skip the None buffers
            sink.internal()

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    def test_internal_negative_far_threshold(self, mock_exchange_state):
        """Test internal method with negative FAR threshold."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": -1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = ""

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            sink.internal()

        # Should not attempt to find or publish events

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    def test_internal_flush_kafka_at_eos(self, mock_exchange_state):
        """Test that Kafka client is flushed at EOS."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": -1.0}
        sink.client = mock.MagicMock()
        sink.output_kafka_server = "localhost:9092"

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=True
        ):
            sink.internal()

        sink.client.flush.assert_called_once()

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.exchange_state")
    def test_internal_no_client(self, mock_exchange_state):
        """Test internal method with no client."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        sink.name = "test"
        sink.state = {"far-threshold": 1.0}
        sink.client = None

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock, return_value=False
        ):
            # Should not raise any errors
            sink.internal()


class TestEventTrigsToCoincXmldoc:
    """Tests for event_trigs_to_coinc_xmldoc function."""

    def _create_mock_sngl(self):
        """Helper to create mock sngl with all columns."""

        class MockSngl:
            pass

        mock_sngl = MockSngl()
        for col in lsctables.SnglInspiralTable.validcolumns:
            col_name = col.split(":")[-1]
            setattr(mock_sngl, col_name, 0)
        mock_sngl.mass1 = 1.4
        mock_sngl.mass2 = 1.4
        mock_sngl.mchirp = 1.2
        mock_sngl.Gamma1 = 1.0
        mock_sngl.f_final = 1024.0  # Required for downsample_snr
        return mock_sngl

    def _create_snr_ts(self, epoch_sec=1000000000, length=4097, sample_rate=4096):
        """Helper to create SNR time series."""
        snr_ts = lal.CreateCOMPLEX8TimeSeries(
            name="snr",
            epoch=LIGOTimeGPS(epoch_sec, 0),
            f0=0.0,
            deltaT=1.0 / sample_rate,
            sampleUnits=lal.DimensionlessUnit,
            length=length,
        )
        snr_ts.data.data = numpy.ones(length, dtype=numpy.complex64) * (1 + 0j)
        return snr_ts

    def test_basic_conversion(self):
        """Test basic conversion of event/triggers to XML doc."""
        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}
        snr_ts = self._create_snr_ts()
        snr_ts_dict = {"H1": snr_ts}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None
        # Check that required tables exist
        assert len(lsctables.SnglInspiralTable.get_table(result)) == 1
        assert len(lsctables.CoincInspiralTable.get_table(result)) == 1
        assert len(lsctables.CoincTable.get_table(result)) == 1

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_with_subthreshold_trigger(self, mock_light_travel):
        """Test with subthreshold trigger detection."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data[2048] = 5 + 0j  # Add a peak

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None
        sngl_table = lsctables.SnglInspiralTable.get_table(result)
        assert len(sngl_table) >= 1

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_not_enough_samples(self, mock_light_travel):
        """Test subthreshold search with insufficient samples."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        # Very short L1 series with different epoch - triggers "not enough samples"
        snr_ts_l1 = self._create_snr_ts(epoch_sec=1000000002, length=10)

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_zero_maxsnr(self, mock_light_travel):
        """Test subthreshold search with zero max SNR."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data = numpy.zeros(4097, dtype=numpy.complex64)

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_padding_front(self, mock_light_travel):
        """Test subthreshold search with padding at front."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_010000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_010000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data[10] = 5 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_padding_end(self, mock_light_travel):
        """Test subthreshold search with padding at end."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_990000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_990000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data[4090] = 5 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_not_enough_samples_for_snippet(self, mock_light_travel):
        """Test subthreshold search when peak is too close to edge for bayestar."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_002000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_002000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data[5] = 5 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_exception_handling(self, mock_light_travel):
        """Test that exceptions in subthreshold search are handled."""
        mock_light_travel.side_effect = Exception("Test exception")

        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data = numpy.ones(4097, dtype=numpy.complex64) * (5 + 0j)

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None


class TestTemplateSnglsInit:
    """Tests for template_sngls initialization in GraceDBSink."""

    @mock.patch("sgnl.sinks.gracedb_sink.HTTPControlSinkElement.__post_init__")
    def test_template_sngls_dict_building(self, mock_super_init):
        """Test that template_sngls are correctly parsed into sngls_dict."""
        with mock.patch.object(gracedb_sink.GraceDBSink, "__post_init__"):
            sink = object.__new__(gracedb_sink.GraceDBSink)

        mock_sngl1 = mock.MagicMock()
        mock_sngl2 = mock.MagicMock()

        sink.strike_object = mock.MagicMock()
        sink.event_pad = "events"
        sink.spectrum_pads = ("psd_H1",)
        sink.far_thresh = 1.0
        sink.aggregator_far_thresh = 3.84e-07
        sink.aggregator_far_trials_factor = 1
        sink.output_kafka_server = None
        sink.gracedb_service_url = None
        sink.gracedb_group = "Test"
        sink.gracedb_pipeline = "SGNL"
        sink.gracedb_search = "AllSky"
        sink.gracedb_label = None
        sink.gracedb_cred_reload = True
        sink.gracedb_reload_buffer = 300
        sink.template_sngls = [{1: mock_sngl1}, {2: mock_sngl2}]
        sink.analysis_tag = "test"
        sink.job_type = "noninj"
        sink.analysis_ifos = None
        sink.process_params = None
        sink.delta_t = 0.005
        sink.channel_dict = None
        sink.autocorrelation_lengths = None
        sink.name = "test_sink"

        gracedb_sink.GraceDBSink.__post_init__(sink)

        assert sink.sngls_dict[1] == mock_sngl1
        assert sink.sngls_dict[2] == mock_sngl2


class TestSubthresholdEdgeCases:
    """Tests for edge cases in subthreshold trigger processing."""

    def _create_mock_sngl(self):
        """Helper to create mock sngl with all columns."""

        class MockSngl:
            pass

        mock_sngl = MockSngl()
        for col in lsctables.SnglInspiralTable.validcolumns:
            col_name = col.split(":")[-1]
            setattr(mock_sngl, col_name, 0)
        mock_sngl.mass1 = 1.4
        mock_sngl.mass2 = 1.4
        mock_sngl.mchirp = 1.2
        mock_sngl.Gamma1 = 1.0
        mock_sngl.f_final = 1024.0
        return mock_sngl

    def _create_snr_ts(self, epoch_sec=1000000000, length=4097, sample_rate=4096):
        """Helper to create SNR time series."""
        snr_ts = lal.CreateCOMPLEX8TimeSeries(
            name="snr",
            epoch=LIGOTimeGPS(epoch_sec, 0),
            f0=0.0,
            deltaT=1.0 / sample_rate,
            sampleUnits=lal.DimensionlessUnit,
            length=length,
        )
        snr_ts.data.data = numpy.ones(length, dtype=numpy.complex64) * (1 + 0j)
        return snr_ts

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_multiple_triggerless_ifos(self, mock_light_travel):
        """Test with multiple triggerless IFOs to hit subthresh_trigs iteration."""
        mock_light_travel.return_value = 0.01

        event = {
            "time_subthresh": 1000000000_500000000,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        # Only H1 has a real trigger; L1 and V1 are triggerless
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": 1000000000_500000000,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data[2048] = 5 + 0j  # Peak for L1
        snr_ts_v1 = self._create_snr_ts()
        snr_ts_v1.data.data[2048] = 4 + 0j  # Peak for V1

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1, "V1": snr_ts_v1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1", "V1"],
            process_params={},
            delta_t=0.005,
            channel_dict={
                "H1": "H1:GDS-CALIB_STRAIN",
                "L1": "L1:GDS-CALIB_STRAIN",
                "V1": "V1:Hrec_hoft_16384Hz",
            },
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None
        sngl_table = lsctables.SnglInspiralTable.get_table(result)
        # Should have H1 + L1 + V1 = 3 triggers
        assert len(sngl_table) == 3

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_bayestar_not_enough_samples(self, mock_light_travel):
        """Test when peak is too close to edge for Bayestar (lines 516-522)."""
        mock_light_travel.return_value = 0.01

        # Trigger time close to epoch start so coincidence segment is near beginning
        # epoch=1000000000, trigger at 1000000000.020
        # coinc_segment ~ [0.020-0.015, 0.020+0.015] = [0.005, 0.035] from epoch
        # idx0 ~ 20, idxf ~ 143 (at 4096 Hz)
        # If peak is at idx 50 within this segment, peak_idx = 50 < min_num_samps=109
        trigger_time_ns = 1000000000_020000000
        event = {
            "time_subthresh": trigger_time_ns,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": trigger_time_ns,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        # L1 with peak at idx 50 (within coincidence segment, < min_num_samps)
        snr_ts_l1 = self._create_snr_ts()
        snr_ts_l1.data.data = numpy.zeros(4097, dtype=numpy.complex64)
        snr_ts_l1.data.data[50] = 10 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            autocorrelation_lengths={"0001": 101},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_snr_padding_front(self, mock_light_travel):
        """Test SNR padding at front when snip0 < 0 (lines 527-533)."""
        mock_light_travel.return_value = 0.01

        # Trigger at 0.040s from epoch puts coincidence segment at [0.025, 0.055]
        # idx range ~ [102, 225] at 4096 Hz
        # Peak at idx 150 will be found, which passes min_num_samps check (109)
        # With half_autocorr=200, snip0 = 150-200 = -50 < 0 → hits padding front
        trigger_time_ns = 1000000000_040000000
        event = {
            "time_subthresh": trigger_time_ns,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": trigger_time_ns,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        # Set peak at index 150: >= min_num_samps (109) but < half_autocorr (200)
        snr_ts_l1.data.data = numpy.zeros(4097, dtype=numpy.complex64)
        snr_ts_l1.data.data[150] = 10 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            # Large autocorrelation so half_autocorr_length=200 > peak_idx=150
            autocorrelation_lengths={"0001": 401},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_snr_padding_end(self, mock_light_travel):
        """Test SNR padding at end when snipf > sniplength (lines 540-546)."""
        mock_light_travel.return_value = 0.01

        # Peak at 3950, epoch=1000000000, dt=1/4096
        # Peak time from epoch = 3950/4096 ≈ 0.965s
        # Trigger at 0.965s puts coinc segment at [0.950, 0.980]
        # idx range ~ [3891, 4014] at 4096 Hz, so peak at 3950 is found
        # sniplength=4097, peak_idx=3950, sniplength-peak_idx=147 >= 109 (passes check)
        # With half_autocorr=200, snipf = 3950+200+1 = 4151 > 4097 → padding end
        trigger_time_ns = 1000000000_965000000
        event = {
            "time_subthresh": trigger_time_ns,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": trigger_time_ns,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        # Peak at idx 3950: close to end
        snr_ts_l1.data.data = numpy.zeros(4097, dtype=numpy.complex64)
        snr_ts_l1.data.data[3950] = 10 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        result = gracedb_sink.event_trigs_to_coinc_xmldoc(
            event,
            trigs,
            snr_ts_dict,
            sngls_dict,
            analysis_ifos=["H1", "L1"],
            process_params={},
            delta_t=0.005,
            channel_dict={"H1": "H1:GDS-CALIB_STRAIN", "L1": "L1:GDS-CALIB_STRAIN"},
            # Large autocorrelation so half_autocorr_length=200
            autocorrelation_lengths={"0001": 401},
        )

        assert result is not None

    @mock.patch("sgnl.sinks.gracedb_sink.light_travel_time")
    def test_subthresh_zero_length_snippet_caught(self, mock_light_travel):
        """Test zero length snippet triggers error (lines 556-565).

        The ValueError is caught by except block and function continues.
        """
        mock_light_travel.return_value = 0.01

        # Use trigger time that positions coinc segment for padding front branch
        trigger_time_ns = 1000000000_040000000
        event = {
            "time_subthresh": trigger_time_ns,
            "network_snr_subthresh": 10.0,
            "combined_far": 1e-10,
            "false_alarm_probability": 0.0,
            "likelihood": 100.0,
        }
        trigs = [
            {
                "_filter_id": 1,
                "ifo": "H1",
                "time": trigger_time_ns,
                "phase": 0.0,
                "chisq": 1.0,
                "snr": 8.0,
            }
        ]

        mock_sngl = self._create_mock_sngl()
        sngls_dict = {1: mock_sngl}

        snr_ts_h1 = self._create_snr_ts()
        snr_ts_l1 = self._create_snr_ts()
        # Peak at idx 150 - will trigger padding front with large autocorr
        snr_ts_l1.data.data = numpy.zeros(4097, dtype=numpy.complex64)
        snr_ts_l1.data.data[150] = 10 + 0j

        snr_ts_dict = {"H1": snr_ts_h1, "L1": snr_ts_l1}

        # Mock numpy.pad to return empty array which triggers ValueError
        # The exception is caught by except block (lines 590-591), not raised
        def mock_pad(*args, **kwargs):
            return numpy.array([], dtype=numpy.complex64)

        with mock.patch.object(gracedb_sink.numpy, "pad", side_effect=mock_pad):
            # Exception is caught internally, function should complete
            result = gracedb_sink.event_trigs_to_coinc_xmldoc(
                event,
                trigs,
                snr_ts_dict,
                sngls_dict,
                analysis_ifos=["H1", "L1"],
                process_params={},
                delta_t=0.005,
                channel_dict={
                    "H1": "H1:GDS-CALIB_STRAIN",
                    "L1": "L1:GDS-CALIB_STRAIN",
                },
                # Large autocorrelation to trigger padding branch
                autocorrelation_lengths={"0001": 401},
            )

        assert result is not None
