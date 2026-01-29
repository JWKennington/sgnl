"""Tests for sgnl.sinks.stillsuit_sink module."""

from queue import Empty
from unittest import mock

import igwn_segments as segments
import pytest

from sgnl.sinks import stillsuit_sink


class TestInitConfigRow:
    """Tests for init_config_row function."""

    def test_init_config_row_basic(self):
        """Test basic config row initialization."""
        table = {
            "columns": [
                {"name": "col1"},
                {"name": "col2"},
                {"name": "__private"},
            ]
        }
        result = stillsuit_sink.init_config_row(table)
        assert result == {"col1": None, "col2": None}
        assert "__private" not in result

    def test_init_config_row_with_extra(self):
        """Test config row initialization with extra values."""
        table = {
            "columns": [
                {"name": "col1"},
                {"name": "col2"},
            ]
        }
        extra = {"col1": "value1", "extra_col": "extra_value"}
        result = stillsuit_sink.init_config_row(table, extra=extra)
        assert result["col1"] == "value1"
        assert result["col2"] is None
        assert result["extra_col"] == "extra_value"


class TestInitStatic:
    """Tests for init_static function."""

    @mock.patch("sgnl.sinks.stillsuit_sink.now")
    @mock.patch("sgnl.sinks.stillsuit_sink.stillsuit.StillSuit")
    def test_init_static_basic(self, mock_stillsuit, mock_now):
        """Test basic static initialization."""
        mock_now.return_value = 1000000000
        mock_db = mock.MagicMock()
        mock_stillsuit.return_value = mock_db

        ifos = ["H1", "L1"]
        config_name = "test_config.yaml"
        process_row = {}
        params = [{"param": "test"}]
        filters = [{"filter": "test"}]

        db, temp_segs = stillsuit_sink.init_static(
            ifos, config_name, process_row, params, filters
        )

        assert db == mock_db
        assert isinstance(temp_segs, segments.segmentlistdict)
        assert "H1" in temp_segs
        assert "L1" in temp_segs
        mock_db.insert_static.assert_any_call({"process_params": params})
        mock_db.insert_static.assert_any_call({"filter": filters})

    @mock.patch("sgnl.sinks.stillsuit_sink.now")
    @mock.patch("sgnl.sinks.stillsuit_sink.stillsuit.StillSuit")
    def test_init_static_with_sims(self, mock_stillsuit, mock_now):
        """Test static initialization with simulations."""
        mock_now.return_value = 1000000000
        mock_db = mock.MagicMock()
        mock_stillsuit.return_value = mock_db

        ifos = ["H1"]
        config_name = "test_config.yaml"
        process_row = {}
        params = []
        filters = []
        sims = [{"sim": "test"}]

        db, temp_segs = stillsuit_sink.init_static(
            ifos, config_name, process_row, params, filters, sims=sims
        )

        mock_db.insert_static.assert_any_call({"simulation": sims})


class TestInitDbs:
    """Tests for init_dbs function."""

    @mock.patch("sgnl.sinks.stillsuit_sink.init_static")
    def test_init_dbs(self, mock_init_static):
        """Test database initialization for multiple banks."""
        mock_db = mock.MagicMock()
        mock_segs = segments.segmentlistdict({"H1": segments.segmentlist()})
        mock_init_static.return_value = (mock_db, mock_segs)

        ifos = ["H1"]
        config_name = "test.yaml"
        bankids = ["0000", "0001"]
        process = {}
        process_params = []
        sim = []
        filters = {"0000": [], "0001": []}

        dbs, temp_segs = stillsuit_sink.init_dbs(
            ifos, config_name, bankids, process, process_params, sim, filters
        )

        assert "0000" in dbs
        assert "0001" in dbs
        assert mock_init_static.call_count == 2


class TestOnSnapshot:
    """Tests for on_snapshot function."""

    @mock.patch("sgnl.sinks.stillsuit_sink.init_static")
    @mock.patch("sgnl.sinks.stillsuit_sink.init_config_row")
    @mock.patch("sgnl.sinks.stillsuit_sink.now")
    @mock.patch("sgnl.sinks.stillsuit_sink.Offset")
    def test_on_snapshot_basic(
        self, mock_offset, mock_now, mock_init_config_row, mock_init_static
    ):
        """Test basic snapshot handling."""
        mock_now.return_value = 1000000000
        mock_offset.tons.side_effect = lambda x: int(x)
        mock_init_config_row.return_value = {}

        mock_db = mock.MagicMock()
        mock_new_db = mock.MagicMock()
        mock_new_segs = segments.segmentlistdict({"H1": segments.segmentlist()})
        mock_init_static.return_value = (mock_new_db, mock_new_segs)

        data = {"snapshot": {"fn": "test.sqlite", "bankid": "0000"}}
        temp_segments = {
            "0000": segments.segmentlistdict(
                {"H1": segments.segmentlist([segments.segment(0, 100)])}
            )
        }
        dbs = {"0000": mock_db}
        ifos = ["H1"]
        config_name = "test.yaml"
        config_segment = {"columns": [{"name": "start_time"}, {"name": "end_time"}]}
        process = {}
        process_params = []
        filters = {"0000": []}
        sim = []

        result = stillsuit_sink.on_snapshot(
            data,
            temp_segments,
            dbs,
            ifos,
            config_name,
            config_segment,
            process,
            process_params,
            filters,
            sim,
            shutdown=False,
        )

        mock_db.to_file.assert_called_once_with("test.sqlite")
        mock_db.db.close.assert_called_once()
        assert result is None

    @mock.patch("sgnl.sinks.stillsuit_sink.StrikeObject.on_snapshot_reload")
    @mock.patch("sgnl.sinks.stillsuit_sink.init_static")
    @mock.patch("sgnl.sinks.stillsuit_sink.init_config_row")
    @mock.patch("sgnl.sinks.stillsuit_sink.now")
    @mock.patch("sgnl.sinks.stillsuit_sink.Offset")
    def test_on_snapshot_with_lr_file(
        self,
        mock_offset,
        mock_now,
        mock_init_config_row,
        mock_init_static,
        mock_reload,
    ):
        """Test snapshot handling with LR file reload."""
        mock_now.return_value = 1000000000
        mock_offset.tons.side_effect = lambda x: int(x)
        mock_init_config_row.return_value = {}
        mock_reload.return_value = {"key": "value"}

        mock_db = mock.MagicMock()
        mock_new_db = mock.MagicMock()
        mock_new_segs = segments.segmentlistdict({"H1": segments.segmentlist()})
        mock_init_static.return_value = (mock_new_db, mock_new_segs)

        data = {
            "snapshot": {"fn": "test.sqlite", "bankid": "0000", "in_lr_file": "lr.h5"}
        }
        temp_segments = {
            "0000": segments.segmentlistdict({"H1": segments.segmentlist()})
        }
        dbs = {"0000": mock_db}
        ifos = ["H1"]
        config_name = "test.yaml"
        config_segment = {"columns": []}
        process = {}
        process_params = []
        filters = {"0000": []}
        sim = []

        result = stillsuit_sink.on_snapshot(
            data,
            temp_segments,
            dbs,
            ifos,
            config_name,
            config_segment,
            process,
            process_params,
            filters,
            sim,
            shutdown=False,
        )

        mock_reload.assert_called_once_with("lr.h5")
        assert result is not None
        assert result["bankid"] == "0000"

    @mock.patch("sgnl.sinks.stillsuit_sink.init_static")
    @mock.patch("sgnl.sinks.stillsuit_sink.init_config_row")
    @mock.patch("sgnl.sinks.stillsuit_sink.now")
    @mock.patch("sgnl.sinks.stillsuit_sink.Offset")
    def test_on_snapshot_shutdown_with_lr_file(
        self, mock_offset, mock_now, mock_init_config_row, mock_init_static
    ):
        """Test snapshot handling during shutdown with LR file (should not reload)."""
        mock_now.return_value = 1000000000
        mock_offset.tons.side_effect = lambda x: int(x)
        mock_init_config_row.return_value = {}

        mock_db = mock.MagicMock()
        mock_new_db = mock.MagicMock()
        mock_new_segs = segments.segmentlistdict({"H1": segments.segmentlist()})
        mock_init_static.return_value = (mock_new_db, mock_new_segs)

        data = {
            "snapshot": {"fn": "test.sqlite", "bankid": "0000", "in_lr_file": "lr.h5"}
        }
        temp_segments = {
            "0000": segments.segmentlistdict({"H1": segments.segmentlist()})
        }
        dbs = {"0000": mock_db}
        ifos = ["H1"]
        config_name = "test.yaml"
        config_segment = {"columns": []}
        process = {}
        process_params = []
        filters = {"0000": []}
        sim = []

        result = stillsuit_sink.on_snapshot(
            data,
            temp_segments,
            dbs,
            ifos,
            config_name,
            config_segment,
            process,
            process_params,
            filters,
            sim,
            shutdown=True,
        )

        # During shutdown, lr_dict should be None even with in_lr_file
        assert result is None


class TestInsertEvent:
    """Tests for insert_event function."""

    def test_insert_event(self):
        """Test event insertion."""
        mock_db = mock.MagicMock()
        dbs = {"0000": mock_db}

        data = {
            "event_dict": {
                "event": [{"bankid": "0000", "snr": 10.0}],
                "trigger": [{"time": 1000000000}],
            }
        }

        stillsuit_sink.insert_event(data, dbs)

        mock_db.insert_event.assert_called_once()


class TestAppendSegment:
    """Tests for append_segment function."""

    def test_append_segment(self):
        """Test segment appending."""
        temp_segments = {
            "0000": segments.segmentlistdict({"H1": segments.segmentlist()}),
            "0001": segments.segmentlistdict({"H1": segments.segmentlist()}),
        }

        data = {"segment": {"H1": (0, 100)}}

        stillsuit_sink.append_segment(data, temp_segments)

        assert len(temp_segments["0000"]["H1"]) == 1
        assert len(temp_segments["0001"]["H1"]) == 1


class TestStillSuitSink:
    """Tests for StillSuitSink class."""

    def _create_mock_config(self):
        """Create a mock config dictionary."""
        return {
            "process": {"columns": [{"name": "ifos"}, {"name": "program"}]},
            "process_params": {"columns": [{"name": "param"}, {"name": "value"}]},
            "filter": {
                "columns": [
                    {"name": "_filter_id"},
                    {"name": "bank_id"},
                    {"name": "mass1"},
                ]
            },
            "segment": {"columns": [{"name": "start_time"}, {"name": "end_time"}]},
            "simulation": {"columns": [{"name": "_simulation_id"}, {"name": "mass1"}]},
        }

    def _create_mock_sngl(self):
        """Create a mock sngl object."""
        sngl = mock.MagicMock()
        sngl.end_time = 1000000000
        sngl.end_time_ns = 0
        sngl.mass1 = 1.4
        sngl.mass2 = 1.4
        sngl.spin1x = 0.0
        sngl.spin1y = 0.0
        sngl.spin1z = 0.0
        sngl.spin2x = 0.0
        sngl.spin2y = 0.0
        sngl.spin2z = 0.0
        sngl.template_duration = 10.0
        return sngl

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_missing_config_name(
        self, mock_snap_init, mock_par_init, mock_yaml, mock_open
    ):
        """Test __post_init__ raises error when config_name is None."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.trigger_output = None
        sink.process_params = None
        sink.injection_list = None
        sink.multiprocess = False
        sink.config_name = None
        sink.is_online = False

        with pytest.raises(ValueError, match="config_name"):
            stillsuit_sink.StillSuitSink.__post_init__(sink)

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_missing_trigger_output_offline(
        self, mock_snap_init, mock_par_init, mock_yaml, mock_open
    ):
        """Test __post_init__ raises error when offline without trigger_output."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.trigger_output = None
        sink.process_params = None
        sink.injection_list = None
        sink.multiprocess = False
        sink.config_name = "test.yaml"
        sink.is_online = False

        with pytest.raises(ValueError, match="trigger_output"):
            stillsuit_sink.StillSuitSink.__post_init__(sink)

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.socket.gethostname")
    @mock.patch("sgnl.sinks.stillsuit_sink.os.getpid")
    def test_post_init_basic(
        self,
        mock_getpid,
        mock_hostname,
        mock_snap_init,
        mock_par_init,
        mock_yaml,
        mock_open,
    ):
        """Test basic __post_init__ setup."""
        mock_getpid.return_value = 12345
        mock_hostname.return_value = "testhost"
        mock_yaml.return_value = self._create_mock_config()

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.trigger_output = {"0000": "output.sqlite"}
        sink.process_params = None
        sink.injection_list = None
        sink.multiprocess = False
        sink.config_name = "test.yaml"
        sink.is_online = False
        sink.ifos = ["H1", "L1"]
        sink.bankids_map = {"0000": [0]}
        sink.template_ids = [0]
        sink.template_sngls = [{0: self._create_mock_sngl()}]
        sink.subbankids = ["0000_0"]
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {}
        sink.program = "test_program"
        sink.injections = False
        sink.nsubbank_pretend = False
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                with mock.patch.object(sink, "get_username", return_value="testuser"):
                    stillsuit_sink.StillSuitSink.__post_init__(sink)

        assert sink.tables == ["trigger", "event"]
        assert not sink.init

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.socket.gethostname")
    @mock.patch("sgnl.sinks.stillsuit_sink.os.getpid")
    def test_post_init_with_injections(
        self,
        mock_getpid,
        mock_hostname,
        mock_snap_init,
        mock_par_init,
        mock_yaml,
        mock_open,
    ):
        """Test __post_init__ with injections enabled."""
        mock_getpid.return_value = 12345
        mock_hostname.return_value = "testhost"
        mock_yaml.return_value = self._create_mock_config()

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        # Create mock injection
        mock_inj = mock.MagicMock()
        mock_inj.simulation_id = 1
        mock_inj.coa_phase = 0.0
        mock_inj.distance = 100.0
        mock_inj.f_final = 1024.0
        mock_inj.f_lower = 30.0
        mock_inj.geocent_end_time = 1000000000
        mock_inj.geocent_end_time_ns = 0
        mock_inj.inclination = 0.0
        mock_inj.polarization = 0.0
        mock_inj.mass1 = 1.4
        mock_inj.mass2 = 1.4
        mock_inj.alpha4 = 10.0
        mock_inj.alpha5 = 10.0
        mock_inj.alpha6 = 10.0
        mock_inj.spin1x = 0.0
        mock_inj.spin1y = 0.0
        mock_inj.spin1z = 0.0
        mock_inj.spin2x = 0.0
        mock_inj.spin2y = 0.0
        mock_inj.spin2z = 0.0
        mock_inj.waveform = "test"

        sink.trigger_output = {"0000": "output.sqlite"}
        sink.process_params = None
        sink.injection_list = [mock_inj]
        sink.multiprocess = False
        sink.config_name = "test.yaml"
        sink.is_online = False
        sink.ifos = ["H1"]
        sink.bankids_map = {"0000": [0]}
        sink.template_ids = [0]
        sink.template_sngls = [{0: self._create_mock_sngl()}]
        sink.subbankids = ["0000_0"]
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {}
        sink.program = "test_program"
        sink.injections = True
        sink.nsubbank_pretend = False
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                with mock.patch.object(sink, "get_username", return_value="testuser"):
                    stillsuit_sink.StillSuitSink.__post_init__(sink)

        assert sink.sims is not None
        assert len(sink.sims) == 1

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.socket.gethostname")
    @mock.patch("sgnl.sinks.stillsuit_sink.os.getpid")
    def test_post_init_with_process_params(
        self,
        mock_getpid,
        mock_hostname,
        mock_snap_init,
        mock_par_init,
        mock_yaml,
        mock_open,
    ):
        """Test __post_init__ with process parameters."""
        mock_getpid.return_value = 12345
        mock_hostname.return_value = "testhost"
        mock_yaml.return_value = self._create_mock_config()

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.trigger_output = {"0000": "output.sqlite"}
        sink.process_params = {
            "test_param": "value1",
            "list_param": ["val1", "val2"],
            "none_param": None,
        }
        sink.injection_list = None
        sink.multiprocess = False
        sink.config_name = "test.yaml"
        sink.is_online = False
        sink.ifos = ["H1"]
        sink.bankids_map = {"0000": [0]}
        sink.template_ids = [0]
        sink.template_sngls = [{0: self._create_mock_sngl()}]
        sink.subbankids = ["0000_0"]
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {}
        sink.program = "test_program"
        sink.injections = False
        sink.nsubbank_pretend = False
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                with mock.patch.object(sink, "get_username", return_value="testuser"):
                    stillsuit_sink.StillSuitSink.__post_init__(sink)

        # Check params were processed
        assert len(sink.params) == 3  # 1 for test_param + 2 for list_param

    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("sgnl.sinks.stillsuit_sink.yaml.safe_load")
    @mock.patch("sgnl.sinks.stillsuit_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.stillsuit_sink.socket.gethostname")
    @mock.patch("sgnl.sinks.stillsuit_sink.os.getpid")
    def test_post_init_nsubbank_pretend(
        self,
        mock_getpid,
        mock_hostname,
        mock_snap_init,
        mock_par_init,
        mock_yaml,
        mock_open,
    ):
        """Test __post_init__ with nsubbank_pretend enabled."""
        mock_getpid.return_value = 12345
        mock_hostname.return_value = "testhost"
        mock_yaml.return_value = self._create_mock_config()

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.trigger_output = {"0000_0": "output.sqlite"}
        sink.process_params = None
        sink.injection_list = None
        sink.multiprocess = False
        sink.config_name = "test.yaml"
        sink.is_online = False
        sink.ifos = ["H1"]
        sink.bankids_map = {"0000_0": [0]}
        sink.template_ids = [0]
        sink.template_sngls = [{0: self._create_mock_sngl()}]
        sink.subbankids = ["0000_0"]
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {}
        sink.program = "test_program"
        sink.injections = False
        sink.nsubbank_pretend = True
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                with mock.patch.object(sink, "get_username", return_value="testuser"):
                    stillsuit_sink.StillSuitSink.__post_init__(sink)

        # Check filters were created with nsubbank_pretend logic
        assert "0000_0" in sink.filters

    def test_get_username_from_logname(self):
        """Test get_username from LOGNAME environment variable."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        with mock.patch.dict("os.environ", {"LOGNAME": "testuser"}, clear=False):
            result = sink.get_username()
            assert result == "testuser"

    def test_get_username_from_username(self):
        """Test get_username from USERNAME environment variable."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        env = {"USERNAME": "testuser2"}
        with mock.patch.dict("os.environ", env, clear=True):
            result = sink.get_username()
            assert result == "testuser2"

    def test_get_username_from_pwd(self):
        """Test get_username from pwd module (basic coverage)."""
        # This test verifies the pwd path is reachable
        # The actual pwd module behavior is tested implicitly
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        # When LOGNAME/USERNAME are not set, it should try pwd
        # Most systems have pwd available, so this should work
        with mock.patch.dict("os.environ", {}, clear=True):
            try:
                result = sink.get_username()
                # If we get here, pwd worked
                assert isinstance(result, str)
            except KeyError:
                # pwd not available or user not found - that's OK for this test
                pass

    def test_get_username_raises_keyerror_import_error(self):
        """Test get_username raises KeyError when pwd import fails."""
        import builtins

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pwd":
                raise ImportError("pwd not available")
            return original_import(name, *args, **kwargs)

        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch.object(builtins, "__import__", side_effect=mock_import):
                with pytest.raises(KeyError):
                    sink.get_username()

    def test_get_username_pwd_keyerror(self):
        """Test get_username raises KeyError when pwd.getpwuid raises KeyError."""
        import builtins

        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        # Create a mock pwd module that raises KeyError
        mock_pwd = mock.MagicMock()
        mock_pwd.getpwuid.side_effect = KeyError("user not found")

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pwd":
                return mock_pwd
            return original_import(name, *args, **kwargs)

        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch.object(builtins, "__import__", side_effect=mock_import):
                with mock.patch("os.getuid", return_value=9999):
                    with pytest.raises(KeyError):
                        sink.get_username()

    def test_process_outqueue(self):
        """Test process_outqueue method."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike

        data = {
            "new_lr": {"lr": 1.0},
            "frankenstein": {"data": "test"},
            "likelihood_ratio_upload": True,
            "bankid": "0000",
        }

        sink.process_outqueue(data)

        mock_strike.update_dynamic.assert_called_once_with(
            "0000", {"data": "test"}, True, {"lr": 1.0}
        )

    def test_get_state_from_queue_empty(self):
        """Test get_state_from_queue when queue is empty."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_queue = mock.MagicMock()
        mock_queue.get_nowait.side_effect = Empty()
        sink.out_queue = mock_queue

        # Should not raise, just return
        sink.get_state_from_queue()

    def test_get_state_from_queue_with_data(self):
        """Test get_state_from_queue when queue has data."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_queue = mock.MagicMock()
        mock_queue.get_nowait.return_value = {
            "new_lr": {},
            "frankenstein": {},
            "likelihood_ratio_upload": False,
            "bankid": "0000",
        }
        sink.out_queue = mock_queue
        sink.strike_object = mock.MagicMock()

        sink.get_state_from_queue()

        sink.strike_object.update_dynamic.assert_called_once()

    def test_pull_with_eos(self):
        """Test pull method with EOS frame."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = True

        sink.rsnks = {mock_pad: "test_pad"}
        sink.itacacac_pad_name = "itacacac"
        sink.in_queue = mock.MagicMock()
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.mark_eos.assert_called_once_with(mock_pad)

    def test_pull_itacacac_pad_with_events(self):
        """Test pull method with itacacac pad and events."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.events = [
            {"trigger": [{"time": 100}], "event": [{"snr": 10}]},
            {"trigger": None, "event": []},  # Should be skipped
        ]

        sink.rsnks = {mock_pad: "itacacac"}
        sink.itacacac_pad_name = "itacacac"
        sink.in_queue = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.in_queue.put.assert_called_once()
        call_args = sink.in_queue.put.call_args[0][0]
        assert "event_dict" in call_args

    def test_pull_itacacac_pad_no_events(self):
        """Test pull method with itacacac pad but no events."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.events = [{"trigger": None, "event": []}]

        sink.rsnks = {mock_pad: "itacacac"}
        sink.itacacac_pad_name = "itacacac"
        sink.in_queue = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.in_queue.put.assert_not_called()

    def test_pull_segment_pad(self):
        """Test pull method with segment pad."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_pad = mock.MagicMock()
        mock_buf = mock.MagicMock()
        mock_buf.data = [1, 2, 3]  # Not None

        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.offset = 0
        mock_frame.end_offset = 100
        mock_frame.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        sink.rsnks = {mock_pad: "segment_pad"}
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {"segment_pad": "H1"}
        sink.in_queue = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.in_queue.put.assert_called_once()
        call_args = sink.in_queue.put.call_args[0][0]
        assert "segment" in call_args
        assert "H1" in call_args["segment"]

    def test_pull_segment_pad_none_data(self):
        """Test pull method with segment pad but None data."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_pad = mock.MagicMock()
        mock_buf = mock.MagicMock()
        mock_buf.data = None

        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.__iter__ = mock.MagicMock(return_value=iter([mock_buf]))

        sink.rsnks = {mock_pad: "segment_pad"}
        sink.itacacac_pad_name = "itacacac"
        sink.segments_pad_map = {"segment_pad": "H1"}
        sink.in_queue = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.in_queue.put.assert_not_called()

    def test_internal_at_eos_online_injection(self):
        """Test internal method at EOS with online injection mode."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = True
        sink.injections = True
        sink.in_queue = mock.MagicMock()
        sink.terminated = mock.MagicMock()
        sink.terminated.is_set.return_value = False
        sink.sub_process_shutdown = mock.MagicMock(return_value=[])

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            with mock.patch.object(
                sink, "snapshot_filenames", return_value="test.sqlite"
            ):
                with mock.patch(
                    "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
                ):
                    sink.internal()

        sink.in_queue.put.assert_called_once()
        call_args = sink.in_queue.put.call_args[0][0]
        assert "snapshot" in call_args
        assert call_args["snapshot"]["bankid"] == "0000"

    def test_internal_at_eos_offline(self):
        """Test internal method at EOS with offline mode."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = False
        sink.injections = False
        sink.trigger_output = {"0000": "output.sqlite"}
        sink.in_queue = mock.MagicMock()
        sink.terminated = mock.MagicMock()
        sink.terminated.is_set.return_value = False
        sink.sub_process_shutdown = mock.MagicMock(return_value=[])

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            with mock.patch(
                "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
            ):
                sink.internal()

        sink.in_queue.put.assert_called_once()
        call_args = sink.in_queue.put.call_args[0][0]
        assert call_args["snapshot"]["fn"] == "output.sqlite"

    def test_internal_at_eos_terminated(self):
        """Test internal method at EOS when subprocess is terminated."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = True
        sink.injections = False
        sink.in_queue = mock.MagicMock()
        sink.terminated = mock.MagicMock()
        sink.terminated.is_set.return_value = True

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            with mock.patch.object(
                sink, "snapshot_filenames", return_value="test.sqlite"
            ):
                with mock.patch(
                    "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
                ):
                    sink.internal()

        # Should not call sub_process_shutdown since terminated

    def test_internal_not_eos_online_snapshot_ready(self):
        """Test internal method when not at EOS and snapshot is ready."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = True
        sink.injections = False
        sink.in_queue = mock.MagicMock()
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=True):
                with mock.patch.object(
                    sink, "snapshot_filenames", return_value="test.sqlite"
                ):
                    with mock.patch(
                        "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
                    ):
                        sink.internal()

        sink.in_queue.put.assert_called_once()

    def test_internal_not_eos_online_snapshot_ready_with_injections(self):
        """Test internal method with snapshot ready and injections."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_strike = mock.MagicMock()
        mock_strike.input_likelihood_file = {"0000": "lr_0000.h5"}

        sink.bankids = ["0000"]
        sink.is_online = True
        sink.injections = True
        sink.in_queue = mock.MagicMock()
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.strike_object = mock_strike

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=True):
                with mock.patch.object(
                    sink, "snapshot_filenames", return_value="test.sqlite"
                ):
                    with mock.patch(
                        "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
                    ):
                        sink.internal()

        mock_strike.load_rank_stat_pdf.assert_called_once()
        call_args = sink.in_queue.put.call_args[0][0]
        assert "in_lr_file" in call_args["snapshot"]

    def test_internal_not_eos_online_snapshot_not_ready(self):
        """Test internal method when not at EOS and snapshot is not ready."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = True
        sink.injections = False
        sink.in_queue = mock.MagicMock()
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=False):
                with mock.patch(
                    "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
                ):
                    sink.internal()

        sink.in_queue.put.assert_not_called()

    def test_internal_not_eos_offline(self):
        """Test internal method when not at EOS in offline mode."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        sink.bankids = ["0000"]
        sink.is_online = False

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch(
                "sgnl.sinks.stillsuit_sink.SnapShotControlSinkElement.internal"
            ):
                sink.internal()

        # Offline mode with not at_eos does nothing special


class TestWorkerProcess:
    """Tests for worker_process method."""

    def test_worker_process_init(self):
        """Test worker_process initialization."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_context = mock.MagicMock()
        mock_context.state = {}
        mock_context.input_queue.get.side_effect = Empty()

        with mock.patch("sgnl.sinks.stillsuit_sink.init_dbs") as mock_init_dbs:
            mock_init_dbs.return_value = ({}, {})
            sink.worker_process(
                mock_context,
                ifos=["H1"],
                config_name="test.yaml",
                bankids=["0000"],
                process={},
                process_params=[],
                sim=[],
                config_segment={},
                filters={},
            )

        assert mock_context.state["subproc_init"]
        mock_init_dbs.assert_called_once()

    def test_worker_process_segment(self):
        """Test worker_process with segment data."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_context = mock.MagicMock()
        mock_context.state = {
            "subproc_init": True,
            "temp_segments": {
                "0000": segments.segmentlistdict({"H1": segments.segmentlist()})
            },
            "dbs": {},
        }
        mock_context.input_queue.get.return_value = {"segment": {"H1": (0, 100)}}

        sink.worker_process(
            mock_context,
            ifos=["H1"],
            config_name="test.yaml",
            bankids=["0000"],
            process={},
            process_params=[],
            sim=[],
            config_segment={},
            filters={},
        )

        # Segment should be appended
        assert len(mock_context.state["temp_segments"]["0000"]["H1"]) == 1

    def test_worker_process_event(self):
        """Test worker_process with event data."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_db = mock.MagicMock()
        mock_context = mock.MagicMock()
        mock_context.state = {
            "subproc_init": True,
            "temp_segments": {},
            "dbs": {"0000": mock_db},
        }
        mock_context.input_queue.get.return_value = {
            "event_dict": {
                "event": [{"bankid": "0000"}],
                "trigger": [{}],
            }
        }

        sink.worker_process(
            mock_context,
            ifos=["H1"],
            config_name="test.yaml",
            bankids=["0000"],
            process={},
            process_params=[],
            sim=[],
            config_segment={},
            filters={},
        )

        mock_db.insert_event.assert_called_once()

    def test_worker_process_snapshot(self):
        """Test worker_process with snapshot data."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_db = mock.MagicMock()
        mock_context = mock.MagicMock()
        mock_context.state = {
            "subproc_init": True,
            "temp_segments": {
                "0000": segments.segmentlistdict({"H1": segments.segmentlist()})
            },
            "dbs": {"0000": mock_db},
        }
        mock_context.input_queue.get.return_value = {
            "snapshot": {"fn": "test.sqlite", "bankid": "0000"}
        }
        mock_context.should_shutdown.return_value = False

        with mock.patch("sgnl.sinks.stillsuit_sink.on_snapshot") as mock_on_snapshot:
            mock_on_snapshot.return_value = None
            sink.worker_process(
                mock_context,
                ifos=["H1"],
                config_name="test.yaml",
                bankids=["0000"],
                process={},
                process_params=[],
                sim=[],
                config_segment={},
                filters={},
            )

        mock_on_snapshot.assert_called_once()

    def test_worker_process_snapshot_with_lr_dict(self):
        """Test worker_process with snapshot that returns lr_dict."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_db = mock.MagicMock()
        mock_context = mock.MagicMock()
        mock_context.state = {
            "subproc_init": True,
            "temp_segments": {
                "0000": segments.segmentlistdict({"H1": segments.segmentlist()})
            },
            "dbs": {"0000": mock_db},
        }
        mock_context.input_queue.get.return_value = {
            "snapshot": {"fn": "test.sqlite", "bankid": "0000", "in_lr_file": "lr.h5"}
        }
        mock_context.should_shutdown.return_value = False

        with mock.patch("sgnl.sinks.stillsuit_sink.on_snapshot") as mock_on_snapshot:
            mock_on_snapshot.return_value = {"bankid": "0000", "lr": 1.0}
            sink.worker_process(
                mock_context,
                ifos=["H1"],
                config_name="test.yaml",
                bankids=["0000"],
                process={},
                process_params=[],
                sim=[],
                config_segment={},
                filters={},
            )

        mock_context.output_queue.put.assert_called_once()

    def test_worker_process_unknown_data(self):
        """Test worker_process with unknown data type."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_context = mock.MagicMock()
        mock_context.state = {"subproc_init": True, "temp_segments": {}, "dbs": {}}
        mock_context.input_queue.get.return_value = {"unknown": "data"}

        with pytest.raises(ValueError, match="Unknown data"):
            sink.worker_process(
                mock_context,
                ifos=["H1"],
                config_name="test.yaml",
                bankids=["0000"],
                process={},
                process_params=[],
                sim=[],
                config_segment={},
                filters={},
            )

    def test_worker_process_empty_queue(self):
        """Test worker_process when queue is empty."""
        with mock.patch.object(stillsuit_sink.StillSuitSink, "__post_init__"):
            sink = object.__new__(stillsuit_sink.StillSuitSink)

        mock_context = mock.MagicMock()
        mock_context.state = {"subproc_init": True, "temp_segments": {}, "dbs": {}}
        mock_context.input_queue.get.side_effect = Empty()

        # Should not raise, just return
        sink.worker_process(
            mock_context,
            ifos=["H1"],
            config_name="test.yaml",
            bankids=["0000"],
            process={},
            process_params=[],
            sim=[],
            config_segment={},
            filters={},
        )
