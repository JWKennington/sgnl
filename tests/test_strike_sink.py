"""Tests for sgnl.sinks.strike_sink module."""

from queue import Empty
from unittest import mock

from sgnl.sinks import strike_sink

# Path constant to avoid line too long
EXCHANGE_STATE_PATH = "sgnl.sinks.strike_sink.SnapShotControlSinkElement.exchange_state"


class TestOnSnapshot:
    """Tests for on_snapshot function."""

    @mock.patch("sgnl.sinks.strike_sink.StrikeObject")
    def test_on_snapshot_shutdown(self, mock_strike_class):
        """Test on_snapshot with shutdown=True."""
        data = {
            "lr": mock.MagicMock(),
            "zero_lr": mock.MagicMock(),
            "bankid": "0000",
            "output_likelihood_file": "lr.xml",
            "output_zerolag_likelihood_file": "zerolag.xml",
            "fn": "snapshot.xml",
        }

        result = strike_sink.on_snapshot(data, shutdown=True)

        mock_strike_class.snapshot_io.assert_called_once_with(
            data["lr"],
            data["zero_lr"],
            data["fn"],
            data["output_likelihood_file"],
            data["output_zerolag_likelihood_file"],
        )
        assert result is None

    @mock.patch("sgnl.sinks.strike_sink.StrikeObject")
    def test_on_snapshot_normal(self, mock_strike_class):
        """Test on_snapshot with normal operation."""
        mock_lr = mock.MagicMock()
        mock_zero_lr = mock.MagicMock()

        data = {
            "lr": mock_lr,
            "zero_lr": mock_zero_lr,
            "bankid": "0000",
            "output_likelihood_file": "lr.xml",
            "output_zerolag_likelihood_file": "zerolag.xml",
            "fn": "snapshot.xml",
        }

        mock_strike_class.snapshot_fileobj.return_value = {"xml": {}, "zerolagxml": {}}
        mock_strike_class._update_assign_lr.return_value = (
            {"key": "frank"},
            {"key": "upload"},
        )

        result = strike_sink.on_snapshot(data, shutdown=False, reset_dynamic=True)

        mock_strike_class.snapshot_io.assert_called_once()
        mock_strike_class.snapshot_fileobj.assert_called_once_with(
            mock_lr, mock_zero_lr, "0000"
        )
        mock_strike_class._update_assign_lr.assert_called_once_with(mock_lr)
        mock_strike_class.reset_dynamic.assert_called_once()

        assert result is not None
        assert result["bankid"] == "0000"
        assert "frankenstein" in result
        assert "likelihood_ratio_upload" in result

    @mock.patch("sgnl.sinks.strike_sink.StrikeObject")
    def test_on_snapshot_no_reset_dynamic(self, mock_strike_class):
        """Test on_snapshot with reset_dynamic=False."""
        data = {
            "lr": mock.MagicMock(),
            "zero_lr": mock.MagicMock(),
            "bankid": "0000",
            "output_likelihood_file": "lr.xml",
            "output_zerolag_likelihood_file": "zerolag.xml",
            "fn": "snapshot.xml",
        }

        mock_strike_class.snapshot_fileobj.return_value = {"xml": {}, "zerolagxml": {}}
        mock_strike_class._update_assign_lr.return_value = ({}, {})

        result = strike_sink.on_snapshot(data, shutdown=False, reset_dynamic=False)

        mock_strike_class.reset_dynamic.assert_not_called()
        assert result is not None


class TestStrikeSink:
    """Tests for StrikeSink class."""

    def _create_mock_strike_object(self):
        """Create a mock StrikeObject."""
        mock_strike = mock.MagicMock()

        # Create mock likelihood ratio with nested structure
        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_SNR_chisq": mock.MagicMock(remove_counts_times=[]),
            "P_of_tref_Dh": mock.MagicMock(
                triggerrates={"H1": mock.MagicMock(), "L1": mock.MagicMock()},
                horizon_history={"H1": {}, "L1": {}},
            ),
        }

        mock_strike.likelihood_ratios = {"0000": mock_lr}
        mock_strike.zerolag_rank_stat_pdfs = {"0000": mock.MagicMock()}
        mock_strike.output_likelihood_file = {"0000": "output.xml"}
        return mock_strike

    @mock.patch("sgnl.sinks.strike_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.strike_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_basic(self, mock_snap_init, mock_par_init):
        """Test basic __post_init__ setup."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.ifos = ["H1", "L1"]
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1", "horizon_L1"]
        sink.strike_object = self._create_mock_strike_object()
        sink.bankids_map = {"0000": [0]}
        sink.multiprocess = False
        sink.is_online = False
        sink.injections = False
        sink.count_removal_times = None
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        strike_sink.StrikeSink.__post_init__(sink)

        assert sink.sink_pad_names == ("background", "horizon_H1", "horizon_L1")
        assert sink.temp == 0

    @mock.patch("sgnl.sinks.strike_sink.xml_string")
    @mock.patch("sgnl.sinks.strike_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.strike_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_online_non_injection(
        self, mock_snap_init, mock_par_init, mock_xml_string
    ):
        """Test __post_init__ with online mode and non-injection."""
        mock_xml_string.return_value = "<xml>test</xml>"

        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.ifos = ["H1"]
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = self._create_mock_strike_object()
        sink.bankids_map = {"0000": [0]}
        sink.multiprocess = False
        sink.is_online = True
        sink.injections = False
        sink.count_removal_times = None
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                strike_sink.StrikeSink.__post_init__(sink)

        assert "xml" in sink.state_dict
        assert "zerolagxml" in sink.state_dict
        assert sink.count_removal_times == []

    @mock.patch("sgnl.sinks.strike_sink.xml_string")
    @mock.patch("sgnl.sinks.strike_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.strike_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_with_existing_count_removal_times(
        self, mock_snap_init, mock_par_init, mock_xml_string
    ):
        """Test __post_init__ with pre-existing count_removal_times."""
        mock_xml_string.return_value = "<xml>test</xml>"

        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.ifos = ["H1"]
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        mock_strike = self._create_mock_strike_object()
        mock_strike.likelihood_ratios["0000"].terms[
            "P_of_SNR_chisq"
        ].remove_counts_times = [1000, 2000]
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.multiprocess = False
        sink.is_online = True
        sink.injections = False
        sink.count_removal_times = [1000, 2000]  # Pre-set
        sink.name = "test_sink"
        sink.descriptions = []
        sink.extensions = []

        with mock.patch.object(sink, "add_snapshot_filename"):
            with mock.patch.object(sink, "register_snapshot"):
                strike_sink.StrikeSink.__post_init__(sink)

        # Should reach the assert statement
        assert sink.count_removal_times == [1000, 2000]

    @mock.patch("sgnl.sinks.strike_sink.ParallelizeSinkElement.__post_init__")
    @mock.patch("sgnl.sinks.strike_sink.SnapShotControlSinkElement.__post_init__")
    def test_post_init_injections(self, mock_snap_init, mock_par_init):
        """Test __post_init__ with injections=True."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.ifos = ["H1"]
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = self._create_mock_strike_object()
        sink.bankids_map = {"0000": [0]}
        sink.multiprocess = False
        sink.is_online = False
        sink.injections = True
        sink.count_removal_times = None

        strike_sink.StrikeSink.__post_init__(sink)

        # ParallelizeSinkElement.__post_init__ should NOT be called when injections=True
        mock_par_init.assert_not_called()

    def test_pull_eos(self):
        """Test pull method with EOS frame."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = True

        sink.rsnks = {mock_pad: "background"}
        sink.background_pad = "background"
        sink.horizon_pads = []
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        sink.mark_eos.assert_called_once_with(mock_pad)

    def test_pull_background_pad_with_background_data(self):
        """Test pull method with background pad and data."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.start = 1000000000000000000  # nanoseconds
        mock_frame.events = [
            {
                "background": {
                    "snrs": [5.0, 6.0],
                    "chisqs": [1.0, 1.2],
                    "single_masks": [[True, False]],
                },
                "trigger_rates": {
                    "H1": {"0000": ((0, 100), 10)},
                },
            }
        ]

        mock_strike = mock.MagicMock()
        mock_lr = mock.MagicMock()
        mock_lr.terms = {
            "P_of_tref_Dh": mock.MagicMock(
                triggerrates={"H1": mock.MagicMock()},
            ),
        }
        mock_strike.likelihood_ratios = {"0000": mock_lr}

        sink.rsnks = {mock_pad: "background"}
        sink.background_pad = "background"
        sink.horizon_pads = []
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        mock_strike.train_noise.assert_called_once()

    def test_pull_background_pad_none_background(self):
        """Test pull method with background pad but None background data."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.events = [{"background": None, "trigger_rates": None}]

        mock_strike = mock.MagicMock()

        sink.rsnks = {mock_pad: "background"}
        sink.background_pad = "background"
        sink.horizon_pads = []
        sink.strike_object = mock_strike
        sink.bankids_map = {}
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        mock_strike.train_noise.assert_not_called()

    def test_pull_horizon_pad_valid_data(self):
        """Test pull method with horizon pad and valid data."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.start = 1000000000000000000
        mock_frame.end = 1001000000000000000
        mock_frame.events = [
            {
                "ifo": "H1",
                "horizon": {"0000": 100.0},
                "epoch": 1000500000000000000,
                "n_samples": 100,
                "navg": 200,  # stability = 0.5 > 0.3
            }
        ]

        mock_lr = mock.MagicMock()
        mock_lr.terms = {"P_of_tref_Dh": mock.MagicMock(horizon_history={"H1": {}})}
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}

        sink.rsnks = {mock_pad: "horizon_H1"}
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        # Check horizon was set
        assert (
            mock_lr.terms["P_of_tref_Dh"].horizon_history["H1"][1000500000.0] == 100.0
        )

    def test_pull_horizon_pad_low_stability(self):
        """Test pull method with horizon pad but low stability."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.start = 1000000000000000000
        mock_frame.end = 1001000000000000000
        mock_frame.events = [
            {
                "ifo": "H1",
                "horizon": {"0000": 100.0},
                "epoch": 1000500000000000000,
                "n_samples": 10,
                "navg": 200,  # stability = 0.05 < 0.3
            }
        ]

        mock_lr = mock.MagicMock()
        mock_lr.terms = {"P_of_tref_Dh": mock.MagicMock(horizon_history={"H1": {}})}
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}

        sink.rsnks = {mock_pad: "horizon_H1"}
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        # Check horizon was set to 0 due to low stability
        assert mock_lr.terms["P_of_tref_Dh"].horizon_history["H1"][1000500000.0] == 0

    def test_pull_horizon_pad_none_horizon(self):
        """Test pull method with horizon pad but None horizon."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.start = 1000000000000000000
        mock_frame.end = 1001000000000000000
        mock_frame.events = [
            {
                "ifo": "H1",
                "horizon": None,
                "epoch": 1000500000000000000,
                "n_samples": 100,
                "navg": 200,
            }
        ]

        mock_lr = mock.MagicMock()
        mock_lr.terms = {"P_of_tref_Dh": mock.MagicMock(horizon_history={"H1": {}})}
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}

        sink.rsnks = {mock_pad: "horizon_H1"}
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.mark_eos = mock.MagicMock()

        sink.pull(mock_pad, mock_frame)

        # Check horizon was set to 0 due to None horizon
        assert mock_lr.terms["P_of_tref_Dh"].horizon_history["H1"][1000500000.0] == 0

    def test_pull_horizon_pad_zero_duration(self):
        """Test pull method with horizon pad but zero duration frame."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_pad = mock.MagicMock()
        mock_frame = mock.MagicMock()
        mock_frame.EOS = False
        mock_frame.start = 1000000000000000000
        mock_frame.end = 1000000000000000000  # Same as start

        mock_strike = mock.MagicMock()

        sink.rsnks = {mock_pad: "horizon_H1"}
        sink.background_pad = "background"
        sink.horizon_pads = ["horizon_H1"]
        sink.strike_object = mock_strike
        sink.bankids_map = {"0000": [0]}
        sink.mark_eos = mock.MagicMock()

        result = sink.pull(mock_pad, mock_frame)

        # Should return early
        assert result is None

    def test_process_outqueue(self):
        """Test process_outqueue method."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike
        sink.state_dict = {
            "xml": {"0000": "<old_xml>", "0001": "<other_xml>"},
            "zerolagxml": {"0000": "<old_zerolag>", "0001": "<other_zerolag>"},
            "count_tracker": 0,
            "count_removal_times": [],
        }

        sdict = {
            "xml": {"0000": "<new_xml>"},
            "zerolagxml": {"0000": "<new_zerolag>"},
            "bankid": "0000",
            "frankenstein": {"0000": {"key": "frank"}},
            "likelihood_ratio_upload": {"0000": {"key": "upload"}},
        }

        sink.process_outqueue(sdict)

        # Check state_dict was updated
        assert sink.state_dict["xml"]["0000"] == "<new_xml>"
        assert sink.state_dict["xml"]["0001"] == "<other_xml>"
        mock_strike.update_dynamic.assert_called_once_with(
            "0000", {"key": "frank"}, {"key": "upload"}
        )

    def test_get_state_from_queue_empty(self):
        """Test get_state_from_queue when queue is empty."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_queue = mock.MagicMock()
        mock_queue.get_nowait.side_effect = Empty()
        sink.out_queue = mock_queue

        sink.get_state_from_queue()

        # Should not raise, just return

    def test_get_state_from_queue_with_data(self):
        """Test get_state_from_queue when queue has data."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_queue = mock.MagicMock()
        mock_queue.get_nowait.return_value = {
            "xml": {"0000": "<xml>"},
            "zerolagxml": {"0000": "<zerolag>"},
            "bankid": "0000",
            "frankenstein": {"0000": {}},
            "likelihood_ratio_upload": {"0000": {}},
        }
        sink.out_queue = mock_queue
        sink.strike_object = mock.MagicMock()
        sink.state_dict = {
            "xml": {},
            "zerolagxml": {},
            "count_tracker": 0,
            "count_removal_times": [],
        }

        sink.get_state_from_queue()

        sink.strike_object.update_dynamic.assert_called_once()

    def test_internal_injections(self):
        """Test internal method with injections=True."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.injections = True

        result = sink.internal()

        # Should return early
        assert result is None

    def test_internal_online_not_eos(self):
        """Test internal method online, not at EOS."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = True
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.state_dict = {"count_tracker": 0, "count_removal_times": []}
        sink.name = "test_sink"

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=False):
                with mock.patch(EXCHANGE_STATE_PATH):
                    sink.internal()

        # Should not call snapshot_filenames since snapshot not ready

    def test_internal_online_not_eos_snapshot_ready(self):
        """Test internal method online, not at EOS, snapshot ready."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = True
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.in_queue = mock.MagicMock()
        sink.state_dict = {"count_tracker": 0, "count_removal_times": []}
        sink.name = "test_sink"

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=True):
                with mock.patch.object(
                    sink, "snapshot_filenames", return_value="test.xml"
                ):
                    with mock.patch(EXCHANGE_STATE_PATH):
                        sink.internal()

        mock_strike.update_array_data.assert_called_once()
        mock_strike.prepare_inq_data.assert_called_once()
        sink.in_queue.put.assert_called_once()
        mock_strike.load_rank_stat_pdf.assert_called_once()

    def test_internal_online_multiprocess(self):
        """Test internal method with multiprocess=True."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        sink.injections = False
        sink.is_online = True
        sink.multiprocess = True
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.state_dict = {"count_tracker": 0, "count_removal_times": []}
        sink.name = "test_sink"

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=False):
                with mock.patch(EXCHANGE_STATE_PATH):
                    with mock.patch(
                        "sgnl.sinks.strike_sink.ParallelizeSinkElement.internal"
                    ) as mock_par_internal:
                        sink.internal()

        mock_par_internal.assert_called_once()

    def test_internal_with_count_tracker_positive(self):
        """Test internal method with positive count_tracker."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock.MagicMock()}
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = True
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.state_dict = {"count_tracker": 1000, "count_removal_times": []}
        sink.count_removal_times = []
        sink.name = "test_sink"

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = False
            with mock.patch.object(sink, "snapshot_ready", return_value=False):
                with mock.patch(EXCHANGE_STATE_PATH):
                    sink.internal()

        # count_removal_callback should have been called
        assert 1000 in sink.count_removal_times

    def test_internal_at_eos_online_terminated(self):
        """Test internal method at EOS online when terminated."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = True
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.state_dict = {"count_tracker": 0, "count_removal_times": []}
        sink.name = "test_sink"
        sink.terminated = mock.MagicMock()
        sink.terminated.is_set.return_value = True
        # These are needed for snapshot_filenames
        sink.descriptions = ["0000_SGNL_LIKELIHOOD_RATIO"]
        sink.extensions = ["xml.gz"]

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            with mock.patch.object(sink, "snapshot_filenames", return_value="test.xml"):
                with mock.patch(EXCHANGE_STATE_PATH):
                    with mock.patch(
                        "sgnl.sinks.strike_sink.on_snapshot"
                    ) as mock_on_snapshot:
                        sink.internal()

        # Should call on_snapshot for each bankid even when terminated
        mock_on_snapshot.assert_called()

    def test_internal_at_eos_online_not_terminated(self):
        """Test internal method at EOS online when not terminated."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = True
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}
        sink.out_queue = mock.MagicMock()
        sink.out_queue.get_nowait.side_effect = Empty()
        sink.state_dict = {"count_tracker": 0, "count_removal_times": []}
        sink.name = "test_sink"
        sink.terminated = mock.MagicMock()
        sink.terminated.is_set.return_value = False
        sink.sub_process_shutdown = mock.MagicMock(return_value=[])

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            with mock.patch.object(sink, "snapshot_filenames", return_value="test.xml"):
                with mock.patch(EXCHANGE_STATE_PATH):
                    with mock.patch(
                        "sgnl.sinks.strike_sink.on_snapshot"
                    ) as mock_on_snapshot:
                        sink.internal()

        sink.sub_process_shutdown.assert_called_once_with(600)
        mock_strike.update_array_data.assert_called()
        mock_on_snapshot.assert_called()

    def test_internal_at_eos_offline(self):
        """Test internal method at EOS offline."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_strike = mock.MagicMock()
        mock_lr = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}
        mock_strike.output_likelihood_file = {"0000": "output.xml"}
        sink.strike_object = mock_strike
        sink.injections = False
        sink.is_online = False
        sink.multiprocess = False
        sink.bankids_map = {"0000": [0]}

        with mock.patch.object(
            type(sink), "at_eos", new_callable=mock.PropertyMock
        ) as mock_eos:
            mock_eos.return_value = True
            sink.internal()

        mock_strike.update_array_data.assert_called_once_with("0000")
        mock_lr.save.assert_called_once_with("output.xml")

    def test_count_removal_callback_positive(self):
        """Test count_removal_callback with positive count_tracker."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_lr = mock.MagicMock()
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}
        sink.strike_object = mock_strike
        sink.count_removal_times = [500]
        sink.state_dict = {"count_tracker": 1000, "count_removal_times": [500]}

        sink.count_removal_callback()

        assert 1000 in sink.count_removal_times
        assert sink.state_dict["count_tracker"] == 0

    def test_count_removal_callback_negative_found(self):
        """Test count_removal_callback with negative count_tracker (removal)."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_lr = mock.MagicMock()
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}
        sink.strike_object = mock_strike
        sink.count_removal_times = [500, 1000]
        sink.state_dict = {"count_tracker": -1000, "count_removal_times": [500, 1000]}

        sink.count_removal_callback()

        assert 1000 not in sink.count_removal_times
        assert 500 in sink.count_removal_times
        assert sink.state_dict["count_tracker"] == 0

    def test_count_removal_callback_negative_not_found(self):
        """Test count_removal_callback with negative count_tracker not in list."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_lr = mock.MagicMock()
        mock_strike = mock.MagicMock()
        mock_strike.likelihood_ratios = {"0000": mock_lr}
        sink.strike_object = mock_strike
        sink.count_removal_times = [500]
        sink.state_dict = {"count_tracker": -1000, "count_removal_times": [500]}

        sink.count_removal_callback()

        # 1000 was not in list, should print message but not raise
        assert sink.count_removal_times == [500]
        assert sink.state_dict["count_tracker"] == 0


class TestWorkerProcess:
    """Tests for worker_process method."""

    @mock.patch("sgnl.sinks.strike_sink.time.sleep")
    @mock.patch("sgnl.sinks.strike_sink.on_snapshot")
    def test_worker_process_with_data(self, mock_on_snapshot, mock_sleep):
        """Test worker_process with data."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_context = mock.MagicMock()
        mock_context.input_queue.get.return_value = {"data": "test"}
        mock_context.should_shutdown.return_value = False
        mock_on_snapshot.return_value = {"result": "snapshot"}

        sink.worker_process(mock_context)

        mock_on_snapshot.assert_called_once_with({"data": "test"}, False, True)
        mock_context.output_queue.put.assert_called_once_with({"result": "snapshot"})
        mock_sleep.assert_called_once_with(10)

    @mock.patch("sgnl.sinks.strike_sink.on_snapshot")
    def test_worker_process_snapshot_returns_none(self, mock_on_snapshot):
        """Test worker_process when on_snapshot returns None."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_context = mock.MagicMock()
        mock_context.input_queue.get.return_value = {"data": "test"}
        mock_context.should_shutdown.return_value = True
        mock_on_snapshot.return_value = None

        sink.worker_process(mock_context)

        mock_on_snapshot.assert_called_once()
        mock_context.output_queue.put.assert_not_called()

    def test_worker_process_empty_queue(self):
        """Test worker_process when queue is empty."""
        with mock.patch.object(strike_sink.StrikeSink, "__post_init__"):
            sink = object.__new__(strike_sink.StrikeSink)

        mock_context = mock.MagicMock()
        mock_context.input_queue.get.side_effect = Empty()

        sink.worker_process(mock_context)

        # Should not raise, just return
