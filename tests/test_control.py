"""Tests for sgnl.control"""

from unittest import mock

import pytest

from sgnl import control


class TestSnapShotControl:
    """Tests for SnapShotControl class."""

    def test_context_manager_enables_snapshots(self):
        """Test __enter__ and __exit__ enable/disable snapshots."""
        with (
            mock.patch.object(control.HTTPControl, "__enter__", return_value=None),
            mock.patch.object(control.HTTPControl, "__exit__", return_value=None),
        ):
            assert control.SnapShotControl.snapshots_enabled is False

            ctrl = control.SnapShotControl()
            ctrl.__enter__()
            assert control.SnapShotControl.snapshots_enabled is True

            ctrl.__exit__(None, None, None)
            assert control.SnapShotControl.snapshots_enabled is False

    def test_register_snapshot(self):
        """Test _register_snapshot adds element to last_snapshot."""
        control.SnapShotControl.last_snapshot = {}
        control.SnapShotControl.startup_delay = 10

        control.SnapShotControl._register_snapshot(
            "test_elem", 1000, ["desc1", "desc2"]
        )

        assert "test_elem" in control.SnapShotControl.last_snapshot
        assert control.SnapShotControl.last_snapshot["test_elem"]["desc1"] == 1010
        assert control.SnapShotControl.last_snapshot["test_elem"]["desc2"] == 1010

    def test_snapshot_ready_returns_false_when_disabled(self):
        """Test _snapshot_ready returns False when snapshots disabled."""
        control.SnapShotControl.snapshots_enabled = False
        control.SnapShotControl.last_snapshot = {"test_elem": {"desc1": 0}}

        result = control.SnapShotControl._snapshot_ready("test_elem", 100000, "desc1")

        assert result is False

    def test_snapshot_ready_raises_when_element_not_registered(self):
        """Test _snapshot_ready raises ValueError for unregistered element."""
        control.SnapShotControl.snapshots_enabled = True
        control.SnapShotControl.last_snapshot = {}

        with pytest.raises(ValueError) as exc_info:
            control.SnapShotControl._snapshot_ready("unknown_elem", 1000, "desc1")

        assert "not found in last_snapshot" in str(exc_info.value)

    def test_snapshot_ready_returns_true_when_interval_exceeded(self):
        """Test _snapshot_ready returns True when interval exceeded."""
        control.SnapShotControl.snapshots_enabled = True
        control.SnapShotControl.snapshot_interval = 100
        control.SnapShotControl.last_snapshot = {"test_elem": {"desc1": 0}}

        result = control.SnapShotControl._snapshot_ready("test_elem", 200, "desc1")

        assert result is True

    def test_snapshot_ready_returns_false_when_interval_not_exceeded(self):
        """Test _snapshot_ready returns False when interval not exceeded."""
        control.SnapShotControl.snapshots_enabled = True
        control.SnapShotControl.snapshot_interval = 100
        control.SnapShotControl.last_snapshot = {"test_elem": {"desc1": 0}}

        result = control.SnapShotControl._snapshot_ready("test_elem", 50, "desc1")

        assert result is False

    def test_update_last_snapshot_time(self):
        """Test _update_last_snapshot_time updates time and returns delta."""
        control.SnapShotControl.last_snapshot = {"test_elem": {"desc1": 100}}

        old_t, delta = control.SnapShotControl._update_last_snapshot_time(
            "test_elem", 250, "desc1"
        )

        assert old_t == 100
        assert delta == 150
        assert control.SnapShotControl.last_snapshot["test_elem"]["desc1"] == 250

    def test_update_last_snapshot_time_raises_when_element_not_registered(self):
        """Test _update_last_snapshot_time raises ValueError for unregistered."""
        control.SnapShotControl.last_snapshot = {}

        with pytest.raises(ValueError) as exc_info:
            control.SnapShotControl._update_last_snapshot_time(
                "unknown_elem", 1000, "desc1"
            )

        assert "not found in last_snapshot" in str(exc_info.value)


class TestSnapShotControlSinkElement:
    """Tests for SnapShotControlSinkElement class."""

    @pytest.fixture
    def mock_sink_element(self):
        """Create a mock sink element."""

        # Create a concrete subclass that implements the abstract method
        class ConcreteSnapShotControlSinkElement(control.SnapShotControlSinkElement):
            def pull(self):
                pass

        with mock.patch.object(
            control.HTTPControlSinkElement, "__post_init__", return_value=None
        ):
            elem = ConcreteSnapShotControlSinkElement(name="test_sink")
            return elem

    def test_post_init(self, mock_sink_element):
        """Test __post_init__ initializes attributes."""
        assert mock_sink_element.descriptions == []
        assert mock_sink_element.extensions == []
        assert mock_sink_element.startup_delays == {}

    def test_add_snapshot_filename(self, mock_sink_element):
        """Test add_snapshot_filename appends to lists."""
        mock_sink_element.add_snapshot_filename("PSD", "xml.gz")
        mock_sink_element.add_snapshot_filename("TRIGGERS", "h5")

        assert mock_sink_element.descriptions == ["PSD", "TRIGGERS"]
        assert mock_sink_element.extensions == ["xml.gz", "h5"]

    def test_snapshot_ready(self, mock_sink_element):
        """Test snapshot_ready calls class method."""
        control.SnapShotControl.snapshots_enabled = True
        control.SnapShotControl.snapshot_interval = 100
        control.SnapShotControl.last_snapshot = {"test_sink": {"PSD": 0}}

        with mock.patch("sgnl.control.tconvert", return_value=200):
            result = mock_sink_element.snapshot_ready("PSD")

        assert result is True

    def test_register_snapshot(self, mock_sink_element):
        """Test register_snapshot registers element."""
        control.SnapShotControl.last_snapshot = {}
        control.SnapShotControl.startup_delay = 5

        mock_sink_element.descriptions = ["PSD", "TRIGGERS"]

        with mock.patch("sgnl.control.tconvert", return_value=1000):
            mock_sink_element.register_snapshot()

        assert "test_sink" in control.SnapShotControl.last_snapshot
        assert mock_sink_element.startup_delays["PSD"] == 5
        assert mock_sink_element.startup_delays["TRIGGERS"] == 5

    def test_snapshot_filenames_without_startup_delay(
        self, mock_sink_element, tmp_path
    ):
        """Test snapshot_filenames returns correct filename."""
        control.SnapShotControl.last_snapshot = {"test_sink": {"PSD": 1000000000}}

        mock_sink_element.descriptions = ["PSD"]
        mock_sink_element.extensions = ["xml.gz"]
        mock_sink_element.startup_delays = {"PSD": 0}

        with (
            mock.patch("sgnl.control.tconvert", return_value=1000014400),
            mock.patch("os.mkdir"),
        ):
            filename = mock_sink_element.snapshot_filenames("PSD", ifos="H1L1")

        assert filename == "10000/H1L1-PSD-1000000000-14400.xml.gz"

    def test_snapshot_filenames_with_startup_delay(self, mock_sink_element, tmp_path):
        """Test snapshot_filenames handles startup delay."""
        control.SnapShotControl.last_snapshot = {"test_sink": {"PSD": 1000000000}}

        mock_sink_element.descriptions = ["PSD"]
        mock_sink_element.extensions = ["xml.gz"]
        mock_sink_element.startup_delays = {"PSD": 100}

        with (
            mock.patch("sgnl.control.tconvert", return_value=1000014400),
            mock.patch("os.mkdir"),
        ):
            filename = mock_sink_element.snapshot_filenames("PSD", ifos="H1L1")

        # With startup delay, start is adjusted back and duration extended
        # Directory is first 5 chars of start time (999999900 -> 99999)
        assert filename == "99999/H1L1-PSD-999999900-14500.xml.gz"
        # Startup delay should be reset to 0
        assert mock_sink_element.startup_delays["PSD"] == 0

    def test_snapshot_filenames_creates_directory(self, mock_sink_element, tmp_path):
        """Test snapshot_filenames creates GPS directory."""
        control.SnapShotControl.last_snapshot = {"test_sink": {"PSD": 1000000000}}

        mock_sink_element.descriptions = ["PSD"]
        mock_sink_element.extensions = ["xml.gz"]
        mock_sink_element.startup_delays = {"PSD": 0}

        with (
            mock.patch("sgnl.control.tconvert", return_value=1000014400),
            mock.patch("os.mkdir") as mock_mkdir,
        ):
            mock_sink_element.snapshot_filenames("PSD")

        mock_mkdir.assert_called_once_with("10000")

    def test_snapshot_filenames_handles_existing_directory(self, mock_sink_element):
        """Test snapshot_filenames handles OSError when directory exists."""
        control.SnapShotControl.last_snapshot = {"test_sink": {"PSD": 1000000000}}

        mock_sink_element.descriptions = ["PSD"]
        mock_sink_element.extensions = ["xml.gz"]
        mock_sink_element.startup_delays = {"PSD": 0}

        with (
            mock.patch("sgnl.control.tconvert", return_value=1000014400),
            mock.patch("os.mkdir", side_effect=OSError("Directory exists")),
        ):
            # Should not raise
            filename = mock_sink_element.snapshot_filenames("PSD")

        assert "PSD" in filename
