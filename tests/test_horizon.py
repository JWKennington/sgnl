"""Tests for sgnl.transforms.horizon module."""

from unittest import mock

import lal
import numpy as np

from sgnl.transforms import horizon


class TestHorizonDistanceTrackerDataclass:
    """Tests for HorizonDistanceTracker dataclass."""

    def test_dataclass_creation(self):
        """Test HorizonDistanceTracker dataclass creation."""
        with mock.patch.object(horizon.TSTransform, "__post_init__"):
            tracker = horizon.HorizonDistanceTracker(
                horizon_distance_funcs=mock.MagicMock(),
                ifo="H1",
                range=False,
            )
            assert tracker.ifo == "H1"
            assert tracker.range is False

    def test_dataclass_with_dict_funcs(self):
        """Test HorizonDistanceTracker with dict of funcs."""
        with mock.patch.object(horizon.TSTransform, "__post_init__"):
            funcs = {"bank1": mock.MagicMock(), "bank2": mock.MagicMock()}
            tracker = horizon.HorizonDistanceTracker(
                horizon_distance_funcs=funcs,
                ifo="L1",
                range=True,
            )
            assert tracker.horizon_distance_funcs == funcs
            assert tracker.range is True


class TestHorizonDistanceTrackerPostInit:
    """Tests for HorizonDistanceTracker.__post_init__ method."""

    @mock.patch.object(horizon.TSTransform, "__post_init__")
    def test_post_init_calls_super(self, mock_super_init):
        """Test __post_init__ calls super().__post_init__()."""
        horizon.HorizonDistanceTracker(
            horizon_distance_funcs=mock.MagicMock(),
            ifo="H1",
        )
        mock_super_init.assert_called_once()


class TestHorizonDistanceTrackerNew:
    """Tests for HorizonDistanceTracker.new method."""

    def _create_tracker(self, funcs=None, ifo="H1", range_mode=False):
        """Helper to create a tracker with mocked parent."""
        with mock.patch.object(horizon.TSTransform, "__post_init__"):
            tracker = horizon.HorizonDistanceTracker(
                horizon_distance_funcs=funcs,
                ifo=ifo,
                range=range_mode,
            )
            return tracker

    def _create_mock_frame(self, psd=None, navg=10, n_samples=100, epoch=1000000000):
        """Helper to create a mock frame."""
        frame = mock.MagicMock()
        frame.EOS = False
        frame.shape = (1024,)
        frame.offset = 0
        frame.sample_rate = 16384
        frame.metadata = {
            "psd": psd,
            "navg": navg,
            "n_samples": n_samples,
            "epoch": epoch,
        }
        return frame

    def _create_mock_psd(self):
        """Helper to create a mock PSD."""
        psd = lal.CreateREAL8FrequencySeries(
            name="test_psd",
            epoch=lal.LIGOTimeGPS(0),
            f0=0.0,
            deltaF=1.0,
            sampleUnits=lal.DimensionlessUnit,
            length=100,
        )
        psd.data.data = np.ones(100)
        return psd

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_dict_funcs_and_psd(self, mock_event_buffer, mock_event_frame):
        """Test new method with dict of horizon_distance_funcs and valid PSD."""
        func1 = mock.MagicMock(return_value=(100.0, None))
        func2 = mock.MagicMock(return_value=(150.0, None))
        funcs = {"bank1": func1, "bank2": func2}

        tracker = self._create_tracker(funcs=funcs, ifo="H1", range_mode=False)
        psd = self._create_mock_psd()
        frame = self._create_mock_frame(psd=psd)

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        mock_event_frame.assert_called_once()
        func1.assert_called_once_with(psd, 8)
        func2.assert_called_once_with(psd, 8)

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_callable_func_and_psd(self, mock_event_buffer, mock_event_frame):
        """Test new method with callable horizon_distance_funcs (not dict)."""
        func = mock.MagicMock(return_value=(100.0, None))

        tracker = self._create_tracker(funcs=func, ifo="H1", range_mode=False)
        psd = self._create_mock_psd()
        frame = self._create_mock_frame(psd=psd)

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        mock_event_frame.assert_called_once()
        func.assert_called_once_with(psd, 8)

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_range_true_and_psd(self, mock_event_buffer, mock_event_frame):
        """Test new method with range=True and valid PSD."""
        func = mock.MagicMock(return_value=(100.0, None))

        tracker = self._create_tracker(funcs=func, ifo="H1", range_mode=True)
        psd = self._create_mock_psd()
        frame = self._create_mock_frame(psd=psd)

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        mock_event_frame.assert_called_once()
        # Check that EventBuffer.from_span was called with range_history data
        call_args = mock_event_buffer.from_span.call_args
        data = call_args[0][2][0]
        assert "range_history" in data
        assert data["range_history"]["data"][0] == 100.0 / 2.25

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_none_psd_and_range_false(
        self, mock_event_buffer, mock_event_frame
    ):
        """Test new method when psd is None and range is False."""
        func = mock.MagicMock()

        tracker = self._create_tracker(funcs=func, ifo="H1", range_mode=False)
        frame = self._create_mock_frame(psd=None)

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        mock_event_frame.assert_called_once()
        # Check that EventBuffer.from_span was called with horizon=None data
        call_args = mock_event_buffer.from_span.call_args
        data = call_args[0][2][0]
        assert data["horizon"] is None
        assert data["ifo"] == "H1"
        assert data["navg"] is None
        assert data["n_samples"] is None

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_none_psd_and_range_true(
        self, mock_event_buffer, mock_event_frame
    ):
        """Test new method when psd is None and range is True."""
        func = mock.MagicMock()

        tracker = self._create_tracker(funcs=func, ifo="H1", range_mode=True)
        frame = self._create_mock_frame(psd=None)

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        mock_event_frame.assert_called_once()
        # Check that EventBuffer.from_span was called with data=None
        call_args = mock_event_buffer.from_span.call_args
        data = call_args[0][2][0]
        assert data is None

    @mock.patch("sgnl.transforms.horizon.EventFrame")
    @mock.patch("sgnl.transforms.horizon.EventBuffer")
    def test_new_with_eos_true(self, mock_event_buffer, mock_event_frame):
        """Test new method with EOS=True."""
        func = mock.MagicMock(return_value=(100.0, None))

        tracker = self._create_tracker(funcs=func, ifo="H1", range_mode=False)
        psd = self._create_mock_psd()
        frame = self._create_mock_frame(psd=psd)
        frame.EOS = True

        tracker.sink_pads = ["sink_pad"]
        tracker.preparedframes = {"sink_pad": frame}

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        tracker.new("any_pad")

        # Check that EventFrame was called with EOS=True
        call_args = mock_event_frame.call_args
        assert call_args[1]["EOS"] is True
