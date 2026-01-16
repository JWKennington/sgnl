"""Tests for sgnl.transforms.eye_candy module."""

import math
from unittest import mock

from sgnl.transforms import eye_candy


class TestEyeCandyDataclass:
    """Tests for EyeCandy dataclass."""

    def test_eyecandy_creation(self):
        """Test EyeCandy dataclass creation with mocked parent."""
        with mock.patch.object(eye_candy.EyeCandy, "__post_init__"):
            ec = eye_candy.EyeCandy(
                template_sngls=[{"tid1": mock.MagicMock()}],
                event_pad="events",
                state_vector_pads={"H1": "sv_H1", "L1": "sv_L1"},
                ht_gate_pads={"H1": "ht_H1", "L1": "ht_L1"},
            )
            assert ec.event_pad == "events"
            assert "H1" in ec.state_vector_pads


class TestEyeCandyPostInit:
    """Tests for EyeCandy.__post_init__ method."""

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=1000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "__post_init__")
    def test_post_init_initializes_attributes(self, mock_super_init, mock_now):
        """Test __post_init__ initializes all required attributes."""
        sngl1 = mock.MagicMock()
        sngl2 = mock.MagicMock()
        template_sngls = [{"tid1": sngl1}, {"tid2": sngl2}]

        ec = eye_candy.EyeCandy(
            template_sngls=template_sngls,
            event_pad="events",
            state_vector_pads={"H1": "sv_H1", "L1": "sv_L1"},
            ht_gate_pads={"H1": "ht_H1", "L1": "ht_L1"},
        )

        # Check sink_pad_names
        assert ec.sink_pad_names == ("events", "sv_H1", "sv_L1", "ht_H1", "ht_L1")

        # Check ifos
        assert set(ec.ifos) == {"H1", "L1"}

        # Check sngls_dict
        assert ec.sngls_dict == {"tid1": sngl1, "tid2": sngl2}

        # Check other initializations
        assert ec.outframe is None
        assert ec.startup_time == 1000000000000000000.0
        assert "events" in ec.frames

        mock_super_init.assert_called_once()


class TestEyeCandyPull:
    """Tests for EyeCandy.pull method."""

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=1000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "__post_init__")
    def test_pull_stores_frame(self, mock_super_init, mock_now):
        """Test pull method stores frames correctly."""
        ec = eye_candy.EyeCandy(
            template_sngls=[{}],
            event_pad="events",
            state_vector_pads={"H1": "sv_H1"},
            ht_gate_pads={"H1": "ht_H1"},
        )
        ec.rsnks = {"pad1": "sv_H1"}

        mock_frame = mock.MagicMock()
        ec.pull("pad1", mock_frame)

        assert ec.frames["sv_H1"] == mock_frame


class TestEyeCandyInternal:
    """Tests for EyeCandy.internal method."""

    def _create_eye_candy(self):
        """Helper to create a properly initialized EyeCandy instance."""
        with mock.patch(
            "sgnl.transforms.eye_candy.now", return_value=2000000000000000000
        ):
            with mock.patch.object(eye_candy.TransformElement, "__post_init__"):
                sngl = mock.MagicMock()
                sngl.mass1 = 1.4
                sngl.mass2 = 1.4
                sngl.spin1z = 0.0
                sngl.spin2z = 0.0

                ec = eye_candy.EyeCandy(
                    template_sngls=[{"tid1": sngl}],
                    event_pad="events",
                    state_vector_pads={"H1": "sv_H1"},
                    ht_gate_pads={"H1": "ht_H1"},
                )
                return ec

    def _create_mock_buffer(self, t0=1000000000000000000, is_gap=False):
        """Helper to create a mock buffer."""
        buf = mock.MagicMock()
        buf.t0 = t0
        buf.is_gap = is_gap
        return buf

    def _create_mock_event_frame(
        self,
        events=None,
        start=1000000000000000000,
        end=1001000000000000000,
        eos=False,
    ):
        """Helper to create a mock event frame."""
        frame = mock.MagicMock()
        frame.events = events if events is not None else []
        frame.start = start
        frame.end = end
        frame.EOS = eos
        return frame

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_no_events(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal method when there are no events."""
        ec = self._create_eye_candy()

        # Set up state vector frame
        sv_buf = self._create_mock_buffer(is_gap=False)
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        # Set up ht gate frame
        ht_buf = self._create_mock_buffer(is_gap=True)
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Set up event frame with no events
        event_frame = self._create_mock_event_frame(
            events=[{"max_snr_histories": None, "event": None, "trigger": None}]
        )

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_super_internal.assert_called_once()
        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_max_snr_histories(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with max_snr_histories data."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Event with max_snr_histories
        event_frame = self._create_mock_event_frame(
            events=[
                {
                    "max_snr_histories": {"H1": {"time": 1000.0, "snr": 10.5}},
                    "event": None,
                    "trigger": None,
                }
            ]
        )

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_events_and_triggers(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with events and triggers."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Event with actual events and triggers
        trigger = {
            "_filter_id": "tid1",
            "ifo": "H1",
            "snr": 8.5,
            "chisq": 1.0,
            "phase": 0.5,
        }
        event = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": 100.0,
                    "combined_far": 1e-10,
                    "false_alarm_probability": 0.001,
                }
            ],
            "trigger": [[trigger]],
        }
        event_frame = self._create_mock_event_frame(events=[event])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_none_likelihood(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with events that have None likelihood."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Event with None likelihood
        event = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": None,
                    "combined_far": None,
                    "false_alarm_probability": None,
                }
            ],
            "trigger": [[None]],
        }
        event_frame = self._create_mock_event_frame(events=[event])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_inf_likelihood(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with events that have -inf likelihood."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Event with -inf likelihood
        trigger = {
            "_filter_id": "tid1",
            "ifo": "H1",
            "snr": 8.5,
            "chisq": 1.0,
            "phase": 0.5,
        }
        event = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": -math.inf,
                    "combined_far": 1e-10,
                    "false_alarm_probability": 0.001,
                }
            ],
            "trigger": [[trigger]],
        }
        event_frame = self._create_mock_event_frame(events=[event])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_multiple_events_multiple_triggers(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with multiple events and triggers."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        trigger1 = {
            "_filter_id": "tid1",
            "ifo": "H1",
            "snr": 8.5,
            "chisq": 1.0,
            "phase": 0.5,
        }
        trigger2 = {
            "_filter_id": "tid1",
            "ifo": "L1",
            "snr": 7.0,
            "chisq": 1.2,
            "phase": 0.3,
        }

        event1 = {
            "max_snr_histories": {"H1": {"time": 1000.0, "snr": 8.5}},
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": 100.0,
                    "combined_far": 1e-10,
                    "false_alarm_probability": 0.001,
                }
            ],
            "trigger": [[trigger1, trigger2]],
        }
        event2 = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 15.0,
                    "time": 1001000000000000000,
                    "likelihood": 150.0,
                    "combined_far": 1e-12,
                    "false_alarm_probability": 0.0001,
                }
            ],
            "trigger": [[trigger1]],
        }
        event_frame = self._create_mock_event_frame(events=[event1, event2])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_none_trigger_in_list(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal with None trigger in trigger list."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Mix of None and actual triggers
        trigger1 = {
            "_filter_id": "tid1",
            "ifo": "H1",
            "snr": 8.5,
            "chisq": 1.0,
            "phase": 0.5,
        }
        event = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": 100.0,
                    "combined_far": 1e-10,
                    "false_alarm_probability": 0.001,
                }
            ],
            "trigger": [[trigger1, None]],  # Mix of trigger and None
        }
        event_frame = self._create_mock_event_frame(events=[event])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=2000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "internal")
    @mock.patch("sgnl.transforms.eye_candy.EventFrame")
    @mock.patch("sgnl.transforms.eye_candy.EventBuffer")
    def test_internal_with_only_far_no_likelihood_history(
        self, mock_event_buffer, mock_event_frame, mock_super_internal, mock_now
    ):
        """Test internal when likelihood is inf but combined_far is valid."""
        ec = self._create_eye_candy()

        sv_buf = self._create_mock_buffer()
        sv_frame = mock.MagicMock()
        sv_frame.__iter__ = mock.MagicMock(return_value=iter([sv_buf]))

        ht_buf = self._create_mock_buffer()
        ht_frame = mock.MagicMock()
        ht_frame.__iter__ = mock.MagicMock(return_value=iter([ht_buf]))

        # Event with positive inf likelihood and valid FAR
        trigger = {
            "_filter_id": "tid1",
            "ifo": "H1",
            "snr": 8.5,
            "chisq": 1.0,
            "phase": 0.5,
        }
        event = {
            "max_snr_histories": None,
            "event": [
                {
                    "network_snr": 12.0,
                    "time": 1000000000000000000,
                    "likelihood": math.inf,
                    "combined_far": 1e-10,
                    "false_alarm_probability": 0.001,
                }
            ],
            "trigger": [[trigger]],
        }
        event_frame = self._create_mock_event_frame(events=[event])

        ec.frames = {
            "events": event_frame,
            "sv_H1": sv_frame,
            "ht_H1": ht_frame,
        }

        mock_event_buffer.from_span.return_value = mock.MagicMock()

        ec.internal()

        mock_event_frame.assert_called_once()


class TestEyeCandyNew:
    """Tests for EyeCandy.new method."""

    @mock.patch("sgnl.transforms.eye_candy.now", return_value=1000000000000000000)
    @mock.patch.object(eye_candy.TransformElement, "__post_init__")
    def test_new_returns_outframe(self, mock_super_init, mock_now):
        """Test new method returns outframe."""
        ec = eye_candy.EyeCandy(
            template_sngls=[{}],
            event_pad="events",
            state_vector_pads={"H1": "sv_H1"},
            ht_gate_pads={"H1": "ht_H1"},
        )
        mock_frame = mock.MagicMock()
        ec.outframe = mock_frame

        result = ec.new("any_pad")

        assert result == mock_frame
