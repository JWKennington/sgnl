"""Tests for Strike transform's usage of EventFrame/EventBuffer API"""

from unittest.mock import MagicMock

from sgnts.base import EventBuffer, EventFrame

from sgnl.transforms.strike import StrikeTransform


class TestStrikeTransformDataclass:
    """Tests for StrikeTransform dataclass and initialization."""

    def test_post_init(self):
        """Test __post_init__ sets up frame attributes."""
        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: None}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        assert strike.frame is None
        assert strike.output_frame is None

    def test_pull(self):
        """Test pull method stores the frame."""
        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: None}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        mock_frame = MagicMock()
        strike.pull("events", mock_frame)

        assert strike.frame is mock_frame

    def test_new_returns_output_frame(self):
        """Test new method returns the output_frame (line 105)."""
        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: None}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Set up a mock output frame
        mock_output = MagicMock()
        strike.output_frame = mock_output

        result = strike.new("output")
        assert result is mock_output


class TestStrikeTransformInternal:
    """Tests for StrikeTransform.internal method."""

    def test_internal_event_is_none(self):
        """Test internal when event is None (line 40)."""
        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: None}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Create EventFrame with None event
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[None])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        assert isinstance(strike.output_frame, EventFrame)

    def test_internal_event_dict_event_is_none(self):
        """Test internal when event dict has event=None (line 40)."""
        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: None}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Create EventFrame with event dict where "event" key is None
        event_data = {"event": None, "trigger": None}
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        assert isinstance(strike.output_frame, EventFrame)

    def test_internal_with_frankenstein_and_fapfar(self):
        """Test internal with frankenstein computing likelihood and FAR."""
        mock_frankenstein = MagicMock()
        mock_frankenstein.ln_lr_from_triggers.return_value = 10.5

        mock_fapfar = MagicMock()
        mock_fapfar.fap_from_rank.return_value = 0.001
        mock_fapfar.far_from_rank.return_value = 1e-6
        mock_fapfar.livetime = 86400  # 1 day

        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: mock_frankenstein}
        mock_strike_object.offset_vectors = MagicMock()
        mock_strike_object.fapfar = mock_fapfar
        mock_strike_object.FAR_trialsfactor = 1.0
        mock_strike_object.cap_singles = False
        mock_strike_object.zerolag_rank_stat_pdfs = None

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Create event with trigger data
        event_data = {
            "event": [{"bankid": 123, "network_snr": 5.0}],
            "trigger": [
                [
                    {
                        "time": 1000000000000000000,  # nanoseconds
                        "epoch_start": 1000000000000000000,
                        "epoch_end": 1001000000000000000,
                        "snr": 5.0,
                        "ifo": "H1",
                    }
                ]
            ],
        }
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        # Check that likelihood and FAR were computed
        assert event_data["event"][0]["likelihood"] == 10.5
        assert event_data["event"][0]["false_alarm_probability"] == 0.001
        mock_frankenstein.ln_lr_from_triggers.assert_called_once()

    def test_internal_with_cap_singles(self):
        """Test internal with cap_singles logic (lines 76-83)."""
        mock_frankenstein = MagicMock()
        mock_frankenstein.ln_lr_from_triggers.return_value = 10.5

        mock_fapfar = MagicMock()
        mock_fapfar.fap_from_rank.return_value = 0.001
        # Very low FAR that triggers cap_singles
        mock_fapfar.far_from_rank.return_value = 1e-10
        mock_fapfar.livetime = 86400  # 1 day

        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: mock_frankenstein}
        mock_strike_object.offset_vectors = MagicMock()
        mock_strike_object.fapfar = mock_fapfar
        mock_strike_object.FAR_trialsfactor = 1.0
        mock_strike_object.cap_singles = True
        mock_strike_object.zerolag_rank_stat_pdfs = None

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Single trigger event
        event_data = {
            "event": [{"bankid": 123, "network_snr": 5.0}],
            "trigger": [
                [
                    {
                        "time": 1000000000000000000,
                        "epoch_start": 1000000000000000000,
                        "epoch_end": 1001000000000000000,
                        "snr": 5.0,
                        "ifo": "H1",
                    }
                ]
            ],
        }
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        # Check that cap_singles was applied
        expected_capped_far = 1.0 / mock_fapfar.livetime
        assert event_data["event"][0]["combined_far"] == expected_capped_far

    def test_internal_with_fapfar_none(self):
        """Test internal when fapfar is None (lines 84-86)."""
        mock_frankenstein = MagicMock()
        mock_frankenstein.ln_lr_from_triggers.return_value = 10.5

        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: mock_frankenstein}
        mock_strike_object.offset_vectors = MagicMock()
        mock_strike_object.fapfar = None  # No fapfar
        mock_strike_object.zerolag_rank_stat_pdfs = None

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        event_data = {
            "event": [{"bankid": 123, "network_snr": 5.0}],
            "trigger": [
                [
                    {
                        "time": 1000000000000000000,
                        "epoch_start": 1000000000000000000,
                        "epoch_end": 1001000000000000000,
                        "snr": 5.0,
                        "ifo": "H1",
                    }
                ]
            ],
        }
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        # Likelihood computed but FAP/FAR are None
        assert event_data["event"][0]["likelihood"] == 10.5
        assert event_data["event"][0]["false_alarm_probability"] is None
        assert event_data["event"][0]["combined_far"] is None

    def test_internal_with_zerolag_rank_stat_pdfs(self):
        """Test internal with zerolag_rank_stat_pdfs counting (lines 88-93)."""
        mock_frankenstein = MagicMock()
        mock_frankenstein.ln_lr_from_triggers.return_value = 10.5

        mock_count = MagicMock()
        mock_zerolag_pdf = MagicMock()
        mock_zerolag_pdf.zero_lag_lr_lnpdf.count = mock_count

        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: mock_frankenstein}
        mock_strike_object.offset_vectors = MagicMock()
        mock_strike_object.fapfar = None
        mock_strike_object.zerolag_rank_stat_pdfs = {123: mock_zerolag_pdf}

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        event_data = {
            "event": [{"bankid": 123, "network_snr": 5.0}],
            "trigger": [
                [
                    {
                        "time": 1000000000000000000,
                        "epoch_start": 1000000000000000000,
                        "epoch_end": 1001000000000000000,
                        "snr": 5.0,
                        "ifo": "H1",
                    }
                ]
            ],
        }
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        # Check that count was incremented
        mock_count.__setitem__.assert_called()

    def test_internal_trigger_is_none_in_list(self):
        """Test internal when trigger in list is None (line 48 condition)."""
        mock_frankenstein = MagicMock()
        mock_frankenstein.ln_lr_from_triggers.return_value = 10.5

        mock_strike_object = MagicMock()
        mock_strike_object.frankensteins = {123: mock_frankenstein}
        mock_strike_object.offset_vectors = MagicMock()
        mock_strike_object.fapfar = None
        mock_strike_object.zerolag_rank_stat_pdfs = None

        strike = StrikeTransform(
            name="test_strike",
            sink_pad_names=["events"],
            source_pad_names=["output"],
            strike_object=mock_strike_object,
        )

        # Trigger list with a None entry
        event_data = {
            "event": [{"bankid": 123, "network_snr": 5.0}],
            "trigger": [
                [
                    {
                        "time": 1000000000000000000,
                        "epoch_start": 1000000000000000000,
                        "epoch_end": 1001000000000000000,
                        "snr": 5.0,
                        "ifo": "H1",
                    },
                    None,  # None trigger in the list
                ]
            ],
        }
        buffer = EventBuffer.from_span(1000000000, 1001000000, data=[event_data])
        input_frame = EventFrame(data=[buffer], EOS=False)

        strike.pull("events", input_frame)
        strike.internal()

        # Should still compute likelihood (skipping None triggers)
        assert event_data["event"][0]["likelihood"] == 10.5


def test_strike_eventframe_passthrough():
    """Test that StrikeTransform passes through EventFrame data correctly"""
    mock_strike_object = MagicMock()
    mock_strike_object.frankensteins = {
        123: None
    }  # No likelihood computation for simplicity

    strike = StrikeTransform(
        name="test_strike",
        sink_pad_names=["events"],
        source_pad_names=["output"],
        strike_object=mock_strike_object,
    )

    # Create mock EventFrame with new API structure that matches what Strike expects
    original_data = [
        {
            "event": [{"network_snr": 5.0, "time": 1000000000, "bankid": 123}],
            "trigger": [
                [
                    {
                        "_filter_id": 123,
                        "ifo": "H1",
                        "snr": 5.0,
                        "chisq": 1.0,
                        "phase": 0.0,
                        "time": 1000000000,
                        "epoch_start": 1000000000,
                        "epoch_end": 1001000000,
                    }
                ]
            ],
            "snr_ts": [{}],
            "max_snr_histories": {"H1": {"time": 1000000000, "snr": 5.0}},
        }
    ]

    original_buffer = EventBuffer.from_span(1000000000, 1001000000, data=original_data)

    input_frame = EventFrame(data=[original_buffer], EOS=False)

    # Test the pull and internal methods
    strike.pull("events", input_frame)
    strike.internal()
    result_frame = strike.output_frame

    # Test that result is an EventFrame
    assert isinstance(result_frame, EventFrame)

    # Test that EOS is preserved
    assert result_frame.EOS == input_frame.EOS

    # Test that data structure is preserved
    assert len(result_frame.data) == 1
    assert isinstance(result_frame.data[0], EventBuffer)

    # Test that the data content is preserved
    result_data = result_frame.data[0].data
    assert isinstance(result_data, list)
    assert len(result_data) == 1
    assert isinstance(result_data[0], dict)

    # Test that the event structure is preserved (Strike passes data through)
    assert "event" in result_data[0]
    assert "trigger" in result_data[0]
    assert "snr_ts" in result_data[0]
    assert "max_snr_histories" in result_data[0]


def test_strike_multiple_events_processing():
    """Test that StrikeTransform processes multiple events correctly"""
    mock_strike_object = MagicMock()
    mock_strike_object.frankensteins = {
        123: None,
        124: None,
    }  # No likelihood computation for simplicity

    strike = StrikeTransform(
        name="test_strike",
        sink_pad_names=["events"],
        source_pad_names=["output"],
        strike_object=mock_strike_object,
    )

    # Create mock EventFrame with multiple events
    event1_data = {
        "event": [{"network_snr": 4.0, "time": 1000000000, "bankid": 123}],
        "trigger": [
            [
                {
                    "_filter_id": 123,
                    "ifo": "H1",
                    "snr": 4.0,
                    "chisq": 1.0,
                    "phase": 0.0,
                    "time": 1000000000,
                    "epoch_start": 1000000000,
                    "epoch_end": 1001000000,
                }
            ]
        ],
        "snr_ts": [{}],
        "max_snr_histories": {"H1": {"time": 1000000000, "snr": 4.0}},
    }
    event2_data = {
        "event": [{"network_snr": 6.0, "time": 1001000000, "bankid": 124}],
        "trigger": [
            [
                {
                    "_filter_id": 124,
                    "ifo": "H1",
                    "snr": 6.0,
                    "chisq": 1.0,
                    "phase": 0.0,
                    "time": 1001000000,
                    "epoch_start": 1001000000,
                    "epoch_end": 1002000000,
                }
            ]
        ],
        "snr_ts": [{}],
        "max_snr_histories": {"H1": {"time": 1001000000, "snr": 6.0}},
    }

    buffer1 = EventBuffer.from_span(1000000000, 1001000000, data=[event1_data])
    buffer2 = EventBuffer.from_span(1001000000, 1002000000, data=[event2_data])

    input_frame = EventFrame(data=[buffer1, buffer2], EOS=True)

    # Test the pull and internal methods
    strike.pull("events", input_frame)
    strike.internal()
    result_frame = strike.output_frame

    # Test that result preserves structure
    assert isinstance(result_frame, EventFrame)
    assert result_frame.EOS == input_frame.EOS
    assert len(result_frame.data) == 2

    # Test that both events are processed correctly
    for buffer in result_frame.data:
        assert isinstance(buffer, EventBuffer)
        assert isinstance(buffer.data, list)
        assert len(buffer.data) == 1
        assert isinstance(buffer.data[0], dict)
        assert "event" in buffer.data[0]
        assert "trigger" in buffer.data[0]
