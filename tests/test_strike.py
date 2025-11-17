"""Tests for Strike transform's usage of EventFrame/EventBuffer API"""

from sgnts.base import EventBuffer, EventFrame

from sgnl.transforms.strike import StrikeTransform


def test_strike_eventframe_passthrough():
    """Test that StrikeTransform passes through EventFrame data correctly"""
    # Create a minimal StrikeTransform instance
    from unittest.mock import MagicMock

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
    # Create a minimal StrikeTransform instance
    from unittest.mock import MagicMock

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
