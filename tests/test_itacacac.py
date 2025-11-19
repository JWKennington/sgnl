"""Tests for Itacacac transform's usage of EventFrame/EventBuffer API"""

import torch
from sgnts.base import EventBuffer

from sgnl.transforms.itacacac import Itacacac


def test_itacacac_output_events_structure():
    """Test that output_events returns EventBuffer with correct dictionary structure"""
    # Create a minimal Itacacac instance
    itacacac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths=torch.tensor([10]),
        template_ids=torch.tensor([[0]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )

    # Mock clustered_coinc data
    clustered_coinc = {
        "sngls": {
            "H1": {
                "time": torch.tensor([1000000000]),
                "shifted_time": torch.tensor([1000000]),
                "snr": torch.tensor([5.0]),
                "chisq": torch.tensor([1.0]),
                "phase": torch.tensor([0.0]),
            }
        },
        "clustered_template_ids": torch.tensor([0]),
        "clustered_template_durations": torch.tensor([1.0]),
        "clustered_ifo_combs": torch.tensor([1]),
        "clustered_snr": torch.tensor([5.0]),
        "clustered_bankids": [0],
        "snr_ts_snippet_clustered": {"H1": torch.zeros(1, 2, 10)},
        "snr_ts_clustered": {"H1": torch.zeros(1, 2, 100)},
    }

    ts, te = 1000000000, 1001000000
    result = itacacac.output_events(clustered_coinc, ts, te)

    # Test that result is an EventBuffer
    assert isinstance(result, EventBuffer)

    # Test that data is a list with one dictionary
    assert isinstance(result.data, list)
    assert len(result.data) == 1
    assert isinstance(result.data[0], dict)

    # Test that dictionary has expected keys
    expected_keys = {"event", "trigger", "snr_ts", "max_snr_histories"}
    assert set(result.data[0].keys()) == expected_keys

    # Test that event data is a list
    assert isinstance(result.data[0]["event"], list)
    assert len(result.data[0]["event"]) == 1

    # Test that trigger data is a list
    assert isinstance(result.data[0]["trigger"], list)
    assert len(result.data[0]["trigger"]) == 1


def test_itacacac_output_background_structure():
    """Test that output_background returns EventBuffer with correct structure"""
    # Create a minimal Itacacac instance with strike pad
    itacacac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths=torch.tensor([10]),
        template_ids=torch.tensor([[0]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        strike_pad="strike",
        stillsuit_pad="stillsuit",
    )

    # Mock triggers data
    triggers = {
        "snrs": {"H1": torch.tensor([[5.0]])},
        "chisqs": {"H1": torch.tensor([[1.0]])},
    }
    single_background_masks = {"H1": torch.tensor([[True]])}

    ts, te = 1000000000, 1001000000
    result = itacacac.output_background(triggers, single_background_masks, ts, te)

    # Test that result is an EventBuffer
    assert isinstance(result, EventBuffer)

    # Test that data is a list with one dictionary
    assert isinstance(result.data, list)
    assert len(result.data) == 1
    assert isinstance(result.data[0], dict)

    # Test that dictionary has expected keys
    expected_keys = {"trigger_rates", "background"}
    assert set(result.data[0].keys()) == expected_keys
