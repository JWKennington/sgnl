"""Tests for Itacacac transform's usage of EventFrame/EventBuffer API"""

import pytest
import torch
from sgnts.base import EventBuffer

from sgnl.transforms.itacacac import Itacacac, index_select, light_travel_time


def test_itacacac_output_events_structure():
    """Test that output_events returns EventBuffer with correct dictionary structure"""
    # Create a minimal Itacacac instance
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
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
    result = itac.output_events(clustered_coinc, ts, te)

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
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
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
    result = itac.output_background(triggers, single_background_masks, ts, te)

    # Test that result is an EventBuffer
    assert isinstance(result, EventBuffer)

    # Test that data is a list with one dictionary
    assert isinstance(result.data, list)
    assert len(result.data) == 1
    assert isinstance(result.data[0], dict)

    # Test that dictionary has expected keys
    expected_keys = {"trigger_rates", "background"}
    assert set(result.data[0].keys()) == expected_keys


def test_index_select():
    """Test the index_select helper function."""
    tensor = torch.tensor([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    index = torch.tensor([0, 2, 1])
    result = index_select(tensor, dim=1, index=index)
    expected = torch.tensor([1, 6, 8])
    assert torch.equal(result, expected)


def test_light_travel_time():
    """Test light_travel_time function."""
    # Test with H1 and L1
    dt = light_travel_time("H1", "L1")
    assert dt > 0
    assert dt < 0.02  # Should be about 10ms


def test_itacacac_with_autocorrelation_length_mask():
    """Test Itacacac with autocorrelation_length_mask set."""
    mask = {"H1": torch.ones(1, 1, 10)}
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=mask,
        autocorrelation_lengths={0: 10},
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
    # Verify autocorrelation_norms was computed with mask
    assert "H1" in itac.autocorrelation_norms


def test_make_coincs_single_ifo_min_instruments_2():
    """Test make_coincs with single ifo and min_instruments_candidates=2."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 2, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=2,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )

    triggers = {
        "snrs": {"H1": torch.tensor([[5.0, 3.0]])},
        "peak_locations": {"H1": torch.tensor([[100, 101]])},
        "chisqs": {"H1": torch.tensor([[1.0, 1.0]])},
    }

    ifo_combs, network_snr, single_masks, noevents_mask = itac.make_coincs(triggers)

    # With min_instruments_candidates=2, single ifo should not produce events
    assert ifo_combs is None or torch.all(noevents_mask)


def test_make_coincs_nifo_greater_than_3():
    """Test make_coincs raises ValueError for nifo > 3."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1", "V1", "K1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "V1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "K1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )

    triggers = {
        "snrs": {
            "H1": torch.tensor([[5.0, 3.0]]),
            "L1": torch.tensor([[5.0, 3.0]]),
            "V1": torch.tensor([[5.0, 3.0]]),
            "K1": torch.tensor([[5.0, 3.0]]),
        },
        "peak_locations": {
            "H1": torch.tensor([[100, 101]]),
            "L1": torch.tensor([[100, 101]]),
            "V1": torch.tensor([[100, 101]]),
            "K1": torch.tensor([[100, 101]]),
        },
    }

    with pytest.raises(ValueError, match="nifo > 3 is not implemented"):
        itac.make_coincs(triggers)


def test_coinc2_min_instruments_2():
    """Test coinc2 with min_instruments_candidates=2."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=2,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )
    itac.rate = 1024

    snrs = [torch.tensor([[5.0, 3.0]]), torch.tensor([[3.0, 5.0]])]
    times = [torch.tensor([[100, 101]]), torch.tensor([[200, 201]])]

    result = itac.coinc2(snrs, times, ["H1", "L1"])

    # With min_instruments_candidates=2, single_mask should be None
    assert result[1] is None  # single_mask1
    assert result[2] is None  # single_mask2


def test_coinc3_min_instruments_2():
    """Test coinc3 with min_instruments_candidates=2."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1", "V1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "V1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=2,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )
    itac.rate = 1024

    triggers = {
        "snrs": {
            "H1": torch.tensor([[5.0, 3.0]]),
            "L1": torch.tensor([[5.0, 3.0]]),
            "V1": torch.tensor([[5.0, 3.0]]),
        },
        "peak_locations": {
            "H1": torch.tensor([[100, 101]]),
            "L1": torch.tensor([[200, 201]]),
            "V1": torch.tensor([[300, 301]]),
        },
    }

    result = itac.coinc3(triggers)

    # With min_instruments_candidates=2, single masks should be None
    assert result[4] is None  # single_mask1
    assert result[5] is None  # single_mask2
    assert result[6] is None  # single_mask3


def test_coinc3_all_triggers_to_background():
    """Test coinc3 with all_triggers_to_background=True."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1", "V1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "V1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        all_triggers_to_background=True,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )
    itac.rate = 1024

    triggers = {
        "snrs": {
            "H1": torch.tensor([[5.0, 3.0]]),
            "L1": torch.tensor([[5.0, 3.0]]),
            "V1": torch.tensor([[5.0, 3.0]]),
        },
        "peak_locations": {
            "H1": torch.tensor([[100, 101]]),
            "L1": torch.tensor([[200, 201]]),
            "V1": torch.tensor([[300, 301]]),
        },
    }

    result = itac.coinc3(triggers)

    # With all_triggers_to_background=True, background masks should include all
    # triggers above snr_min
    assert result[7] is not None  # single_background_mask1
    assert result[8] is not None  # single_background_mask2
    assert result[9] is not None  # single_background_mask3


def test_make_coincs_two_ifo():
    """Test make_coincs with 2 ifos."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )
    itac.rate = 1024

    triggers = {
        "snrs": {
            "H1": torch.tensor([[5.0, 3.0]]),
            "L1": torch.tensor([[5.0, 3.0]]),
        },
        "peak_locations": {
            "H1": torch.tensor([[100, 101]]),
            "L1": torch.tensor([[100, 101]]),
        },
    }

    ifo_combs, network_snr, single_masks, noevents_mask = itac.make_coincs(triggers)

    assert ifo_combs is not None
    assert network_snr is not None


def test_make_coincs_two_ifo_all_triggers_to_background():
    """Test make_coincs with 2 ifos and all_triggers_to_background."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1", "L1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={
            "H1": torch.zeros(1, 2, 10, dtype=torch.complex64),
            "L1": torch.zeros(1, 2, 10, dtype=torch.complex64),
        },
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        all_triggers_to_background=True,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )
    itac.rate = 1024

    triggers = {
        "snrs": {
            "H1": torch.tensor([[5.0, 3.0]]),
            "L1": torch.tensor([[5.0, 3.0]]),
        },
        "peak_locations": {
            "H1": torch.tensor([[100, 101]]),
            "L1": torch.tensor([[100, 101]]),
        },
    }

    ifo_combs, network_snr, single_masks, noevents_mask = itac.make_coincs(triggers)

    # Check single_masks includes both ifos
    assert "H1" in single_masks
    assert "L1" in single_masks


def test_make_coincs_single_ifo_all_triggers_to_background():
    """Test make_coincs with single ifo and all_triggers_to_background."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 2, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
        template_ids=torch.tensor([[0, 1]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0, 1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        all_triggers_to_background=True,
        strike_pad=None,
        stillsuit_pad="stillsuit",
    )

    triggers = {
        "snrs": {"H1": torch.tensor([[5.0, 3.0]])},
        "peak_locations": {"H1": torch.tensor([[100, 101]])},
    }

    ifo_combs, network_snr, single_masks, noevents_mask = itac.make_coincs(triggers)

    # Check single_masks includes H1
    assert "H1" in single_masks


def test_output_events_online_with_padding():
    """Test output_events in online mode with different autocorr lengths."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 6},  # Dict mapping bankid to length
        template_ids=torch.tensor([[0]]),
        bankids_map={0: [0]},
        end_time_delta=torch.tensor([0.0]),
        template_durations=torch.tensor([[1.0]]),
        device="cpu",
        coincidence_threshold=0.0,
        min_instruments_candidates=1,
        strike_pad=None,
        stillsuit_pad="stillsuit",
        is_online=True,
    )

    # Create clustered_coinc with snr_ts_snippet that needs padding trimmed
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
        "snr_ts_snippet_clustered": {
            "H1": torch.zeros(1, 2, 10)
        },  # 10 samples, will be trimmed to 6
        "snr_ts_clustered": {"H1": torch.zeros(1, 2, 100)},
    }

    ts, te = 1000000000, 1001000000
    result = itac.output_events(clustered_coinc, ts, te)

    assert isinstance(result, EventBuffer)


def test_output_events_empty_triggers():
    """Test output_events prints when no triggers."""
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 10, dtype=torch.complex64)},
        autocorrelation_length_mask=None,
        autocorrelation_lengths={0: 10},
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

    # Empty clustered_coinc
    clustered_coinc = {
        "sngls": {"H1": {"time": torch.tensor([]), "shifted_time": torch.tensor([])}},
        "clustered_template_ids": torch.tensor([]),
        "clustered_template_durations": torch.tensor([]),
        "clustered_ifo_combs": torch.zeros(0, dtype=torch.int),
        "clustered_snr": torch.tensor([]),
        "clustered_bankids": [],
        "snr_ts_snippet_clustered": {"H1": None},
        "snr_ts_clustered": {"H1": None},
    }

    ts, te = 1000000000, 1001000000
    result = itac.output_events(clustered_coinc, ts, te)

    # Should handle empty case
    assert isinstance(result, EventBuffer)


def test_find_peaks_and_calculate_chisqs_with_mask():
    """Test find_peaks_and_calculate_chisqs with autocorrelation_length_mask."""
    mask = {"H1": torch.ones(1, 1, 20)}
    itac = Itacacac(
        name="test_itacacac",
        sink_pad_names=["H1"],
        sample_rate=1024,
        trigger_finding_duration=1.0,
        snr_min=4.0,
        autocorrelation_banks={"H1": torch.zeros(1, 1, 20, dtype=torch.complex64)},
        autocorrelation_length_mask=mask,
        autocorrelation_lengths={0: 20},
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

    # Create snr time series with shape (nsubbank, ntempmax*2, nsamples)
    # ntempmax*2 because real and imag are interleaved
    nsamples = 100
    snr_ts = {
        "H1": torch.randn(1, 2, nsamples),  # 1 subbank, 1 template * 2 (real/imag)
    }

    triggers = itac.find_peaks_and_calculate_chisqs(snr_ts)

    assert "peak_locations" in triggers
    assert "snrs" in triggers
    assert "chisqs" in triggers
    assert "H1" in triggers["snrs"]
