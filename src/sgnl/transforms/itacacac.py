"""An inspiral trigger, autocorrelation chisq, and coincidence, and clustering
element
"""

# Copyright (C) 2011      Chad Hanna, Kipp Cannon
# Copyright (C) 2018      Cody Messick, Alex Pace
# Copyright (C) 2019      Cody Messick
# Copyright (C) 2024-2025 Yun-Jing Huang

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import combinations
from typing import Any, Dict

import lal
import numpy as np
import torch
from ligo import segments
from sgnts.base import (
    AdapterConfig,
    Array,
    EventBuffer,
    EventFrame,
    Offset,
    TSTransform,
)
from sgnts.base.array_ops import TorchBackend


def index_select(tensor, dim, index):
    return tensor.gather(dim, index.unsqueeze(dim)).squeeze(dim)


def light_travel_time(ifo1: str, ifo2: str) -> float:
    """Compute and return the time required for light to travel through
    free space the distance separating the two ifos. The result is
    returned in seconds.

    Args:
        ifo1:
            str, prefix of the first ifo (e.g., "H1")
        ifo2:
            str, prefix of the first ifo (e.g., "L1")

    Returns:
        float, the light-travel time, in seconds
    """
    dx = (
        lal.cached_detector_by_prefix[ifo1].location
        - lal.cached_detector_by_prefix[ifo2].location
    )
    return math.sqrt((dx * dx).sum()) / lal.C_SI


@dataclass
class Itacacac(TSTransform):
    """An inspiral trigger, autocorrelation chisq, and coincidence, and clustering
    element

    Args:
        sample_rate:
            int, the sample rate of the snr time series
        trigger_finding_duration:
            float, the window to find snr peaks, in seconds
        snr_min:
            float, the minimum snr for identifying triggers
        autocorrelation_banks:
            Array, the autocorrelations of the template bank
        template_ids:
            Array, the template ids as an array
        bankids_map:
            Dict[int, list[int]], the mapping between bankid to the array index in the
            zero-th dimension of the snr time-series array
        end_time_delta:
            Array, the end time correction for the snr peaks
        device:
            str, the device to run the trigger finding function on
        coincidence_threshold:
            float, the time difference threshold to identify coincidence triggers, in
            addition to the light-travel time, in seconds.
        strike_pad:
            str, the source pad name to output triggers to stillsuit
        stillsuit_pad:
            str, the source pad name to output background triggers to strike
    """

    sample_rate: int = None
    trigger_finding_duration: float = None
    snr_min: float = 4
    autocorrelation_banks: Array = None
    template_ids: Array = None
    bankids_map: Dict[int, list[int]] = None
    end_time_delta: Sequence[Any] = None
    device: str = "cpu"
    coincidence_threshold: float = 0
    strike_pad: str = None
    stillsuit_pad: str = None
    is_online: bool = False

    def __post_init__(self):

        assert isinstance(self.stillsuit_pad, str)
        self.source_pad_names = (self.stillsuit_pad,)
        if self.strike_pad is not None:
            self.source_pad_names += (self.strike_pad,)
        self.trigger_finding_samples = self.trigger_finding_duration * self.sample_rate
        assert self.trigger_finding_samples == int(
            self.trigger_finding_samples
        ), "trigger_finding_duration must map to integer number of sample points"
        self.trigger_finding_samples = int(self.trigger_finding_samples)
        self.ifos = list(self.autocorrelation_banks.keys())
        self.nifo = len(self.ifos)

        (
            self.nsubbank,
            self.ntempmax,
            self.autocorrelation_length,
        ) = self.autocorrelation_banks[self.ifos[0]].shape
        self.autocorrelation_banks_real = {}
        self.autocorrelation_banks_imag = {}
        self.ifos_number_map = {ifo: i + 1 for i, ifo in enumerate(self.ifos)}
        for ifo in self.ifos:
            self.autocorrelation_banks_real[ifo] = self.autocorrelation_banks[ifo].real
            self.autocorrelation_banks_imag[ifo] = self.autocorrelation_banks[ifo].imag

        if len(self.ifos) > 1:
            combs = list(combinations(self.ifos, 2))
            max_light_travel_time = max(light_travel_time(*c) for c in combs)
        else:
            max_light_travel_time = 0
        self.trigger_finding_overlap_samples = (
            int((max_light_travel_time + self.coincidence_threshold) * self.sample_rate)
            // 2
        )
        self.padding = self.autocorrelation_length // 2
        self.adapter_config = AdapterConfig(
            # stride=Offset.fromsec(self.trigger_finding_duration),
            overlap=(
                Offset.fromsamples(
                    self.padding + self.trigger_finding_overlap_samples,
                    self.sample_rate,
                ),
                Offset.fromsamples(
                    self.padding + self.trigger_finding_overlap_samples,
                    self.sample_rate,
                ),
            ),
            backend=TorchBackend,
        )
        self.template_ids = self.template_ids.to(self.device)
        self.template_ids_np = self.template_ids.to("cpu").numpy()
        self.end_time_delta = self.end_time_delta.numpy()

        # Denominator Eq 28 from arXiv:1604.04324
        # self.autocorrelation_norms = torch.sum(
        #    2 - 2 * abs(self.autocorrelation_banks) ** 2.0, dim=-1
        # )
        # FIXME: Dropping the factor of 2 in front of abs to match the norm in
        #        gstlal_autocorrelation_chi2.c

        self.autocorrelation_norms = {}
        for ifo in self.ifos:
            self.autocorrelation_norms[ifo] = torch.sum(
                2 - abs(self.autocorrelation_banks[ifo]) ** 2, dim=-1
            )

        self.snr_time_series_indices = torch.arange(
            self.autocorrelation_length, device=self.device
        ).expand(self.nsubbank, self.ntempmax, -1)

        super().__post_init__()

        self.output_frames = {pad: None for pad in self.source_pad_names}

        self.reverse_bankids_map = {
            i: bankid for bankid, ids in self.bankids_map.items() for i in ids
        }

    def find_peaks_and_calculate_chisqs(self, snr_ts: Array) -> Dict[str, list[Array]]:
        """Find snr peaks in a given snr time series window, and obtain peak time,
        phase, and chisq

        Args:
            snr_ts:
                Dict[str, Array], a dictionary of Arrays, with ifos as keys, only
                contains snr time series for ifos with nongap data

        Returns:
            Dict[str, list[Array]], a dictionary of trigger data, with ifos as keys,
            and a list of trigger data with the contents [peak_locations, peaks,
            autocorrelation_chisq]
        """

        padding = self.padding
        idi = padding
        # idf = (
        #    padding
        #    + self.trigger_finding_samples
        #    + self.trigger_finding_overlap_samples * 2
        # )
        idf = -padding
        triggers = {
            "peak_locations": {},
            "snrs": {},
            "chisqs": {},
            "snr_ts_snippet": {},
        }
        for ifo, snr in snr_ts.items():
            shape = snr.shape
            snr = snr.view(shape[0], shape[1] // 2, 2, shape[2])
            real = snr[..., 0, :]
            imag = snr[..., 1, :]
            peaks, peak_locations = torch.max(
                (real[..., idi:idf] ** 2 + imag[..., idi:idf] ** 2), dim=-1
            )
            peaks **= 0.5
            peak_locations += idi
            time_series_indices = self.snr_time_series_indices + (
                peak_locations - self.padding
            ).unsqueeze(2)
            real_imag_time_series = snr.gather(
                3,
                time_series_indices.unsqueeze(2).expand(
                    shape[0], shape[1] // 2, 2, self.autocorrelation_length
                ),
            )
            real_time_series = real_imag_time_series[..., 0, :]
            imag_time_series = real_imag_time_series[..., 1, :]
            snr_ts_shape = real_time_series.shape

            real_peak = real_time_series[..., padding].unsqueeze(2).expand(snr_ts_shape)
            imag_peak = imag_time_series[..., padding].unsqueeze(2).expand(snr_ts_shape)

            # complex operations are slow with torch compile, make them real
            autocorrelation_chisq = torch.sum(
                (
                    real_time_series
                    - real_peak * self.autocorrelation_banks_real[ifo]
                    + imag_peak * self.autocorrelation_banks_imag[ifo]
                )
                ** 2
                + (
                    imag_time_series
                    - real_peak * self.autocorrelation_banks_imag[ifo]
                    - imag_peak * self.autocorrelation_banks_real[ifo]
                )
                ** 2,
                dim=-1,
            )
            autocorrelation_chisq /= self.autocorrelation_norms[ifo]
            triggers["peak_locations"][ifo] = peak_locations
            triggers["snrs"][ifo] = peaks
            triggers["chisqs"][ifo] = autocorrelation_chisq
            triggers["snr_ts_snippet"][ifo] = real_imag_time_series

        return triggers

    def make_coincs(self, triggers):
        on_ifos = list(triggers["snrs"].keys())
        nifo = len(on_ifos)
        single_background_masks = {}  # for snr chisq histogram

        if nifo == 1:
            # return the single ifo snrs
            snr1 = list(triggers["snrs"].values())[0]
            snr_above_min_mask = snr1 >= self.snr_min
            all_network_snr = snr1 * snr_above_min_mask
            subthresh_mask = snr1 < self.snr_min
            ifo_combs = (
                torch.ones_like(all_network_snr, dtype=torch.int)
                * snr_above_min_mask
                * self.ifos_number_map[on_ifos[0]]
            )

        elif nifo == 2:
            times = list(triggers["peak_locations"].values())
            snrs = list(triggers["snrs"].values())
            (
                coinc2_mask,
                single_mask1,
                single_mask2,
                snr1_above_min_mask,
                snr2_above_min_mask,
                all_network_snr,
                subthresh_mask,
            ) = self.coinc2(snrs, times, on_ifos)

            # convert ifo combination masks to numbers
            ifo_numbers = [self.ifos_number_map[ifo] for ifo in on_ifos]
            ifo_combs = (
                coinc2_mask * (ifo_numbers[0] * 10 + ifo_numbers[1])
                + single_mask1 * ifo_numbers[0]
                + single_mask2 * ifo_numbers[1]
            )

            single_background_mask1 = ~coinc2_mask & snr1_above_min_mask
            single_background_mask2 = ~coinc2_mask & snr2_above_min_mask

            smasks = [single_background_mask1, single_background_mask2]
            for i, ifo in enumerate(on_ifos):
                single_background_masks[ifo] = smasks[i]

        elif nifo == 3:
            (
                coinc3_mask,
                coinc2_mask12,
                coinc2_mask23,
                coinc2_mask31,
                single_mask1,
                single_mask2,
                single_mask3,
                single_background_mask1,
                single_background_mask2,
                single_background_mask3,
                all_network_snr,
                subthresh_mask,
            ) = self.coinc3(triggers)

            # convert ifo combination masks to numbers
            ifo_numbers = list(self.ifos_number_map.values())

            ifo_combs = (
                coinc3_mask
                * (ifo_numbers[0] * 100 + ifo_numbers[1] * 10 + ifo_numbers[2])
                + coinc2_mask12 * (ifo_numbers[0] * 10 + ifo_numbers[1])
                + coinc2_mask23 * (ifo_numbers[1] * 10 + ifo_numbers[2])
                + coinc2_mask31 * (ifo_numbers[0] * 10 + ifo_numbers[2])
                + single_mask1 * ifo_numbers[0]
                + single_mask2 * ifo_numbers[1]
                + single_mask3 * ifo_numbers[2]
            )

            smasks = [
                single_background_mask1,
                single_background_mask2,
                single_background_mask3,
            ]
            for i, ifo in enumerate(on_ifos):
                single_background_masks[ifo] = smasks[i]
        else:
            raise ValueError("nifo > 3 is not implemented")

        return ifo_combs, all_network_snr, single_background_masks, subthresh_mask

    def coinc3(self, triggers):
        ifos = list(triggers["snrs"].keys())
        times = list(triggers["peak_locations"].values())
        snrs = list(triggers["snrs"].values())

        snr1 = snrs[0]
        snr2 = snrs[1]
        snr3 = snrs[2]

        # all combinations
        coinc2_mask12, _, _, _, _, _, _ = self.coinc2(
            [snr1, snr2], [times[0], times[1]], [ifos[0], ifos[1]]
        )
        coinc2_mask23, _, _, _, _, _, _ = self.coinc2(
            [snr2, snr3], [times[1], times[2]], [ifos[1], ifos[2]]
        )
        coinc2_mask31, _, _, _, _, _, _ = self.coinc2(
            [snr1, snr3], [times[0], times[2]], [ifos[0], ifos[2]]
        )

        # 3 ifo coincs
        coinc3_mask = coinc2_mask12 & coinc2_mask23 & coinc2_mask31
        network_snr123 = (
            (snr1 * coinc3_mask) ** 2
            + (snr2 * coinc3_mask) ** 2
            + (snr3 * coinc3_mask) ** 2
        ) ** 0.5

        # 2 ifo coincs
        # update coinc masks: filter out 3 ifo coincs
        coinc2_mask12 = coinc2_mask12 & ~coinc3_mask
        coinc2_mask23 = coinc2_mask23 & ~coinc3_mask
        coinc2_mask31 = coinc2_mask31 & ~coinc3_mask

        network_snr12 = (
            (snr1 * coinc2_mask12) ** 2 + (snr2 * coinc2_mask12) ** 2
        ) ** 0.5
        network_snr23 = (
            (snr2 * coinc2_mask23) ** 2 + (snr3 * coinc2_mask23) ** 2
        ) ** 0.5
        network_snr31 = (
            (snr1 * coinc2_mask31) ** 2 + (snr3 * coinc2_mask31) ** 2
        ) ** 0.5

        # update coinc masks: there may be cases where a template has
        # two coincs, (e.g., HV coinc and LV coinc, but not HL coinc),
        # in this case, compare HV, LV coinc network snrs and choose
        # the larger one
        # FIXME: what to do when snrs are equal?
        coinc2_mask12 = (
            coinc2_mask12
            & (network_snr12 > network_snr23)
            & (network_snr12 >= network_snr31)
        )
        coinc2_mask23 = (
            coinc2_mask23
            & (network_snr23 >= network_snr12)
            & (network_snr23 > network_snr31)
        )
        coinc2_mask31 = (
            coinc2_mask31
            & (network_snr31 > network_snr12)
            & (network_snr31 >= network_snr23)
        )

        # update 2 ifo network snrs
        network_snr12 = (
            (snr1 * coinc2_mask12) ** 2 + (snr2 * coinc2_mask12) ** 2
        ) ** 0.5
        network_snr23 = (
            (snr2 * coinc2_mask23) ** 2 + (snr3 * coinc2_mask23) ** 2
        ) ** 0.5
        network_snr31 = (
            (snr1 * coinc2_mask31) ** 2 + (snr3 * coinc2_mask31) ** 2
        ) ** 0.5

        # 1 ifo
        # FIXME: what to do when snrs are equal?
        single_mask1 = (
            ~coinc3_mask
            & ~coinc2_mask12
            & ~coinc2_mask23
            & ~coinc2_mask31
            & (snr1 > snr2)
            & (snr1 >= snr3)
            & (snr1 >= self.snr_min)
        )
        single_mask2 = (
            ~coinc3_mask
            & ~coinc2_mask12
            & ~coinc2_mask23
            & ~coinc2_mask31
            & (snr2 >= snr1)
            & (snr2 > snr3)
            & (snr2 >= self.snr_min)
        )
        single_mask3 = (
            ~coinc3_mask
            & ~coinc2_mask12
            & ~coinc2_mask23
            & ~coinc2_mask31
            & (snr3 > snr1)
            & (snr3 >= snr2)
            & (snr3 >= self.snr_min)
        )

        single_snr1 = snr1 * single_mask1
        single_snr2 = snr2 * single_mask2
        single_snr3 = snr3 * single_mask3

        all_network_snrs = (
            network_snr123
            + network_snr12
            + network_snr23
            + network_snr31
            + single_snr1
            + single_snr2
            + single_snr3
        )

        subthresh_mask = (
            (snr1 < self.snr_min) & (snr2 < self.snr_min) & (snr3 < self.snr_min)
        )

        single_background_mask1 = (
            ~coinc3_mask & ~coinc2_mask12 & ~coinc2_mask31 & (snr1 >= self.snr_min)
        )
        single_background_mask2 = (
            ~coinc3_mask & ~coinc2_mask12 & ~coinc2_mask23 & (snr2 >= self.snr_min)
        )
        single_background_mask3 = (
            ~coinc3_mask & ~coinc2_mask23 & ~coinc2_mask31 & (snr3 >= self.snr_min)
        )

        return (
            coinc3_mask,
            coinc2_mask12,
            coinc2_mask23,
            coinc2_mask31,
            single_mask1,
            single_mask2,
            single_mask3,
            single_background_mask1,
            single_background_mask2,
            single_background_mask3,
            all_network_snrs,
            subthresh_mask,
        )

    def coinc2(self, snrs, times, ifos):
        dt = (light_travel_time(*ifos) + self.coincidence_threshold) * self.rate
        snr1 = snrs[0]
        snr2 = snrs[1]
        time1 = times[0]
        time2 = times[1]
        snr1_above_min_mask = snr1 >= self.snr_min
        snr2_above_min_mask = snr2 >= self.snr_min
        coinc_mask = (
            (abs(time1 - time2) < dt) & snr1_above_min_mask & snr2_above_min_mask
        )
        single_mask1 = (snr1 > snr2) & ~coinc_mask & snr1_above_min_mask
        single_mask2 = (snr1 <= snr2) & ~coinc_mask & snr2_above_min_mask

        snr_masked1 = snr1 * coinc_mask
        snr_masked2 = snr2 * coinc_mask
        coinc_network_snr = (snr_masked1**2 + snr_masked2**2) ** 0.5

        single1 = snr1 * single_mask1
        single2 = snr2 * single_mask2

        all_network_snr = coinc_network_snr + single1 + single2

        subthresh_mask = (snr1 < self.snr_min) & (snr2 < self.snr_min)

        return (
            coinc_mask,
            single_mask1,
            single_mask2,
            snr1_above_min_mask,
            snr2_above_min_mask,
            all_network_snr,
            subthresh_mask,
        )

    def cluster_coincs(
        self, ifo_combs, all_network_snr, template_ids, triggers, snr_ts, subthresh_mask
    ):
        clustered_snr, max_locations = torch.max(all_network_snr, dim=-1)

        mask = ~subthresh_mask.to("cpu").numpy()[range(self.nsubbank), max_locations]
        clustered_ifo_combs = ifo_combs.gather(1, max_locations.unsqueeze(1)).squeeze(
            -1
        )
        max_locations = max_locations.to("cpu").numpy()
        clustered_template_ids = template_ids[range(self.nsubbank), max_locations]
        clustered_bankids = []
        sngls = {}
        for i, m in enumerate(mask):
            m = m.item()
            if m is True:
                clustered_bankids.append(self.reverse_bankids_map[i])
        trig_peak_locations = triggers["peak_locations"]
        trig_snrs = triggers["snrs"]
        trig_chisqs = triggers["chisqs"]
        trig_snr_ts_snippet = triggers["snr_ts_snippet"]
        snr_ts_snippet_clustered = {}
        snr_ts_clustered = {}
        for ifo in trig_snrs.keys():
            sngls[ifo] = {}
            max_peak_locations = trig_peak_locations[ifo][
                range(self.nsubbank), max_locations
            ]
            sngl_snr = trig_snrs[ifo][range(self.nsubbank), max_locations]
            sngl_chisq = trig_chisqs[ifo][range(self.nsubbank), max_locations]

            sngls[ifo]["time"] = (
                np.round(
                    (Offset.fromsamples(max_peak_locations, self.rate) + self.offset)
                    / Offset.MAX_RATE
                    * 1_000_000_000
                ).astype(int)
                + Offset.offset_ref_t0
                + self.end_time_delta
            )[mask]
            sngls[ifo]["snr"] = sngl_snr[mask]
            sngls[ifo]["chisq"] = sngl_chisq[mask]

            # go back and find the phase only for the clustered coincs
            # FIXME: find the snr snippet
            snrs0 = snr_ts[ifo]
            snrs1 = snrs0.view(snrs0.shape[0], snrs0.shape[1] // 2, 2, snrs0.shape[2])
            snr_pairs = snrs1[range(snrs1.shape[0]), max_locations]
            sngl_peaks = snr_pairs[range(snr_pairs.shape[0]), :, max_peak_locations]
            real = sngl_peaks[:, 0]
            imag = sngl_peaks[:, 1]
            phase = torch.atan2(imag, real)
            sngls[ifo]["phase"] = phase.to("cpu").numpy()[mask]

            if self.is_online:
                # get snr snippet around the peak for the clustered coincs
                # only for online case
                snr_ts_snippet_clustered[ifo] = (
                    trig_snr_ts_snippet[ifo][range(snrs1.shape[0]), max_locations]
                    .to("cpu")
                    .numpy()[mask]
                )

                snr_ts_clustered[ifo] = (
                    snr_ts[ifo]
                    .view(snrs0.shape[0], snrs0.shape[1] // 2, 2, snrs0.shape[2])[
                        range(snrs1.shape[0]), max_locations
                    ]
                    .to("cpu")
                    .numpy()[mask]
                )
            else:
                snr_ts_snippet_clustered[ifo] = None
                snr_ts_clustered[ifo] = None

        # FIXME: is stacking then index_select faster?
        # FIXME: is stacking then copying to cpu faster?
        return {
            "clustered_bankids": clustered_bankids,
            "clustered_template_ids": clustered_template_ids[mask],
            "clustered_ifo_combs": clustered_ifo_combs[mask].to("cpu").numpy(),
            "clustered_snr": clustered_snr[mask].to("cpu").numpy(),
            "sngls": sngls,
            "snr_ts_snippet_clustered": snr_ts_snippet_clustered,
            "snr_ts_clustered": snr_ts_clustered,
        }

    # @torch.compile
    def itacacac(self, snr_ts):
        triggers = self.find_peaks_and_calculate_chisqs(snr_ts)

        ifo_combs, all_network_snr, single_background_masks, subthresh_mask = (
            self.make_coincs(triggers)
        )

        self.max_snr_histories = {}
        if self.is_online:
            for ifo, snr in triggers["snrs"].items():
                maxsnr_id = np.unravel_index(
                    torch.argmax(snr).to("cpu").numpy(), snr.shape
                )
                max_snr = float(snr[maxsnr_id])
                if max_snr >= self.snr_min:
                    time = triggers["peak_locations"][ifo][maxsnr_id].to("cpu").numpy()
                    time = (
                        np.round(
                            (Offset.fromsamples(time, self.rate) + self.offset)
                            / Offset.MAX_RATE
                            * 1_000_000_000
                        ).astype(int)
                        + Offset.offset_ref_t0
                        + self.end_time_delta[maxsnr_id[0]]
                    ) / 1_000_000_000
                    self.max_snr_histories[ifo] = {"time": time, "snr": max_snr}

        # FIXME: this part and clustered_coinc is lowering the GPU utilization
        for trig_type in triggers.keys():
            if trig_type != "snr_ts_snippet":
                for k, v in triggers[trig_type].items():
                    triggers[trig_type][k] = v.to("cpu").numpy()

        if False not in subthresh_mask:
            clustered_coinc = {}
        else:
            clustered_coinc = self.cluster_coincs(
                ifo_combs,
                all_network_snr,
                self.template_ids_np,
                triggers,
                snr_ts,
                subthresh_mask,
            )

        return (
            triggers,
            ifo_combs,
            all_network_snr,
            single_background_masks,
            clustered_coinc,
        )

    def output_background(self, triggers, single_background_masks, ts, te):
        # Populate background snr, chisq, time for each bank, ifo
        # FIXME: is stacking then copying to cpu faster?
        # FIXME: do we only need snr chisq for singles?
        trig_peak_locations = triggers["peak_locations"]
        trig_snrs = triggers["snrs"]
        trig_chisqs = triggers["chisqs"]
        ifos = trig_snrs.keys()

        background = {
            bankid: {ifo: None}
            for bankid, ids in self.bankids_map.items()
            for ifo in ifos
        }
        # loop over banks
        for bankid, ids in self.bankids_map.items():
            # loop over ifos
            for ifo in ifos:
                bg_times = []
                bg_snrs = []
                bg_chisqs = []
                bg_template_ids = []

                if ifo in single_background_masks:
                    if True in single_background_masks[ifo]:
                        smask0 = single_background_masks[ifo].to("cpu").numpy()
                        # loop over subbank ids in this bank
                        for i in ids:
                            smask = smask0[i]
                            bg_time = trig_peak_locations[ifo][i][smask]
                            bg_time = (
                                np.round(
                                    (
                                        Offset.fromsamples(bg_time, self.rate)
                                        + self.offset
                                    )
                                    / Offset.MAX_RATE
                                    * 1_000_000_000
                                ).astype(int)
                                + Offset.offset_ref_t0
                                + self.end_time_delta[i]
                            )
                            bg_times.append(bg_time)
                            bg_snrs.append(trig_snrs[ifo][i][smask])
                            bg_chisqs.append(trig_chisqs[ifo][i][smask])
                            bg_template_ids.append(self.template_ids_np[i][smask])

                background[bankid][ifo] = {
                    "time": bg_times,
                    "snrs": bg_snrs,
                    "chisqs": bg_chisqs,
                    "template_ids": bg_template_ids,
                }

        # FIXME: check buf seg definition
        trigger_rates = {ifo: {} for ifo in ifos}
        for ifo, snr in trig_snrs.items():
            for bankid, ids in self.bankids_map.items():
                trigger_rates[ifo][bankid] = (
                    segments.segment(
                        (ts + min(self.end_time_delta[ids])) / 1_000_000_000,
                        te / 1_000_000_000 + 0.000000001,
                    ),
                    np.sum(snr[ids] >= self.snr_min).item(),
                )

        return {
            "background": EventBuffer(ts, te, data=background),
            "trigger_rates": EventBuffer(ts, te, data=trigger_rates),
        }

    def output_events(self, clustered_coinc, ts, te):
        #
        # Construct event buffers
        #
        out_triggers = []
        out_snr_ts = []
        sngls = clustered_coinc["sngls"]
        # Zero-out the non-coinc ifos
        for j, c in enumerate(clustered_coinc["clustered_ifo_combs"]):
            trigs_this_event = []
            snr_ts_this_event = {}
            for ifo in sngls.keys():
                sngl = sngls[ifo]
                ifo_num = self.ifos_number_map[ifo]
                if str(ifo_num) in str(c):
                    trig = {
                        col: sngl[col][j].item()
                        for col in ["time", "snr", "chisq", "phase"]
                    }
                    trig["_filter_id"] = clustered_coinc["clustered_template_ids"][j]
                    trig["ifo"] = ifo
                    trig["epoch_start"] = ts
                    trig["epoch_end"] = te
                    trigs_this_event.append(trig)

                    if self.is_online:
                        # Prepare the snr time series snippet
                        # snr time series for subthreshold ifos have length of the
                        # autocorrelation length
                        snr_ts_snippet = clustered_coinc["snr_ts_snippet_clustered"][
                            ifo
                        ][j]
                        half_autocorr_length = (snr_ts_snippet.shape[-1] - 1) // 2
                        snr_ts_snippet_out = lal.CreateCOMPLEX8TimeSeries(
                            name="snr",
                            epoch=trig["time"] / 1_000_000_000
                            - half_autocorr_length / self.sample_rate,
                            f0=0.0,
                            deltaT=1 / self.sample_rate,
                            sampleUnits=lal.DimensionlessUnit,
                            length=snr_ts_snippet.shape[-1],
                        )
                        snr_ts_snippet_out.data.data = (
                            snr_ts_snippet[0] + 1j * snr_ts_snippet[1]
                        )

                        snr_ts_this_event[ifo] = snr_ts_snippet_out
                    else:
                        snr_ts_this_event[ifo] = None
                else:
                    trigs_this_event.append(None)
                    if self.is_online:
                        # Get the subthreshold snr time series
                        # snr time series for subthreshold ifos have length of trigger
                        # finding window. This will be used for subthreshold trigger
                        # finding in the GraceDBSink
                        snr_ts_snippet = clustered_coinc["snr_ts_clustered"][ifo][j]
                        assert snr_ts_snippet.shape[-1] > 0, f"{ifo}"
                        snr_ts_snippet_out = lal.CreateCOMPLEX8TimeSeries(
                            name="snr",
                            epoch=Offset.tosec(self.offset),
                            f0=0.0,
                            deltaT=1 / self.sample_rate,
                            sampleUnits=lal.DimensionlessUnit,
                            length=snr_ts_snippet.shape[-1],
                        )
                        snr_ts_snippet_out.data.data = (
                            snr_ts_snippet[0] + 1j * snr_ts_snippet[1]
                        )
                        snr_ts_this_event[ifo] = snr_ts_snippet_out
                    else:
                        snr_ts_this_event[ifo] = None

            out_triggers.append(trigs_this_event)
            out_snr_ts.append(snr_ts_this_event)

        out_events = [
            {
                "time": list(dict(sorted(clustered_coinc["sngls"].items())).values())[
                    0
                ]["time"][j].item(),
                "network_snr": clustered_coinc["clustered_snr"][j].item(),
                "bankid": clustered_coinc["clustered_bankids"][j],
            }
            for j in range(clustered_coinc["clustered_ifo_combs"].shape[0])
        ]

        # Put in chisq weighted snr
        for event, trigger in zip(out_events, out_triggers):
            network_chisq_weighted_snr2 = 0
            for trig in trigger:
                if trig is not None:
                    chisq_weighted_snr = trig["snr"] / (
                        (1 + max(1.0, trig["chisq"]) ** 3) / 2.0
                    ) ** (1.0 / 5.0)
                    trig["chisq_weighted_snr"] = chisq_weighted_snr
                    network_chisq_weighted_snr2 += chisq_weighted_snr**2
            event["network_chisq_weighted_snr"] = network_chisq_weighted_snr2**0.5

        if len(out_triggers) == 0:
            print("out events", out_events)

        return {
            "event": EventBuffer(ts, te, data=out_events),
            "trigger": EventBuffer(ts, te, data=out_triggers),
            "snr_ts": EventBuffer(ts, te, data=out_snr_ts),
            "max_snr_histories": EventBuffer(ts, te, data=self.max_snr_histories),
        }

    def internal(self):
        super().internal()

        frames = self.preparedframes
        self.preparedframes = {}

        snr_ts = {}

        for sink_pad in self.sink_pads:
            # FIXME: consider multiple buffers
            frame = frames[sink_pad]
            assert len(frame.buffers) == 1
            buf = frame.buffers[0]
            if not buf.is_gap:
                snr_ts[self.rsnks[sink_pad]] = buf.data
        self.rate = frame.sample_rate
        self.offset = frame.offset

        offset0 = self.preparedoutoffsets[self.sink_pads[0]][0]["offset"]
        ts = Offset.tons(offset0) - int(
            self.trigger_finding_overlap_samples / self.sample_rate * 1e9
        )
        te = Offset.tons(
            offset0 + self.preparedoutoffsets[self.sink_pads[0]][0]["noffset"]
        ) + int(self.trigger_finding_overlap_samples / self.sample_rate * 1e9)

        if len(snr_ts.keys()) == 0:
            events = {
                "event": EventBuffer(ts, te, data=None),
                "trigger": EventBuffer(ts, te, data=None),
                "snr_ts": EventBuffer(ts, te, data=None),
                "max_snr_histories": EventBuffer(ts, te, data=None),
            }
            if self.strike_pad is not None:
                background_events = {
                    "background": EventBuffer(ts, te, data=None),
                    "trigger_rates": EventBuffer(ts, te, data=None),
                }
        else:
            (
                triggers,
                ifo_combs,
                all_network_snr,
                single_background_masks,
                clustered_coinc,
            ) = self.itacacac(snr_ts)
            if len(clustered_coinc) == 0:
                # There are no coincs
                events = {
                    "event": EventBuffer(ts, te, data=None),
                    "trigger": EventBuffer(ts, te, data=None),
                    "snr_ts": EventBuffer(ts, te, data=None),
                    "max_snr_histories": EventBuffer(
                        ts, te, data=self.max_snr_histories
                    ),
                }
            else:
                events = self.output_events(clustered_coinc, ts, te)

            if self.strike_pad is not None:
                background_events = self.output_background(
                    triggers, single_background_masks, ts, te
                )

        self.output_frames[self.stillsuit_pad] = EventFrame(
            events=events, EOS=frame.EOS
        )
        if self.strike_pad is not None:
            self.output_frames[self.strike_pad] = EventFrame(
                events=background_events, EOS=frame.EOS
            )

    def new(self, pad):
        return self.output_frames[self.rsrcs[pad]]
