"""A sink element to write out triggers to likelihood ratio class in strike."""

# Copyright (C) 2024 Yun-Jing Huang, Prathamesh Joshi, Leo Tsukada

import io
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from strike.stats import likelihood_ratio
from strike.stats.far import RankingStatPDF

from sgnl.control import SnapShotControlSinkElement


@dataclass
class event_dummy(object):
    ifo: list[str]
    end: float
    snr: float
    chisq: float
    combochisq: float
    template_id: int


def xml_string(rstat):
    f = io.BytesIO()
    rstat.save_fileobj(f)
    f.seek(0)
    return f.read().decode("utf-8")


@dataclass
class StrikeSink(SnapShotControlSinkElement):
    ifos: list[str] = None
    all_template_ids: Sequence[Any] = None
    bankids_map: dict[str, list] = None
    ranking_stat_output: dict[str, str] = None
    coincidence_threshold: float = None
    background_pad: str = None
    horizon_pads: list[str] = None
    zerolag_pad: str = None

    def __post_init__(self):
        assert isinstance(self.ifos, list)
        assert isinstance(self.background_pad, str)
        assert isinstance(self.horizon_pads, list)
        self.sink_pad_names = (
            (self.background_pad,) + tuple(self.horizon_pads) + (self.zerolag_pad,)
        )
        super().__post_init__()
        self.ranking_stats = {}
        if self.zerolag_pad is not None:
            self.zerolagpdfs = {}
        self.state_dict = {"xml": {}, "zerolagxml": {}}
        for bankid, ids in self.bankids_map.items():
            four_digit_id = "%04d" % int(bankid)
            bank_template_ids = self.all_template_ids[ids]
            bank_template_ids = tuple(bank_template_ids[bank_template_ids != -1])
            # Ranking stat output
            rstat = likelihood_ratio.LnLikelihoodRatio(
                template_ids=bank_template_ids,
                instruments=self.ifos,
                delta_t=self.coincidence_threshold,
            )
            self.ranking_stats[bankid] = rstat
            self.add_snapshot_filename(
                "%s_SGNL_INSPIRAL_DIST_STATS" % four_digit_id, "xml.gz"
            )
            self.state_dict["xml"][four_digit_id] = xml_string(
                self.ranking_stats[bankid]
            )

            if self.zerolag_pad is not None:
                # FIXME: add option to read in existing files
                self.zerolagpdfs[bankid] = RankingStatPDF(rstat, nsamples=0)
                self.state_dict["zerolagxml"][four_digit_id] = xml_string(
                    self.zerolagpdfs[bankid]
                )

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

        if self.rsnks[pad] == self.background_pad:
            background = frame["background"].data
            if background is None:
                return
            #
            # Background triggers
            #
            # form events
            for ifo in self.ifos:
                for bankid in self.bankids_map:
                    if ifo in background[bankid]:
                        trigs = background[bankid][ifo]
                        bgtime = trigs["time"]
                        snr = trigs["snrs"]
                        chisq = trigs["chisqs"]
                        template_id = trigs["template_ids"]

                        bg_events = []
                        # loop over subbanks
                        for time0, snr0, chisq0, templateid0 in zip(
                            bgtime, snr, chisq, template_id
                        ):
                            # loop over triggers in subbanks, and send them to
                            # train_noise in a burst
                            for t, s, c, tid in zip(time0, snr0, chisq0, templateid0):
                                # FIXME: is end time in seconds??
                                bg_event = event_dummy(
                                    ifo=ifo,
                                    end=t / 1_000_000_000,
                                    snr=s,
                                    chisq=c,
                                    combochisq=c,
                                    template_id=tid,
                                )
                                bg_events.append(bg_event)
                        self.ranking_stats[bankid].train_noise(bg_events)
            #
            # Trigger rates
            #
            # FIXME : come up with a way to make populating the trigger rate
            # object as part of train_noise
            trigger_rates = frame["trigger_rates"].data
            for ifo, trigger_rate in trigger_rates.items():
                for bankid in self.bankids_map:
                    buf_seg, count = trigger_rate[bankid]
                    self.ranking_stats[bankid].terms["P_of_tref_Dh"].triggerrates[
                        ifo
                    ].add_ratebin(list(buf_seg), count)

        elif self.rsnks[pad] in self.horizon_pads:
            if frame["data"].data is not None:  # happens for the first few buffers
                ifo = frame["data"].data["ifo"]
                horizon = frame["data"].data["horizon"]
                horizon_time = (
                    frame["data"].data["time"] / 1_000_000_000
                )  # NOTE: This is the start time. Do we want the end time?
                # FIXME: We set the same horizon history for every svd bin
                # since the horizon distance calculation in
                # sgnligo/transforms/condition.py
                # uses the same template for every svd bin
                # When that is changed, this will need to be made
                # bin dependent as well, which would require
                # bankid info to be piped along with the horizon distance
                for bankid in self.bankids_map:
                    # self.ranking_stats[bankid].horizon_history[ifo][horizon_time]
                    # = horizon
                    self.ranking_stats[bankid].terms["P_of_tref_Dh"].horizon_history[
                        ifo
                    ][horizon_time] = horizon[bankid]

        elif self.zerolag_pad is not None and self.rsnks[pad] == self.zerolag_pad:
            zerolags = frame["zerolag"].data
            for z in zerolags:
                if "likelihood" in z:
                    self.zerolagpdfs[z["bankid"]].zero_lag_lr_lnpdf.count[
                        z["likelihood"],
                    ] += 1

    def internal(self):
        SnapShotControlSinkElement.exchange_state(self.name, self.state_dict)
        if self.at_eos:
            for bankid in self.bankids_map:
                # FIXME correct file name assignment
                # write ranking stats file
                self.ranking_stats[bankid].save(self.ranking_stat_output[bankid])
        if self.snapshot_ready():
            for bankid, fn in zip(self.bankids_map, self.snapshot_filenames()):
                four_digit_id = "%04d" % int(bankid)
                self.state_dict["xml"][four_digit_id] = xml_string(
                    self.ranking_stats[bankid]
                )
                self.ranking_stats[bankid].save(fn)
                self.state_dict["zerolagxml"][four_digit_id] = xml_string(
                    self.zerolagpdfs[bankid]
                )
                self.zerolagpdfs[bankid].save(
                    fn.replace("DIST_STATS", "ZEROLAG_RANK_STAT_PDFS")
                )
