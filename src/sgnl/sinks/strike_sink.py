from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sgn.sinks import SinkElement
from strike.stats import likelihood_ratio


@dataclass
class event_dummy(object):
    ifo: list[str]
    end: float
    snr: float
    chisq: float
    combochisq: float
    template_id: int


@dataclass
class StrikeSink(SinkElement):
    ifos: list[str] = None
    all_template_ids: Sequence[Any] = None
    bankids_map: dict[str, list] = None
    ranking_stat_output: list[str] = None
    background_pad: str = None
    horizon_pads: list[str] = None

    def __post_init__(self):
        assert isinstance(self.ifos, list)
        assert isinstance(self.background_pad, str)
        assert isinstance(self.horizon_pads, list)
        self.sink_pad_names = (self.background_pad,) + tuple(self.horizon_pads)
        super().__post_init__()
        self.ranking_stats = {}
        for bankid, ids in self.bankids_map.items():
            bank_template_ids = self.all_template_ids[ids]
            bank_template_ids = tuple(bank_template_ids[bank_template_ids != -1])
            # Ranking stat output
            self.ranking_stats[bankid] = likelihood_ratio.LnLikelihoodRatio(
                template_ids=bank_template_ids,
                instruments=self.ifos,
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

        if self.rsnks[pad] in self.horizon_pads:
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
                    ][horizon_time] = horizon

    def internal(self, pad):
        if self.at_eos:
            for i, bankid in enumerate(self.bankids_map):
                # FIXME correct file name assignment
                # write ranking stats file
                self.ranking_stats[bankid].save(self.ranking_stat_output[i])
