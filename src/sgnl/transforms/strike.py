"""A transform element to assign likelihood and FAR to events"""

# Copyright (C) 2024-2025 Yun-Jing Huang

from dataclasses import dataclass

import math
from sgn.base import TransformElement
from sgnts.base import EventBuffer, EventFrame

from sgnl.strike_object import StrikeObject


@dataclass
class StrikeTransform(TransformElement):
    """
    Compute LR and FAR
    """

    event_pad: str = None
    kafka_pad: str = None
    strike_object: StrikeObject = None

    def __post_init__(self):
        self.source_pad_names = (self.event_pad, self.kafka_pad)
        assert len(self.sink_pad_names) == 1
        assert self.strike_object is not None
        super().__post_init__()
        self.frame = None
        self.output_frames = {p: None for p in self.source_pad_names}

    def pull(self, pad, frame):
        self.frame = frame

    def internal(self):
        """
        compute LR and FAR
        """
        frame = self.frame
        events = self.frame.events
        event_data = events["event"].data
        ts = events["event"].ts
        te = events["event"].te
        trigger_data = events["trigger"].data

        if event_data is not None:
            for e, t in zip(event_data, trigger_data):
                #
                # Assign likelihoods and FARs!
                #
                bankid = e["bankid"]
                if self.strike_object.ln_lr_from_triggers[bankid] is not None:
                    # update triggers
                    trigs = []
                    for i, ti in enumerate(t):
                        if ti is not None:
                            # FIXME the time conversion should happen in the
                            # ln_lr_from_triggers function?
                            # find a way to avoid making a copy
                            copytrig = {k: v for k, v in ti.items()}
                            copytrig["__trigger_id"] = i
                            copytrig["time"] /= 1e9
                            copytrig["epoch_start"] /= 1e9
                            copytrig["epoch_end"] /= 1e9
                            trigs.append(copytrig)

                    e["likelihood"] = self.strike_object.ln_lr_from_triggers[bankid](
                        trigs, self.strike_object.offset_vectors
                    )

                    if self.strike_object.fapfar is not None:
                        # fapfar is updated every time we reload a rank stat pdf file in
                        # StrikeSink
                        # FIXME: reconcile far and combined_far
                        e["far"] = self.strike_object.fapfar.far_from_rank(
                            e["likelihood"]
                        )
                        e["combined_far"] = (
                            e["far"] * self.strike_object.FAR_trialsfactor
                        )
                        if (
                            len(trigs) == 1
                            and self.strike_object.cap_singles
                            and e["combined_far"]
                            < 1.0 / self.strike_object.fapfar.livetime
                        ):
                            # FIXME: do we still need this cap singles?
                            e["combined_far"] = 1.0 / self.strike_object.fapfar.livetime
                    else:
                        e["far"] = None
                        e["combined_far"] = None

                    self.strike_object.zerolag_rank_stat_pdfs[
                        bankid
                    ].zero_lag_lr_lnpdf.count[
                        e["likelihood"],
                    ] += 1
                else:
                    e["likelihood"] = None
                    e["far"] = None
                    e["combined_far"] = None

            max_likelihood, max_likelihood_t, max_likelihood_far = max(
                (e["likelihood"], e["time"], e["far"]) for e in event_data
            )
            max_likelihood_t /= 1e9
            coinc_dict_list = []
            for e in event_data:
                # FIXME: do we need anything else?
                if e["likelihood"] is not None:
                    coinc_dict = {
                        "end": e["time"] / 1e9,
                        "likelihood": e["likelihood"],
                    }
                    coinc_dict_list.append(coinc_dict)

            kafka_data = {"coinc": coinc_dict_list}
            # FIXME: what to do when likelihood is -inf?
            if max_likelihood is not None and not math.isinf(max_likelihood):
                kafka_data["likelihood_history"] = {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood)],
                }
            if max_likelihood_far is not None:
                kafka_data["far_history"] = {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood_far)],
                }

        else:
            kafka_data = None

        self.output_frames[self.event_pad] = EventFrame(
            events=events,
            EOS=frame.EOS,
        )

        self.output_frames[self.kafka_pad] = EventFrame(
            events={"kafka": EventBuffer(ts=ts, te=te, data=kafka_data)},
            EOS=frame.EOS,
        )

    def new(self, pad):
        return self.output_frames[self.rsrcs[pad]]
