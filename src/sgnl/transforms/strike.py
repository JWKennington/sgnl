"""A transform element to assign likelihood and FAR to events"""

# Copyright (C) 2024-2025 Yun-Jing Huang

from dataclasses import dataclass

from sgn.base import TransformElement
from sgnts.base import EventFrame

from sgnl.strike_object import StrikeObject


@dataclass
class StrikeTransform(TransformElement):
    """
    Compute LR and FAR
    """

    strike_object: StrikeObject = None

    def __post_init__(self):
        assert len(self.sink_pad_names) == 1
        assert self.strike_object is not None
        super().__post_init__()
        self.frame = None
        self.output_frame = None

    def pull(self, pad, frame):
        self.frame = frame

    def internal(self):
        """
        compute LR and FAR
        """
        frame = self.frame
        events = self.frame.events
        event_data = events["event"].data
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

                    e["likelihood"] = float(
                        self.strike_object.ln_lr_from_triggers[bankid](
                            trigs, self.strike_object.offset_vectors
                        )
                    )

                    if self.strike_object.fapfar is not None:
                        # fapfar is updated every time we reload a rank stat pdf file in
                        # StrikeSink
                        # FIXME: reconcile far and combined_far
                        e["false_alarm_probability"] = (
                            self.strike_object.fapfar.fap_from_rank(e["likelihood"])
                        )
                        e["combined_far"] = (
                            self.strike_object.fapfar.far_from_rank(e["likelihood"])
                            * self.strike_object.FAR_trialsfactor
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
                        e["false_alarm_probability"] = None
                        e["combined_far"] = None

                    if self.strike_object.zerolag_rank_stat_pdfs is not None:
                        self.strike_object.zerolag_rank_stat_pdfs[
                            bankid
                        ].zero_lag_lr_lnpdf.count[
                            e["likelihood"],
                        ] += 1
                else:
                    e["likelihood"] = None
                    e["false_alarm_probability"] = None
                    e["combined_far"] = None

        self.output_frame = EventFrame(
            events=events,
            EOS=frame.EOS,
        )

    def new(self, pad):
        return self.output_frame
