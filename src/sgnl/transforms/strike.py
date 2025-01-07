import math
from dataclasses import dataclass

from sgn.base import TransformElement
from sgnts.base import EventBuffer, EventFrame


@dataclass
class StrikeTransform(TransformElement):
    """
    Compute LR and FAR
    """

    event_pad: str = None
    kafka_pad: str = None
    zerolag_pad: str = None

    def __post_init__(self):
        self.source_pad_names = (self.event_pad, self.kafka_pad, self.zerolag_pad)
        assert len(self.sink_pad_names) == 1
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

        zerolags = []
        if event_data is not None:
            for e in event_data:
                lr = e["network_snr"] ** 2 / 2
                e["likelihood"] = lr
                e["far"] = math.exp(-lr) * 10000
                zerolags.append({"bankid": e["bankid"], "likelihood": e["likelihood"]})

            max_likelihood, max_likelihood_t, max_likelihood_far = max(
                (e["likelihood"], e["time"], e["far"]) for e in event_data
            )
            max_likelihood_t /= 1e9
            coinc_dict_list = []
            for e in event_data:
                # FIXME: do we need anything else?
                coinc_dict = {
                    "end": e["time"] / 1e9,
                    "likelihood": e["likelihood"],
                }
                coinc_dict_list.append(coinc_dict)

            kafka_data = {
                "likelihood_history": {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood)],
                },
                "far_history": {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood_far)],
                },
                "coinc": coinc_dict_list,
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

        self.output_frames[self.zerolag_pad] = EventFrame(
            events={"zerolag": EventBuffer(ts=ts, te=te, data=zerolags)},
            EOS=frame.EOS,
        )

    def new(self, pad):
        return self.output_frames[self.rsrcs[pad]]
