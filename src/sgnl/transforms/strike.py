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

    def __post_init__(self):
        self.source_pad_names = (self.event_pad, self.kafka_pad)
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

        if event_data is not None:
            for e in event_data:
                lr = e["network_snr"] ** 2 / 2
                e["likelihood"] = lr
                e["far"] = math.exp(-lr) * 10000

            max_likelihood, max_likelihood_t, max_likelihood_far = max(
                (e["likelihood"], e["time"], e["far"]) for e in event_data
            )
            max_likelihood_t /= 1e9
            kafka_data = {
                "likelihood_history": {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood)],
                },
                "far_history": {
                    "time": [float(max_likelihood_t)],
                    "data": [float(max_likelihood_far)],
                },
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
