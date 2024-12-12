import math
from dataclasses import dataclass

from sgn.base import TransformElement
from sgnts.base import EventFrame


@dataclass
class StrikeTransform(TransformElement):
    """
    Compute LR and FAR
    """

    def __post_init__(self):
        super().__post_init__()
        assert len(self.sink_pad_names) == 1
        assert len(self.source_pad_names) == 1
        self.frame = None

    def pull(self, pad, frame):
        self.frame = frame

    def new(self, pad):
        """
        compute LR and FAR
        """
        frame = self.frame
        events = self.frame.events
        event_data = events["event"].data
        if event_data is not None:
            for e in event_data:
                lr = e["network_snr"] ** 2 / 2
                e["likelihood"] = lr
                e["far"] = math.exp(-lr) * 10000

        return EventFrame(
            events=events,
            EOS=frame.EOS,
        )
