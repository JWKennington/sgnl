from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sgn.control import HTTPControlSinkElement
from sgn.sinks import SinkElement


@dataclass
class GraceDBSink(HTTPControlSinkElement):

    def __post_init__(self):
        assert len(self.sink_pad_names) == 1
        super().__post_init__()
        self.events = None
        self.state = {"far-threshold": -1.0}

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.events = frame

    def internal(self):
        HTTPControlSinkElement.exchange_state(self.name, self.state)
