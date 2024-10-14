from dataclasses import dataclass
from sgn.sinks import SinkElement
import stillsuit


@dataclass
class StillSuitSink(SinkElement):
    config_name: str = None
    trigger_output: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.config_name is None:
            raise ValueError("Must provide config_name")
        if self.trigger_output is None:
            raise ValueError("Must provide trigger_output")
        self.out = stillsuit.StillSuit(config=self.config_name, dbname=":memory:")
        self.tables = ["data", "trigger", "event"]
        self.event_dict = {t: [] for t in self.tables}

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

        if "data" in frame.events:
            data = frame.events["data"].data 
            if data is not None:
                self.event_dict["data"].append(data)
        else:
            self.event_dict["trigger"] = frame.events["trigger"].data
            self.event_dict["event"] = frame.events["event"].data

    def internal(self, pad):
        # check if there are empty buffers
        if None in self.event_dict["data"]:
            self.event_dict = {t: [] for t in self.tables}
            return

        for v in self.event_dict.values():
            if v is None:
                self.event_dict = {t: [] for t in self.tables}
                return

        self.out.insert_events(self.event_dict)
        self.event_dict = {t: [] for t in self.tables}

        if self.at_eos:
            self.out.to_file(self.trigger_output)
