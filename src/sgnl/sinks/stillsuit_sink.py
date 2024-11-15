from dataclasses import dataclass
from typing import Any, Sequence

import stillsuit
import yaml
from ligo import segments
from sgn.sinks import SinkElement
from sgnts.base import Offset


@dataclass
class StillSuitSink(SinkElement):
    config_name: str = None
    trigger_output: str = None
    template_ids: Sequence[Any] = None
    template_sngls: list = None
    subbankids: Sequence[Any] = None
    itacacac_pad_name: str = None
    segments_pad_map: dict[str, str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.config_name is None:
            raise ValueError("Must provide config_name")
        if self.trigger_output is None:
            raise ValueError("Must provide trigger_output")
        self.out = stillsuit.StillSuit(config=self.config_name, dbname=":memory:")

        self.tables = ["trigger", "event"]
        self.event_dict = {t: [] for t in self.tables}

        # insert filter
        with open(self.config_name) as f:
            self.config = yaml.safe_load(f)

        # filter table
        filters = []
        for i, subbank in enumerate(self.template_sngls):
            for template_id, sngl in subbank.items():
                filter_row = self.init_config_row(self.config["filter"])
                filter_row["_filter_id"] = template_id
                filter_row["bank_id"] = int(self.subbankids[i].split("_")[0])
                filter_row["subbank_id"] = int(self.subbankids[i].split("_")[1])
                filter_row["end_time_delta"] = (
                    sngl.end_time * 1_000_000_000 + sngl.end_time_ns
                )
                filter_row["mass1"] = sngl.mass1
                filter_row["mass2"] = sngl.mass2
                filter_row["spin1x"] = sngl.spin1x
                filter_row["spin1y"] = sngl.spin1y
                filter_row["spin1z"] = sngl.spin1z
                filter_row["spin2x"] = sngl.spin2x
                filter_row["spin2y"] = sngl.spin2y
                filter_row["spin2z"] = sngl.spin2z
                filters.append(filter_row)
        self.out.insert_static({"filter": filters})

        #
        # Segments
        #
        ifos = self.segments_pad_map.values()
        self.segments = segments.segmentlistdict(
            {ifo: segments.segmentlist() for ifo in ifos}
        )

    def init_config_row(self, table, extra=None):
        out = {
            c["name"]: None for c in table["columns"] if not c["name"].startswith("__")
        }
        if extra is not None:
            out.update(extra)
        return out

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

        pad_name = pad.name.split(":")[-1]
        if pad_name == self.itacacac_pad_name:
            self.event_dict["trigger"] = frame.events["trigger"].data
            self.event_dict["event"] = frame.events["event"].data
        else:
            for buf in frame:
                if buf.data is not None:
                    self.segments[self.segments_pad_map[pad_name]].append(
                        segments.segment(frame.offset, frame.end_offset)
                    )

    def internal(self, pad):
        # check if there are empty buffers
        for v in self.event_dict.values():
            if v is None:
                self.event_dict = {t: [] for t in self.tables}
                return

        for event, trigger in zip(self.event_dict["event"], self.event_dict["trigger"]):
            self.out.insert_event({"event": event, "trigger": trigger})
        self.event_dict = {t: [] for t in self.tables}

        if self.at_eos:
            self.segments.coalesce()
            out_segments = []
            for ifo, seg in self.segments.items():
                for segment in seg:
                    segment_row = self.init_config_row(self.config["segment"])
                    segment_row["start_time"] = Offset.tons(segment[0])
                    segment_row["end_time"] = Offset.tons(segment[1])
                    segment_row["ifo"] = ifo
                    segment_row["name"] = "afterhtgate"
                    out_segments.append(segment_row)

            self.out.insert_static({"segment": out_segments})
            self.out.to_file(self.trigger_output)
