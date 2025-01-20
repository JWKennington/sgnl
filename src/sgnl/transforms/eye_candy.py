# Copyright (C) 2009-2013  Kipp Cannon, Chad Hanna, Drew Keppel
# Copyright (C) 2025       Yun-Jing Huang

from __future__ import annotations

import math
import resource
from dataclasses import dataclass

from sgn.base import TransformElement
from sgnligo.base import now
from sgnts.base import EventBuffer, EventFrame


@dataclass
class EyeCandy(TransformElement):

    template_sngls: list = None
    event_pad: str = None
    state_vector_pads: dict[str, str] = None
    ht_gate_pads: dict[str, str] = None

    def __post_init__(self):
        self.sink_pad_names = (
            (self.event_pad,)
            + tuple(self.state_vector_pads.values())
            + tuple(self.ht_gate_pads.values())
        )
        super().__post_init__()

        self.ifos = self.state_vector_pads.keys()

        self.sngls_dict = {}
        for sub in self.template_sngls:
            for tid, sngl in sub.items():
                self.sngls_dict[tid] = sngl

        self.outframe = None
        self.startup_time = float(now())
        self.frames = {pad: None for pad in self.sink_pad_names}

    def pull(self, pad, frame):
        self.frames[self.rsnks[pad]] = frame

    def internal(self):
        super().internal()

        time_now = float(now())

        kafka_data = {}

        #
        # Segments
        #
        # statevectorsegments
        for ifo in self.ifos:
            kafka_data[ifo + "_statevectorsegments"] = {"time": [], "data": []}
            state_frame = self.frames[self.state_vector_pads[ifo]]
            for buf in state_frame:
                kafka_data[ifo + "_statevectorsegments"]["time"].append(
                    float(buf.t0 / 1e9)
                )
                kafka_data[ifo + "_statevectorsegments"]["data"].append(
                    0 if buf.is_gap else 1
                )

        # afterhtgate
        for ifo in self.ifos:
            kafka_data[ifo + "_afterhtgatesegments"] = {"time": [], "data": []}
            ht_gate_frame = self.frames[self.ht_gate_pads[ifo]]
            for buf in ht_gate_frame:
                kafka_data[ifo + "_afterhtgatesegments"]["time"].append(
                    float(buf.t0 / 1e9)
                )
                kafka_data[ifo + "_afterhtgatesegments"]["data"].append(
                    0 if buf.is_gap else 1
                )

        #
        # ram history
        #
        ram = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        ) / 1048576.0
        kafka_data["ram_history"] = {"time": [time_now], "data": [float(ram)]}

        #
        # uptime
        #
        kafka_data["uptime"] = {
            "time": [float(now())],
            "data": [float(now()) - self.startup_time],
        }

        #
        # ifo snr history
        #
        frame = self.frames[self.event_pad]
        max_snrs = frame["max_snr_histories"].data

        if max_snrs is not None:
            # FIXME: this is different from gstlal
            for ifo, data in max_snrs.items():
                kafka_data[ifo + "_snr_history"] = {
                    "time": [float(data["time"])],
                    "data": [float(data["snr"])],
                }

        events = frame["event"].data
        framets = frame["event"].ts
        framete = frame["event"].te
        EOS = frame.EOS
        if events is None:
            self.outframe = EventFrame(
                events={"kafka": EventBuffer(framets, framete, data=kafka_data)},
                EOS=EOS,
            )
            return

        triggers = frame["trigger"].data

        #
        # coinc snr history
        #
        max_snr, max_snr_t = max((e["network_snr"], e["time"]) for e in events)
        max_snr_t /= 1e9
        kafka_data["time"] = float(max_snr_t)
        kafka_data["snr_history"] = float(max_snr)

        #
        # latency history
        #
        mincoinc_time = min(e["time"] / 1e9 for e in events)
        kafka_data["latency_history"] = {
            "time": [mincoinc_time],
            "data": [time_now - mincoinc_time],
        }

        #
        # likelihood history, far history
        #
        lr_events = [e for e in events if e["likelihood"] is not None]
        if len(lr_events) > 0:
            max_likelihood, max_likelihood_t, max_likelihood_far = max(
                (e["likelihood"], e["time"], e["combined_far"]) for e in events
            )
            max_likelihood_t /= 1e9

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

        #
        # coinc dict
        #
        coinc_dict_list = []
        for e, t in zip(events, triggers):
            # FIXME: do we need anything else?
            coinc_dict = {
                "snr": e["network_snr"],
                "end": e["time"] / 1e9,
            }
            for ti in t:
                if ti is not None:
                    sngl_row = self.sngls_dict[ti["_filter_id"]]
                    ifo = ti["ifo"]
                    for col in ("snr", "chisq"):
                        coinc_dict[ifo + "_" + col] = float(ti[col])
                    coinc_dict[ifo + "_coa_phase"] = float(ti["phase"])
                    for col in ("mass1", "mass2", "spin1z", "spin2z"):
                        coinc_dict[ifo + "_" + col] = float(getattr(sngl_row, col))

            if e["likelihood"] is not None:
                coinc_dict["likelihood"] = e["likelihood"]
            if e["combined_far"] is not None:
                coinc_dict["false_alarm_probability"] = e["false_alarm_probability"]
                coinc_dict["combined_far"] = e["combined_far"]
            coinc_dict_list.append(coinc_dict)

        kafka_data["coinc"] = coinc_dict_list

        #
        # heartbeat FIXME
        #
        # if t - self.heartbeat_time > 10*60:
        #     self.heartbeat_time = int(t)
        #     self.client.write(f"sgnl.{self.analysis}.{self.topic_prefix}events",
        #     {'time': self.heartbeat_time}, tags='heartbeat')

        #
        # segments FIXME
        #

        self.outframe = EventFrame(
            events={"kafka": EventBuffer(framets, framete, data=kafka_data)}, EOS=EOS
        )

    def new(self, pad):
        return self.outframe
