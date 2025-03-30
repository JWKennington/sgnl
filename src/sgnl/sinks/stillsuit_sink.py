"""A sink element to write triggers into a sqlite database."""

# Copyright (C) 2024 Yun-Jing Huang

import os
import socket
from dataclasses import dataclass
from time import asctime
from typing import Any, Sequence

import stillsuit
import yaml
from ligo import segments
from sgnligo.base import now
from sgnts.base import Offset

from sgnl.control import SnapShotControlSinkElement


@dataclass
class StillSuitSink(SnapShotControlSinkElement):
    ifos: list = None
    config_name: str = None
    bankids_map: dict[str, list] = None
    trigger_output: dict[str, str] = None
    template_ids: Sequence[Any] = None
    template_sngls: list = None
    subbankids: Sequence[Any] = None
    itacacac_pad_name: str = None
    segments_pad_map: dict[str, str] = None
    process_params: dict = None
    program: str = ""
    injection_list: list = None
    is_online: bool = False
    jobid: int = 0
    nsubbank_pretend: bool = False
    verbose: bool = False
    injections: bool = False
    strike_object: int = None

    def __post_init__(self):
        super().__post_init__()
        if self.config_name is None:
            raise ValueError("Must provide config_name")
        if not self.is_online and self.trigger_output is None:
            raise ValueError("Must provide trigger_output")

        self.tables = ["trigger", "event"]
        self.event_dict = {t: [] for t in self.tables}
        self.bankids = self.bankids_map.keys()

        with open(self.config_name) as f:
            self.config = yaml.safe_load(f)

        if self.is_online:
            for bankid in self.bankids:
                four_digit_id = "%04d" % int(bankid)

                # FIXME: use job_tag?? but what to use with multi-bank mode?
                if self.injections:
                    four_digit_id = four_digit_id + "_inj"
                else:
                    four_digit_id = four_digit_id + "_noninj"

                self.add_snapshot_filename(
                    "%s_SGNL_TRIGGERS" % four_digit_id, "sqlite.gz"
                )
            self.register_snapshot()

        #
        # Process
        #
        self.process_row = self.init_config_row(self.config["process"])
        self.process_row["ifos"] = ",".join(self.ifos)
        self.process_row["is_online"] = int(self.is_online)
        self.process_row["node"] = socket.gethostname()
        self.process_row["program"] = self.program
        self.process_row["unix_procid"] = os.getpid()
        self.process_row["username"] = self.get_username()

        #
        # Process params
        #
        self.params = []
        if self.process_params is not None:
            for name, values in self.process_params.items():
                name = "--%s" % name.replace("_", "-")
                if values is None:
                    continue
                elif isinstance(values, list):
                    for v in values:
                        param_row = self.init_config_row(self.config["process_params"])
                        param_row["param"] = name
                        param_row["program"] = self.program
                        param_row["value"] = str(v)
                        self.params.append(param_row)
                    continue

                param_row = self.init_config_row(self.config["process_params"])
                param_row["param"] = name
                param_row["program"] = self.program
                param_row["value"] = str(values)
                self.params.append(param_row)

        #
        # Filter
        #
        self.filters = {k: [] for k in self.bankids}
        if self.nsubbank_pretend:
            subbank = self.template_sngls[0]
            for template_id, sngl in subbank.items():
                filter_row = self.init_config_row(self.config["filter"])
                filter_row["_filter_id"] = template_id
                filter_row["bank_id"] = int(self.subbankids[0].split("_")[0])
                filter_row["subbank_id"] = int(self.subbankids[0].split("_")[1])
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
                # FIXME should we keep this as seconds?
                # convert to nanoseconds to be consistent with all times in the database
                filter_row["template_duration"] = int(
                    sngl.template_duration * 1_000_000_000
                )
                self.filters["%04d" % int(bankid) + "_0"].append(filter_row)
        else:
            for i, subbank in enumerate(self.template_sngls):
                for template_id, sngl in subbank.items():
                    bankid = self.subbankids[i].split("_")[0]
                    filter_row = self.init_config_row(self.config["filter"])
                    filter_row["_filter_id"] = template_id
                    filter_row["bank_id"] = int(bankid)
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
                    filter_row["template_duration"] = int(
                        sngl.template_duration * 1_000_000_000
                    )
                    self.filters["%04d" % int(bankid)].append(filter_row)

        #
        # Simulation
        #
        if self.injection_list is not None:
            self.sims = []
            for inj in self.injection_list:
                sim_row = self.init_config_row(self.config["simulation"])
                sim_row["_simulation_id"] = inj.simulation_id
                sim_row["coa_phase"] = inj.coa_phase
                sim_row["distance"] = inj.distance
                sim_row["f_final"] = inj.f_final
                sim_row["f_lower"] = inj.f_lower
                sim_row["geocent_end_time"] = (
                    inj.geocent_end_time * 1_000_000_000 + inj.geocent_end_time_ns
                )
                sim_row["inclination"] = inj.inclination
                sim_row["polarization"] = inj.polarization
                sim_row["mass1"] = inj.mass1
                sim_row["mass2"] = inj.mass2
                sim_row["snr_H1"] = inj.alpha4
                sim_row["snr_L1"] = inj.alpha5
                sim_row["snr_V1"] = inj.alpha6
                sim_row["spin1x"] = inj.spin1x
                sim_row["spin1y"] = inj.spin1y
                sim_row["spin1z"] = inj.spin1z
                sim_row["spin2x"] = inj.spin2x
                sim_row["spin2y"] = inj.spin2y
                sim_row["spin2z"] = inj.spin2z
                sim_row["waveform"] = inj.waveform
                self.sims.append(sim_row)

        self.out = {}
        for bankid in self.bankids:
            self.init_static(bankid)

    def get_username(self):
        try:
            return os.environ["LOGNAME"]
        except KeyError:
            pass
        try:
            return os.environ["USERNAME"]
        except KeyError:
            pass
        try:
            import pwd

            return pwd.getpwuid(os.getuid())[0]
        except (ImportError, KeyError):
            raise KeyError

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

        pad_name = self.rsnks[pad]
        if pad_name == self.itacacac_pad_name:
            self.event_dict["trigger"] = frame.events["trigger"].data
            self.event_dict["event"] = frame.events["event"].data
        else:
            for buf in frame:
                if buf.data is not None:
                    self.segments[self.segments_pad_map[pad_name]].append(
                        segments.segment(frame.offset, frame.end_offset)
                    )

    def internal(self):
        # check if there are empty buffers
        if self.event_dict["event"] is not None:
            for event, trigger in zip(
                self.event_dict["event"], self.event_dict["trigger"]
            ):
                # FIXME: make insert event skip bankid
                bankid = event["bankid"]
                event.pop("bankid")
                for trig in trigger:
                    if trig is not None:
                        trig.pop("template_duration")
                self.out[bankid].insert_event({"event": event, "trigger": trigger})
        self.event_dict = {t: [] for t in self.tables}

        if self.at_eos:
            if self.is_online:
                for bankid in self.bankids:
                    if self.injections:
                        fn_bankid = bankid + "_inj"
                    else:
                        fn_bankid = bankid + "_noninj"
                    desc = "%s_SGNL_TRIGGERS" % fn_bankid
                    fn = self.snapshot_filenames(desc)
                    self.on_snapshot(fn, bankid)
            else:
                for bankid, fn in self.trigger_output.items():
                    self.on_snapshot(fn, bankid)

        if self.is_online:
            for i, bankid in enumerate(self.bankids):
                if self.injections:
                    fn_bankid = bankid + "_inj"
                else:
                    fn_bankid = bankid + "_noninj"
                desc = "%s_SGNL_TRIGGERS" % fn_bankid
                if self.snapshot_ready(desc):
                    fn = self.snapshot_filenames(desc)
                    self.on_snapshot(fn, bankid)
                    self.init_static(bankid)
                    if self.injections:
                        print(f"{asctime()} StillSuitSink update assign lr: {bankid}")
                        self.strike_object.update_assign_lr(bankid)
                        if i == 0:
                            self.strike_object.load_rank_stat_pdf()

    def init_static(self, bankid):
        print(f"{asctime()} Initialize a new db for bankid: {bankid}")
        self.process_row["start_time"] = int(now())

        self.out[bankid] = stillsuit.StillSuit(
            config=self.config_name, dbname=":memory:"
        )
        self.out[bankid].insert_static({"process_params": self.params})
        self.out[bankid].insert_static({"filter": self.filters[bankid]})
        #
        # Segments
        #
        ifos = self.segments_pad_map.values()
        self.segments = segments.segmentlistdict(
            {ifo: segments.segmentlist() for ifo in ifos}
        )
        if self.injection_list is not None:
            self.out[bankid].insert_static({"simulation": self.sims})

    def on_snapshot(self, fn, bankid):
        # Write out segments
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
        self.out[bankid].insert_static({"segment": out_segments})

        # Add end time in process table
        self.process_row["end_time"] = int(now())
        self.out[bankid].insert_static({"process": [self.process_row]})

        # Write in-memory database to file
        print(f"{asctime()} Writing out db {fn}...")
        self.out[bankid].to_file(fn)
        self.out[bankid].db.close()
