"""A sink element to write triggers into a sqlite database."""

# Copyright (C) 2024-2025 Yun-Jing Huang

import os
import socket
from dataclasses import dataclass
from queue import Empty
from time import asctime
from typing import Any, Sequence

import igwn_segments as segments
import stillsuit
import yaml
from sgn.subprocess import Parallelize, ParallelizeSinkElement, WorkerContext
from sgnligo.base import now
from sgnts.base import Offset

from sgnl.control import SnapShotControlSinkElement
from sgnl.strike_object import StrikeObject


def init_config_row(table, extra=None):
    out = {c["name"]: None for c in table["columns"] if not c["name"].startswith("__")}
    if extra is not None:
        out.update(extra)
    return out


def init_dbs(
    ifos: list,
    config_name: str,
    bankids: list,
    process: dict,
    process_params: list,
    sim: list,
    filters: dict,
):
    dbs = {}
    temp_segments = {}
    for bankid in bankids:
        print(f"{asctime()} Initialize a new db for bankid: {bankid}", flush=True)
        dbs[bankid], temp_segments[bankid] = init_static(
            ifos, config_name, process, process_params, filters[bankid], sim
        )
    return dbs, temp_segments


def init_static(ifos, config_name, process_row, params, filters, sims=None):
    process_row["start_time"] = int(now())

    out = stillsuit.StillSuit(config=config_name, dbname=":memory:")
    out.insert_static({"process_params": params})
    out.insert_static({"filter": filters})
    if sims is not None:
        out.insert_static({"simulation": sims})

    #
    # Segments
    #
    temp_segments = segments.segmentlistdict(
        {ifo: segments.segmentlist() for ifo in ifos}
    )

    return out, temp_segments


def on_snapshot(
    data,
    temp_segments,
    dbs,
    ifos,
    config_name,
    config_segment,
    process,
    process_params,
    filters,
    sim,
    shutdown,
):
    snapshot_data = data["snapshot"]
    fn = snapshot_data["fn"]
    bankid = snapshot_data["bankid"]
    process["end_time"] = int(now())

    # Insert segments
    # FIXME: they don't have to be bank dependent if we switch to
    # writing out all the banks at once
    temp_segments[bankid].coalesce()
    out_segments = []
    for ifo, seg in temp_segments[bankid].items():
        for segment in seg:
            segment_row = init_config_row(config_segment)
            segment_row["start_time"] = Offset.tons(segment[0])
            segment_row["end_time"] = Offset.tons(segment[1])
            segment_row["ifo"] = ifo
            segment_row["name"] = "afterhtgate"
            out_segments.append(segment_row)

    # Write in-memory database to file
    print(f"{asctime()} Writing out db {fn}...", flush=True)
    dbs[bankid].insert_static({"segment": out_segments})
    dbs[bankid].insert_static({"process": [process]})
    dbs[bankid].to_file(fn)
    dbs[bankid].db.close()

    # Create a new db
    dbs[bankid], temp_segments[bankid] = init_static(
        ifos, config_name, process, process_params, filters[bankid], sim
    )

    if "in_lr_file" in snapshot_data and not shutdown:
        # Update LRs for injection jobs here because they don't have
        # snapshots in StrikeSink
        print(f"{asctime()} StillSuitSink update assign lr: {bankid}")
        in_lr_file = snapshot_data["in_lr_file"]
        lr_dict = StrikeObject.on_snapshot_reload(in_lr_file)
        lr_dict["bankid"] = bankid
    else:
        lr_dict = None
    return lr_dict


def insert_event(data, dbs):
    event_dict = data["event_dict"]
    for event, trigger in zip(event_dict["event"], event_dict["trigger"]):
        # FIXME: make insert event skip bankid
        bankid = event["bankid"]
        dbs[bankid].insert_event(
            {"event": event, "trigger": trigger},
            ignore_cols={
                "event": ["network_snr_subthresh", "time_subthresh", "bankid"],
                "trigger": ["template_duration"],
            },
        )


def append_segment(data, temp_segments):
    seg_data = data["segment"]
    for ifo, seg in seg_data.items():
        for temp_segment in temp_segments.values():
            temp_segment[ifo].append(segments.segment(*seg))


@dataclass
class StillSuitSink(SnapShotControlSinkElement, ParallelizeSinkElement):
    ifos: list = None  # type: ignore[assignment]
    config_name: str = None  # type: ignore[assignment]
    bankids_map: dict[str, list] = None  # type: ignore[assignment]
    template_ids: Sequence[Any] = None  # type: ignore[assignment]
    template_sngls: list = None  # type: ignore[assignment]
    subbankids: Sequence[Any] = None  # type: ignore[assignment]
    itacacac_pad_name: str = None  # type: ignore[assignment]
    segments_pad_map: dict[str, str] = None  # type: ignore[assignment]
    trigger_output: dict[str, str] = None  # type: ignore[assignment]
    process_params: dict = None  # type: ignore[assignment]
    program: str = ""
    injection_list: list = None  # type: ignore[assignment]
    is_online: bool = False
    multiprocess: bool = False
    jobid: int = 0
    nsubbank_pretend: bool = False
    verbose: bool = False
    injections: bool = False
    strike_object: StrikeObject | None = None

    def __post_init__(self):
        # Initialize mutable defaults
        if self.trigger_output is None:
            self.trigger_output = {}
        if self.process_params is None:
            self.process_params = {}
        if self.injection_list is None:
            self.injection_list = []

        self._use_threading_override = not self.multiprocess
        if self.config_name is None:
            raise ValueError("Must provide config_name")
        if not self.is_online and not self.trigger_output:
            raise ValueError("Must provide trigger_output")

        SnapShotControlSinkElement.__post_init__(self)

        self.tables = ["trigger", "event"]
        self.event_dict = {t: [] for t in self.tables}
        self.bankids = self.bankids_map.keys()

        with open(self.config_name) as f:
            self.config = yaml.safe_load(f)

        if self.is_online:
            for bankid in self.bankids:
                # FIXME: use job_tag?? but what to use with multi-bank mode?
                if self.injections:
                    fn_bankid = bankid + "_inj"
                else:
                    fn_bankid = bankid + "_noninj"

                self.add_snapshot_filename("%s_SGNL_TRIGGERS" % fn_bankid, "sqlite.gz")
            self.register_snapshot()

        #
        # Process
        #
        self.process_row = init_config_row(self.config["process"])
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
                        param_row = init_config_row(self.config["process_params"])
                        param_row["param"] = name
                        param_row["program"] = self.program
                        param_row["value"] = str(v)
                        self.params.append(param_row)
                    continue

                param_row = init_config_row(self.config["process_params"])
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
                filter_row = init_config_row(self.config["filter"])
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
                    filter_row = init_config_row(self.config["filter"])
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
                sim_row = init_config_row(self.config["simulation"])
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
        else:
            self.sims = None

        # Derived parameters needed by worker_process
        self.bankids = list(self.bankids)
        self.process = self.process_row
        self.process_params = self.params
        self.sim = None if self.injection_list is None else self.sims
        self.config_segment = self.config["segment"]

        ParallelizeSinkElement.__post_init__(self)

        # FIXME Parallelize.enabled is only enabled once the
        # pipeline starts, so delay initializing the dbs until later
        self.init = False

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

    def process_outqueue(self, data):
        new_lr = data["new_lr"]
        frankenstein = data["frankenstein"]
        likelihood_ratio_upload = data["likelihood_ratio_upload"]
        bankid = data["bankid"]
        print(f"{asctime()} StillSuit Update dynamic", bankid, flush=True)
        self.strike_object.update_dynamic(
            bankid, frankenstein, likelihood_ratio_upload, new_lr
        )

    def get_state_from_queue(self):
        try:
            data = self.out_queue.get_nowait()
            self.process_outqueue(data)
        except Empty:
            return

    def pull(self, pad, frame):
        # FIXME Parallelize.enabled is only enabled once the
        # pipeline starts, so delay initializing the dbs to here
        # if not in subprocess mode
        if not self.init and not Parallelize.enabled:
            self.dbs, self.temp_segments = init_dbs(
                self.ifos,
                self.config_name,
                self.bankids,
                self.process,
                self.process_params,
                self.sim,
                self.filters,
            )
            self.init = True

        if frame.EOS:
            self.mark_eos(pad)

        pad_name = self.rsnks[pad]
        if pad_name == self.itacacac_pad_name:
            if frame.events["event"].data is not None:
                data = {
                    "event_dict": {
                        "trigger": frame.events["trigger"].data,
                        "event": frame.events["event"].data,
                    }
                }
                self.in_queue.put(data)
        else:
            for buf in frame:
                if buf.data is not None:
                    data = {
                        "segment": {
                            self.segments_pad_map[pad_name]: (
                                frame.offset,
                                frame.end_offset,
                            )
                        }
                    }
                    self.in_queue.put(data)

    def internal(self):
        super().internal()
        if self.at_eos:
            if self.is_online:
                for bankid in self.bankids:
                    if self.injections:
                        fn_bankid = bankid + "_inj"
                    else:
                        fn_bankid = bankid + "_noninj"
                    desc = "%s_SGNL_TRIGGERS" % fn_bankid
                    fn = self.snapshot_filenames(desc)
                    sdict = {
                        "snapshot": {
                            "fn": fn,
                            "bankid": bankid,
                        }
                    }
                    self.in_queue.put(sdict)
                if self.terminated.is_set():
                    print("At EOS and subprocess is terminated")
                else:
                    drained_outq = self.sub_process_shutdown(600)
                    print("after shutdown", len(drained_outq))
            else:
                for bankid, fn in self.trigger_output.items():
                    sdict = {
                        "snapshot": {
                            "fn": fn,
                            "bankid": bankid,
                        }
                    }
                    on_snapshot(
                        sdict,
                        self.temp_segments,
                        self.dbs,
                        self.ifos,
                        self.config_name,
                        self.config["segment"],
                        self.process_row,
                        self.params,
                        self.filters,
                        self.sims,
                        True,
                    )
        else:
            if self.is_online:
                self.get_state_from_queue()
                for i, bankid in enumerate(self.bankids):
                    # FIXME: consider not looping over banks, since
                    # we have the subprocess
                    if self.injections:
                        fn_bankid = bankid + "_inj"
                    else:
                        fn_bankid = bankid + "_noninj"
                    desc = "%s_SGNL_TRIGGERS" % fn_bankid
                    if self.snapshot_ready(desc):
                        fn = self.snapshot_filenames(desc)
                        sdict = {
                            "snapshot": {
                                "fn": fn,
                                "bankid": bankid,
                            }
                        }
                        if self.injections:
                            if i == 0:
                                self.strike_object.load_rank_stat_pdf()
                            in_lr_file = self.strike_object.input_likelihood_file[
                                bankid
                            ]
                            sdict["snapshot"]["in_lr_file"] = in_lr_file
                        self.in_queue.put(sdict)

    @staticmethod
    def worker_process(
        context: WorkerContext,
        ifos: list,
        config_name: str,
        bankids: list,
        process: dict,
        process_params: list,
        sim: list,
        config_segment: dict,
        filters: dict,
    ):
        # Initialize worker state if needed
        if not context.state.get("subproc_init", False):
            context.state["dbs"], context.state["temp_segments"] = init_dbs(
                ifos,
                config_name,
                bankids,
                process,
                process_params,
                sim,
                filters,
            )
            context.state["subproc_init"] = True

        try:
            data = context.input_queue.get(timeout=1)
            if "segment" in data:
                append_segment(data, context.state["temp_segments"])
            elif "event_dict" in data:
                insert_event(data, context.state["dbs"])
            elif "snapshot" in data:
                lr_dict = on_snapshot(
                    data,
                    context.state["temp_segments"],
                    context.state["dbs"],
                    ifos,
                    config_name,
                    config_segment,
                    process,
                    process_params,
                    filters,
                    sim,
                    context.should_shutdown(),
                )
                if lr_dict is not None:
                    context.output_queue.put(lr_dict)
            else:
                raise ValueError("Unknown data")
        except Empty:
            return
