"""An inspiral analysis tool built with the stream graph navigator library."""

# Copyright (C) 2009-2014 Kipp Cannon, Chad Hanna, Drew Keppel
# Copyright (C) 2024      Yun-Jing Huang

from __future__ import annotations

import logging
import math
import os
import re
import sys
from argparse import ArgumentParser
from collections import OrderedDict
from typing import List, Tuple

import torch
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.param import use_in as param_use_in
from sgn.apps import Pipeline
from sgn.control import HTTPControl
from sgn.sinks import NullSink
from sgnligo.sinks import KafkaSink
from sgnligo.sources import DataSourceInfo, datasource
from sgnligo.transforms import ConditionInfo, Latency, condition
from sgnts.sinks import DumpSeriesSink
from strike.config import get_analysis_config

from sgnl import simulation
from sgnl.control import SnapShotControl
from sgnl.sinks import GraceDBSink, ImpulseSink, StillSuitSink, StrikeSink
from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnl.strike_object import StrikeObject
from sgnl.transforms import (
    EyeCandy,
    HorizonDistanceTracker,
    Itacacac,
    StrikeTransform,
    lloid,
)

logger = logging.getLogger("sgn.sgnl")

torch.set_num_threads(1)
#  NOTE: experiment with this to see if it helps with performance.
# torch.set_grad_enabled(False)


@array_use_in
@param_use_in
@lsctables.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


def parse_command_line():
    parser = ArgumentParser()

    DataSourceInfo.append_options(parser)
    ConditionInfo.append_options(parser)

    group = parser.add_argument_group(
        "Trigger Generator", "Adjust trigger generator behaviour"
    )
    group.add_argument(
        "--svd-bank",
        metavar="filename",
        action="append",
        required=True,
        help="Set the name of the LIGO light-weight XML file from which to load the "
        "svd bank for a given instrument.  To analyze multiple instruments, --svd-bank "
        "can be called multiple times for svd banks corresponding to different "
        "instruments.  If --data-source is lvshm or framexmit, then only svd banks "
        "corresponding to a single bin must be given. If given multiple times, the "
        "banks will be processed one-by-one, in order.  At least one svd bank for at "
        "least 2 detectors is required, but see also --svd-bank-cache.",
    )
    group.add_argument(
        "--trigger-finding-duration",
        type=float,
        metavar="seconds",
        action="store",
        default=1,
        help="Produce triggers in blocks of this duration.",
    )
    group.add_argument(
        "--snr-min",
        metavar="snr",
        action="store",
        type=float,
        default=4,
        help="Set the minimum snr for identifying triggers.",
    )
    group.add_argument(
        "--coincidence-threshold",
        metavar="seconds",
        action="store",
        type=float,
        default=0.005,
        help="Set the coincidence window in seconds (default = 0.005 s).  The"
        " light-travel time between instruments will be added automatically in the"
        " coincidence test.",
    )
    group.add_argument(
        "--event-config",
        metavar="filename",
        action="store",
        help="Set the name of the config yaml file for event buffers",
    )
    group.add_argument(
        "--trigger-output",
        metavar="filename",
        action="append",
        help="Set the name of the sqlite output file *.sqlite",
    )
    group.add_argument(
        "--snr-timeseries-output",
        metavar="filename",
        help="Set the name of the file to dump SNR timeseries data to",
    )
    group.add_argument(
        "--impulse-bank",
        metavar="filename",
        action="store",
        default=None,
        help="The full original templates to compare the impulse response test with.",
    )
    group.add_argument(
        "--impulse-bankno",
        type=int,
        metavar="index",
        action="store",
        help="The template bank index to perform the impulse test on.",
    )
    group.add_argument(
        "--impulse-ifo",
        action="store",
        help="Only do impulse test on data from this ifo.",
    )
    group.add_argument(
        "--nsubbank-pretend",
        type=int,
        action="store",
        default=0,
        help="Pretend we have this many subbanks by copying the first subbank "
        "this many times",
    )
    group.add_argument(
        "--nslice",
        type=int,
        action="store",
        default=-1,
        help="Only filter this many timeslices. Default: -1, filter all timeslices.",
    )

    group = parser.add_argument_group(
        "Ranking Statistic Options", "Adjust ranking statistic behaviour"
    )
    group.add_argument(
        "--search",
        type=str,
        default=None,
        choices=["ew", None],
        help="Set the search, if you want search-specific changes to be implemented "
        "while creating the RankingStat, and data whitening. "
        "Allowed choices: ['ew', None].",
    )
    group.add_argument(
        "--snapshot-interval",
        metavar="seconds",
        type=int,
        default=14400,
        help="The interval at which to procude snapshots of trigger, likelihood, and "
        "zerolag files",
    )
    # group.add_argument(
    #     "--snapshot-delay",
    #     metavar="seconds",
    #     type=float,
    #     default=0,
    #     help="The delay between different banks at which to procude snapshots.",
    # )
    group.add_argument(
        "--snapshot-multiprocess",
        action="store_true",
        help="Use multiprocessing for online snapshotting.",
    )
    group.add_argument(
        "--far-trials-factor",
        metavar="trials",
        type=float,
        default=1.0,
        help="Add trials factor to FAR before uploading to gracedb",
    )
    group.add_argument(
        "--cap-singles",
        action="store_true",
        help="Cap singles to 1 / livetime if computing FAR. No effect otherwise",
    )
    group.add_argument(
        "--compress-likelihood-ratio",
        action="store_true",
        help="Choose whether to compress the ranking stat upon start up. Only used "
        "when --ranking-stat-input is set.",
    )
    group.add_argument(
        "--compress-likelihood-ratio-threshold",
        type=float,
        default=0.03,
        help="Only keep horizon distance values that differ by this much, "
        "fractionally, from their neighbours (default = 0.03).",
    )
    group.add_argument(
        "--all-triggers-to-background",
        action="store_true",
        help="Save all triggers that pass the snr_min to background. If not set, only "
        "singles during coinc time will be saved to background.",
    )
    group.add_argument(
        "--min-instruments-candidates",
        metavar="num_ifos",
        type=int,
        default=1,
        help="Set the minimum number of instruments to evaluate the likelihood ratio "
        "and record in an output database (default = 1).",
    )
    group.add_argument(
        "--output-likelihood-file",
        metavar="filename",
        action="append",
        help="Set the name of the LIKELIHOOD_RATIO file to which to write likelihood "
        "ratio data collected from triggers (optional).  Can be given more than once. "
        "If given, exactly as many must be provided as there are --svd-bank options "
        "and they will be writen to in order. Forbidden for when --injections is set.",
    )
    group.add_argument(
        "--input-likelihood-file",
        metavar="filename",
        action="append",
        help="Set the name of the LIKELIHOOD_RATIO file from which to read likelihood "
        "ratio data collected from triggers (optional).  Can be given more than once. "
        "If given, exactly as many must be provided as there are --svd-bank options "
        "and they will be read from in order.",
    )
    group.add_argument(
        "--rank-stat-pdf-file",
        metavar="filename",
        help="Set the URL from which to load the ranking statistic PDF.  This is used "
        "to compute false-alarm probabilities and false-alarm rates and is required "
        "for online operation (when --data-source is arrakis, devshm, or "
        "white-realtime). It is forbidden for offline operation (all other data "
        "sources)",
    )
    group.add_argument(
        "--zerolag-rank-stat-pdf-file",
        metavar="filename",
        action="append",
        help="Record a histogram of the likelihood ratio ranking statistic values "
        "assigned to zero-lag candidates in this XML file.  This is used to construct "
        "the extinction model and set the overall false-alarm rate normalization "
        "during online running.  Counts will be added to the file's contents. "
        "Required when --data-source is arrakis, devshm, or white-realtime; forbidden "
        "otherwise. If given, exactly as many must be provided as there are --svd-bank "
        "options and they will be used in order.",
    )

    group = parser.add_argument_group("GracedB Options", "Adjust GracedB interaction")
    group.add_argument(
        "--gracedb-far-threshold",
        metavar="Hertz",
        type=float,
        default=-1,
        help="False-alarm rate threshold for gracedb uploads in Hertz (default = do not"
        "upload to gracedb).",
    )
    group.add_argument(
        "--gracedb-aggregator-far-threshold",
        metavar="Hertz",
        type=float,
        default=3.84e-07,
        help="False-alarm rate threshold for different aggregation methods for gracedb "
        "uploads in Hertz (default 1/month). Below threshold: max snr, above "
        "threshold: min far",
    )
    group.add_argument(
        "--gracedb-aggregator-far-trials-factor",
        metavar="Hertz",
        type=int,
        default=1,
        help="Trials factor for number of CBC pipelines uploading events to GraceDB. "
        "Default = 1",
    )
    group.add_argument(
        "--gracedb-group",
        metavar="name",
        default="Test",
        help="Gracedb group to which to upload events (default is Test).",
    )
    group.add_argument(
        "--gracedb-pipeline",
        metavar="name",
        default="SGNL",
        help="Name of pipeline to provide in GracedB uploads (default is SGNL).",
    )
    group.add_argument(
        "--gracedb-search",
        metavar="name",
        default="MOCK",
        help="Name of search to provide in GracedB uploads (default is MOCK).",
    )
    group.add_argument(
        "--gracedb-label",
        action="append",
        help="Labels to apply to gracedb uploads. Can be applied multiple times.",
    )
    group.add_argument(
        "--gracedb-service-url",
        metavar="url",
        help="Set the GraceDB service url.",
    )

    group = parser.add_argument_group("Program Behaviour")
    group.add_argument(
        "--torch-device",
        action="store",
        default="cpu",
        help="The device to run LLOID and Trigger generation on.",
    )
    group.add_argument(
        "--torch-dtype",
        action="store",
        type=str,
        default="float32",
        help="The data type to run LLOID and Trigger generation with.",
    )
    group.add_argument(
        "--use-gstlal-cpu-upsample",
        action="store_true",
        help="Use fast gstlal C implementation for upsampling when device is CPU. "
        "Provides ~6x speedup over PyTorch for CPU upsampling. Only effective when "
        "--torch-device=cpu.",
    )
    group.add_argument(
        "--injections",
        action="store_true",
        help="Whether to run this as an injection job. If data-source = 'frames', "
        "--injection-file must also be specified. Additionally, "
        "--output-likelihood-file must not be specified when --injections is set.",
    )
    group.add_argument(
        "--injection-file",
        metavar="filename",
        help="Set the name of the LIGO light-weight XML file from which to load "
        "injections. Required if --injections is set and data-source = 'frames'.",
    )
    group.add_argument(
        "--reconstruct-inj-segments",
        action="store_true",
        help="Whether to only recontruct around injection segments.",
    )
    group.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    group.add_argument(
        "--output-kafka-server",
        metavar="addr",
        help="Set the server address and port number for output data. Optional",
    )
    group.add_argument(
        "--analysis-tag",
        metavar="tag",
        default="test",
        help="Set the string to identify the analysis in which this job is part of. "
        'Used when --output-kafka-server is set. May not contain "." nor "-". Default '
        "is test.",
    )
    group.add_argument(
        "--job-tag",
        metavar="tag",
        default="",
        help="Set the string to identify this job and register the resources it"
        "provides on a node.  Should be 4 digits of the form 0001, 0002, etc.;  may not"
        'contain "." nor "-".',
    )
    group.add_argument(
        "--graph-name", metavar="filename", help="Plot pipieline graph to graph_name."
    )
    group.add_argument("--fake-sink", action="store_true", help="Connect to a NullSink")

    options = parser.parse_args()

    return options


def build_pipeline(
    data_source_info: DataSourceInfo,
    condition_info: ConditionInfo,
    svd_bank: List[str],
    aggregator_far_threshold: float = 3.84e-07,
    aggregator_far_trials_factor: int = 1,
    all_triggers_to_background: bool = False,
    analysis_tag: str = "test",
    coincidence_threshold: float = 0.005,
    compress_likelihood_ratio: bool = False,
    compress_likelihood_ratio_threshold: float = 0.03,
    event_config: str | None = None,
    fake_sink: bool = False,
    search: str | None = None,
    far_trials_factor: float = 1.0,
    gracedb_far_threshold: float = -1,
    gracedb_group: str = "Test",
    gracedb_label: list[str] | None = None,
    gracedb_pipeline: str = "SGNL",
    gracedb_search: str = "MOCK",
    gracedb_service_url: str | None = None,
    graph_name: str | None = None,
    impulse_bank: str | None = None,
    impulse_bankno: int | None = None,
    impulse_ifo: str | None = None,
    injections: bool = False,
    injection_file: str | None = None,
    input_likelihood_file: List[str] | None = None,
    job_tag: str = "",
    min_instruments_candidates: int = 1,
    nslice: int = -1,
    nsubbank_pretend: int | None = None,
    output_kafka_server: str | None = None,
    output_likelihood_file: List[str] | None = None,
    process_params: dict | None = None,
    rank_stat_pdf_file: str | None = None,
    reconstruct_inj_segments: bool = False,
    # snapshot_delay: float = 0,
    snapshot_interval: int = 14400,
    snapshot_multiprocess: bool = False,
    snr_min: float = 4,
    snr_timeseries_output: str | None = None,
    torch_device: str = "cpu",
    torch_dtype: str = "float32",
    trigger_output: List[str] | None = None,
    trigger_finding_duration: float = 1,
    use_gstlal_cpu_upsample: bool = False,
    verbose: bool = False,
    zerolag_rank_stat_pdf_file: List[str] | None = None,
    zero_latency: bool = False,
) -> Tuple[Pipeline, bool, OrderedDict]:
    #
    # Decide if we are online or offline
    #

    IS_ONLINE = data_source_info.data_source in [
        "arrakis",
        "devshm",
        "white-realtime",
        "gwdata-noise-realtime",
    ]
    if snapshot_multiprocess and not IS_ONLINE:
        raise ValueError("snapshot_multiprocess is only allowed for online mode")

    #
    # Sanity check
    #
    if trigger_output is not None:
        if IS_ONLINE:
            logger.warning(
                "warning: trigger_output will not be produced in online mode"
            )
        else:
            for fn in trigger_output:
                if os.path.exists(fn):
                    raise ValueError(f"output db exists: {fn}")

    if output_likelihood_file is not None and input_likelihood_file is None:
        for r in output_likelihood_file:
            if os.path.exists(r):
                raise ValueError(f"ranking stat output exists: {r}")

    if data_source_info.data_source == "impulse":
        if not impulse_bank:
            raise ValueError("Must specify impulse_bank when data_source='impulse'")
        elif impulse_bankno is None:
            raise ValueError("Must specify impulse_bankno when data_source='impulse'")
        elif not impulse_ifo:
            raise ValueError("Must specify impulse_ifo when data_source='impulse'")

    if min_instruments_candidates > 2:
        raise ValueError("min_instruments_candidates > 2 not supported")

    # check pytorch data type
    dtype_str = torch_dtype
    if dtype_str == "float64":
        dtype = torch.float64
    elif dtype_str == "float32":
        dtype = torch.float32
    elif dtype_str == "float16":
        dtype = torch.float16
    else:
        raise ValueError("Unknown data type")

    if (
        fake_sink is False
        and data_source_info.data_source
        not in [
            "arrakis",
            "devshm",
            "impulse",
            "white-realtime",
            "gwdata-noise-realtime",
        ]
    ) and trigger_output is None:
        raise ValueError(
            "Must supply trigger_output when fake_sink is False and "
            "data_source not in ['arrakis', 'devshm', 'impulse', "
            "'white-realtime', 'gwdata-noise-realtime']"
        )
    elif trigger_output is not None and event_config is None:
        raise ValueError("Must supply event_config when trigger_output is specified")

    # FIXME: put in check once we decide whether to put multiple banks in the same
    #        file or not
    # if output_likelihood_file is not None and len(output_likelihood_file) != len(
    #    trigger_output
    # ):
    #    raise ValueError(
    #        "must supply either none or exactly as many --output-likelihood-file "
    #        "options as --output"
    #    )

    if (injections and data_source_info.data_source == "frames") and not injection_file:
        raise ValueError(
            "Must supply --injection-file when --injections is set and "
            "data_source == 'frames'."
        )
    if injection_file and not injections:
        raise ValueError("Must supply --injections when --injection-file is set")

    if output_likelihood_file and injections:
        raise ValueError(
            "Must not set --output-likelihood-file when --injections is set"
        )

    if output_kafka_server is not None and not IS_ONLINE:
        raise ValueError("output_kafka_server can only be set if in online mode")

    SnapShotControl.snapshot_interval = snapshot_interval
    # SnapShotControl.delay = snapshot_delay
    if injections:
        SnapShotControl.startup_delay = 600

    #
    # Build pipeline
    #
    pipeline = Pipeline()

    # Create data source
    source_out_links, source_latency_links = datasource(
        pipeline=pipeline,
        info=data_source_info,
        source_latency=output_kafka_server is not None,
        verbose=verbose,
    )

    ifos = data_source_info.ifos

    # read in the svd banks
    banks = group_and_read_banks(
        svd_bank=svd_bank,
        source_ifos=ifos,
        nsubbank_pretend=nsubbank_pretend if nsubbank_pretend else 0,
        nslice=nslice,
        verbose=True,
    )

    # Choose to optionally reconstruct segments around injections
    if injection_file and reconstruct_inj_segments:
        offset_padding = max(
            math.ceil(abs(row.end)) + 2
            for bank in list(banks.values())[0]
            for row in bank.sngl_inspiral_table
        )
        reconstruction_segment_list = simulation.sim_inspiral_to_segment_list(
            injection_file, pad=offset_padding
        )
    else:
        reconstruction_segment_list = None

    if injection_file is not None:
        # read in injections
        xmldoc = ligolw_utils.load_filename(
            injection_file, verbose=verbose, contenthandler=LIGOLWContentHandler
        )
        sim_inspiral_table = lsctables.SimInspiralTable.get_table(xmldoc)

        # trim injection list to analysis segments
        injection_list = [
            inj
            for inj in sim_inspiral_table
            if inj.time_geocent in data_source_info.seg
        ]
    else:
        injection_list = None

    # sort and group the svd banks by sample rate
    sorted_bank = SortedBank(
        banks=banks,
        device=torch_device,
        dtype=dtype,
        nsubbank_pretend=nsubbank_pretend if nsubbank_pretend else 0,
        nslice=nslice,
        verbose=True,
    )
    bank_metadata = sorted_bank.bank_metadata

    template_maxrate = bank_metadata["maxrate"]

    #
    # Initialize strike data object
    #
    config = get_analysis_config()
    config = config[search] if search else config["default"]
    strike_object = StrikeObject(
        all_template_ids=sorted_bank.template_ids.numpy(),
        bankids_map=sorted_bank.bankids_map,
        coincidence_threshold=coincidence_threshold,
        chi2_over_snr2_min=config["chi2_over_snr2_min"],
        chi2_over_snr2_max=config["chi2_over_snr2_max"],
        chi_bin_min=config["chi_bin_min"],
        chi_bin_max=config["chi_bin_max"],
        chi_bin_num=config["chi_bin_num"],
        compress_likelihood_ratio=compress_likelihood_ratio,
        compress_likelihood_ratio_threshold=compress_likelihood_ratio_threshold,
        FAR_trialsfactor=far_trials_factor,
        ifos=data_source_info.all_analysis_ifos,
        injections=injections,
        input_likelihood_file=input_likelihood_file,
        is_online=IS_ONLINE,
        min_instruments=min_instruments_candidates,
        output_likelihood_file=output_likelihood_file,
        rank_stat_pdf_file=rank_stat_pdf_file,
        verbose=verbose,
        zerolag_rank_stat_pdf_file=zerolag_rank_stat_pdf_file,
        nsubbank_pretend=bool(nsubbank_pretend),
        dtype=dtype,
        device=torch_device,
    )

    # Condition the data source if not doing an impulse test
    if data_source_info.data_source != "impulse":
        condition_out_links, spectrum_out_links, whiten_latency_out_links = condition(
            pipeline=pipeline,
            condition_info=condition_info,
            ifos=ifos,
            data_source=data_source_info.data_source,
            input_sample_rate=data_source_info.input_sample_rate,
            input_links=source_out_links,
            whiten_sample_rate=template_maxrate,
            whiten_latency=output_kafka_server is not None,
            highpass_filter=config["highpass_filter"],
            zero_latency=zero_latency,
        )
    else:
        spectrum_out_links = None
        condition_out_links = source_out_links

    # connect LLOID
    lloid_output_source_link = lloid(
        pipeline,
        sorted_bank,
        condition_out_links,
        nslice,
        torch_device,
        dtype,
        reconstruction_segment_list,
        use_gstlal_cpu_upsample,
    )
    if output_kafka_server is not None:
        for ifo, link in lloid_output_source_link.items():
            pipeline.insert(
                Latency(
                    name=ifo + "_snrSlice_latency",
                    sink_pad_names=("data",),
                    source_pad_names=("latency",),
                    route=ifo + "_snrSlice_latency",
                    interval=1,
                ),
                link_map={ifo + "_snrSlice_latency:snk:data": link},
            )

    # make the sink
    if data_source_info.data_source == "impulse":
        # Note: impulse_ifo, impulse_bank, and impulse_bankno are validated
        # to be non-None/non-empty at lines 468-474
        assert impulse_ifo is not None
        assert impulse_bank is not None
        assert impulse_bankno is not None
        pipeline.insert(
            ImpulseSink(
                name="imsink0",
                sink_pad_names=tuple(ifos) + (impulse_ifo + "_src",),
                original_templates=impulse_bank,
                template_duration=141,
                plotname="response",
                impulse_pad=impulse_ifo + "_src",
                data_pad=impulse_ifo,
                bankno=impulse_bankno,
                verbose=verbose,
            ),
        )
        # link output of lloid
        for ifo, link in lloid_output_source_link.items():
            pipeline.insert(
                link_map={
                    "imsink0:snk:" + ifo: link,
                }
            )
            if ifo == impulse_ifo:
                pipeline.insert(
                    link_map={"imsink0:snk:" + ifo + "_src": condition_out_links[ifo]}
                )
    else:
        #
        # connect itacacac
        #

        if output_likelihood_file is not None or (IS_ONLINE and injections):
            strike_pad: str = "strike"
            itacacac_pads: tuple[str, ...] = ("stillsuit", strike_pad)
        else:
            strike_pad = ""  # type: ignore[assignment]
            itacacac_pads = ("stillsuit",)

        pipeline.insert(
            Itacacac(
                name="Itacacac",
                sink_pad_names=tuple(ifos),
                sample_rate=template_maxrate,
                trigger_finding_duration=trigger_finding_duration,
                snr_min=snr_min,
                autocorrelation_banks=sorted_bank.autocorrelation_banks,
                autocorrelation_length_mask=sorted_bank.autocorrelation_length_mask,
                autocorrelation_lengths=sorted_bank.autocorrelation_lengths,
                template_ids=sorted_bank.template_ids,
                bankids_map=sorted_bank.bankids_map,
                end_time_delta=sorted_bank.end_time_delta,
                template_durations=sorted_bank.template_durations,
                device=torch_device,
                coincidence_threshold=coincidence_threshold,
                min_instruments_candidates=min_instruments_candidates,
                all_triggers_to_background=all_triggers_to_background,
                stillsuit_pad="stillsuit",
                strike_pad=strike_pad,
                is_online=IS_ONLINE,
            ),
        )
        for ifo in ifos:
            if snr_timeseries_output:
                outname = os.path.join(
                    os.path.dirname(snr_timeseries_output),
                    ifo + "_" + os.path.basename(snr_timeseries_output),
                )
                pipeline.insert(
                    DumpSeriesSink(
                        name=ifo + "DumpSNR",
                        sink_pad_names=(ifo,),
                        fname=outname,
                        verbose=verbose,
                    ),
                    link_map={
                        "Itacacac:snk:" + ifo: lloid_output_source_link[ifo],
                        ifo + "DumpSNR:snk:" + ifo: lloid_output_source_link[ifo],
                    },
                )
            else:
                pipeline.insert(
                    link_map={
                        "Itacacac:snk:" + ifo: lloid_output_source_link[ifo],
                    }
                )

        if IS_ONLINE:
            #
            # connect LR and FAR assignment
            #
            pipeline.insert(
                StrikeTransform(
                    name="StrikeTransform",
                    sink_pad_names=("trigs",),
                    source_pad_names=("trigs",),
                    strike_object=strike_object,
                ),
                link_map={
                    "StrikeTransform:snk:trigs": "Itacacac:src:stillsuit",
                },
            )
            if output_kafka_server is not None:
                gracedb_sink = GraceDBSink(
                    name="gracedb",
                    event_pad="event",
                    spectrum_pads=tuple(ifos),
                    template_sngls=sorted_bank.sngls,
                    analysis_ifos=ifos,
                    process_params=process_params,
                    output_kafka_server=output_kafka_server,
                    far_thresh=gracedb_far_threshold,
                    aggregator_far_thresh=aggregator_far_threshold,
                    aggregator_far_trials_factor=aggregator_far_trials_factor,
                    gracedb_group=gracedb_group,
                    gracedb_pipeline=gracedb_pipeline,
                    gracedb_search=gracedb_search,
                    gracedb_label=gracedb_label,
                    gracedb_service_url=gracedb_service_url,
                    analysis_tag=analysis_tag,
                    job_type=job_tag.split("_")[-1] if job_tag else "",
                    delta_t=coincidence_threshold,
                    strike_object=strike_object,
                    channel_dict=data_source_info.channel_dict,
                    autocorrelation_lengths=sorted_bank.autocorrelation_lengths,
                )
                pipeline.insert(
                    Latency(
                        name="ItacacacLatency",
                        sink_pad_names=("data",),
                        source_pad_names=("latency",),
                        route="all_itacacac_latency",
                        interval=1,
                    ),
                    gracedb_sink,
                    EyeCandy(
                        name="EyeCandy",
                        source_pad_names=("trigs",),
                        template_sngls=sorted_bank.sngls,
                        event_pad="trigs",
                        state_vector_pads={ifo: "state_vector_" + ifo for ifo in ifos},
                        ht_gate_pads={ifo: "ht_gate_" + ifo for ifo in ifos},
                    ),
                    link_map={
                        "ItacacacLatency:snk:data": "Itacacac:src:stillsuit",
                        "gracedb:snk:event": "StrikeTransform:src:trigs",
                        "EyeCandy:snk:trigs": "StrikeTransform:src:trigs",
                    },
                )
                pipeline.insert(
                    link_map={
                        "gracedb:snk:" + ifo: spectrum_out_links[ifo] for ifo in ifos
                    }
                )
                pipeline.insert(
                    link_map={
                        "EyeCandy:snk:state_vector_" + ifo: source_out_links[ifo]
                        for ifo in ifos
                    }
                )
                pipeline.insert(
                    link_map={
                        "EyeCandy:snk:ht_gate_" + ifo: condition_out_links[ifo]
                        for ifo in ifos
                    }
                )

        # Connect sink
        if fake_sink:
            pipeline.insert(
                NullSink(
                    name="Sink",
                    sink_pad_names=itacacac_pads,
                ),
                link_map={
                    "Sink:snk:" + snk: "Itacacac:src:" + snk for snk in itacacac_pads
                },
            )
            for ifo in ifos:
                pipeline.insert(
                    NullSink(
                        name="Null_" + ifo,
                        sink_pad_names=(ifo,),
                    ),
                    link_map={
                        "Null_" + ifo + ":snk:" + ifo: spectrum_out_links[ifo],
                    },
                )
        elif event_config is not None:
            #
            # StillSuit - trigger output
            #
            stillsuit_sink = StillSuitSink(
                name="StillSuitSnk",
                sink_pad_names=("trigs",) + tuple(["segments_" + ifo for ifo in ifos]),
                ifos=ifos,
                config_name=event_config,
                bankids_map=sorted_bank.bankids_map,
                trigger_output=(
                    OrderedDict(
                        (k, trigger_output[i])
                        for i, k in enumerate(sorted_bank.bankids_map.keys())
                    )
                    if trigger_output
                    else None  # type: ignore[arg-type]
                ),
                template_ids=sorted_bank.template_ids.numpy(),
                template_sngls=sorted_bank.sngls,
                subbankids=sorted_bank.subbankids,
                itacacac_pad_name="trigs",
                segments_pad_map={"segments_" + ifo: ifo for ifo in ifos},
                process_params=process_params,
                program="sgnl-inspiral",
                injection_list=injection_list,
                is_online=IS_ONLINE,
                multiprocess=snapshot_multiprocess,
                nsubbank_pretend=bool(nsubbank_pretend),
                injections=injections,
                strike_object=strike_object,  # type: ignore[arg-type]
                queue_maxsize=0,
            )
            pipeline.insert(stillsuit_sink)

            for ifo in ifos:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:snk:segments_" + ifo: condition_out_links[ifo],
                    }
                )

            if IS_ONLINE:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:snk:trigs": "StrikeTransform:src:trigs",
                    },
                )
            else:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:snk:trigs": "Itacacac:src:stillsuit",
                    },
                )

            #
            # Strike - background output
            #
            # FIXME: update online injection logic, StrikeSink is needed only for
            # updating files on snapshot
            if output_likelihood_file is not None or (IS_ONLINE and injections):
                strike_sink = StrikeSink(
                    name="StrikeSnk",
                    ifos=data_source_info.all_analysis_ifos,
                    strike_object=strike_object,
                    is_online=IS_ONLINE,
                    multiprocess=snapshot_multiprocess,
                    injections=injections,
                    bankids_map=sorted_bank.bankids_map,
                    background_pad="trigs",
                    horizon_pads=["horizon_" + ifo for ifo in ifos],
                    queue_maxsize=0,
                )
                pipeline.insert(
                    strike_sink,
                    link_map={
                        "StrikeSnk:snk:trigs": "Itacacac:src:strike",
                    },
                )
                for ifo in ifos:
                    pipeline.insert(
                        HorizonDistanceTracker(
                            name=ifo + "_Horizon",
                            source_pad_names=(ifo,),
                            sink_pad_names=(ifo,),
                            horizon_distance_funcs=sorted_bank.horizon_distance_funcs,
                            ifo=ifo,
                        ),
                        link_map={
                            ifo + "_Horizon:snk:" + ifo: spectrum_out_links[ifo],
                        },
                    )
                    pipeline.insert(
                        link_map={
                            "StrikeSnk:snk:horizon_" + ifo: ifo + "_Horizon:src:" + ifo,
                        }
                    )
            else:
                pipeline.insert(
                    NullSink(
                        name="NullSink",
                        sink_pad_names=ifos,
                    ),
                    link_map={
                        "NullSink:snk:" + ifo: spectrum_out_links[ifo] for ifo in ifos
                    },
                )

            if output_kafka_server is not None:
                #
                # Kafka Sink
                #
                pipeline.insert(
                    KafkaSink(
                        name="KafkaSnk",
                        sink_pad_names=(
                            "itacacac_latency",
                            "eyecandy_kafka",
                        )
                        + tuple(ifo + "_datasource_latency" for ifo in ifos)
                        + tuple(ifo + "_whiten_latency" for ifo in ifos)
                        + tuple(ifo + "_snrSlice_latency" for ifo in ifos),
                        output_kafka_server=output_kafka_server,
                        time_series_topics=[
                            "latency_history",
                            "snr_history",
                            "all_itacacac_latency",
                            "likelihood_history",
                            "far_history",
                            "ram_history",
                            "uptime",
                        ]
                        + [
                            ifo + topic
                            for ifo in ifos
                            for topic in [
                                "_snr_history",
                                "_datasource_latency",
                                "_whitening_latency",
                                "_snrSlice_latency",
                                "_statevectorsegments",
                                "_afterhtgatesegments",
                            ]
                        ],
                        trigger_topics=["coinc"],
                        tag=job_tag,
                        prefix="sgnl."
                        + analysis_tag
                        + "."
                        + ("inj_" if job_tag and "_inj" in job_tag else ""),
                        interval=3,
                    ),
                    link_map={
                        "KafkaSnk:snk:itacacac_latency": "ItacacacLatency:src:latency",
                        "KafkaSnk:snk:eyecandy_kafka": "EyeCandy:src:trigs",
                    },
                )
                for ifo in ifos:
                    pipeline.insert(
                        link_map={
                            "KafkaSnk:snk:"
                            + ifo
                            + "_whiten_latency": whiten_latency_out_links[ifo],
                            "KafkaSnk:snk:"
                            + ifo
                            + "_datasource_latency": source_latency_links[ifo],
                            "KafkaSnk:snk:"
                            + ifo
                            + "_snrSlice_latency": ifo
                            + "_snrSlice_latency:src:latency",
                        }
                    )

    return pipeline, IS_ONLINE, banks

def inspiral(
    data_source_info: DataSourceInfo,
    condition_info: ConditionInfo,
    svd_bank: List[str],
    aggregator_far_threshold: float = 3.84e-07,
    aggregator_far_trials_factor: int = 1,
    all_triggers_to_background: bool = False,
    analysis_tag: str = "test",
    coincidence_threshold: float = 0.005,
    compress_likelihood_ratio: bool = False,
    compress_likelihood_ratio_threshold: float = 0.03,
    event_config: str | None = None,
    fake_sink: bool = False,
    search: str | None = None,
    far_trials_factor: float = 1.0,
    gracedb_far_threshold: float = -1,
    gracedb_group: str = "Test",
    gracedb_label: list[str] | None = None,
    gracedb_pipeline: str = "SGNL",
    gracedb_search: str = "MOCK",
    gracedb_service_url: str | None = None,
    graph_name: str | None = None,
    impulse_bank: str | None = None,
    impulse_bankno: int | None = None,
    impulse_ifo: str | None = None,
    injections: bool = False,
    injection_file: str | None = None,
    input_likelihood_file: List[str] | None = None,
    job_tag: str = "",
    min_instruments_candidates: int = 1,
    nslice: int = -1,
    nsubbank_pretend: int | None = None,
    output_kafka_server: str | None = None,
    output_likelihood_file: List[str] | None = None,
    process_params: dict | None = None,
    rank_stat_pdf_file: str | None = None,
    reconstruct_inj_segments: bool = False,
    # snapshot_delay: float = 0,
    snapshot_interval: int = 14400,
    snapshot_multiprocess: bool = False,
    snr_min: float = 4,
    snr_timeseries_output: str | None = None,
    torch_device: str = "cpu",
    torch_dtype: str = "float32",
    trigger_output: List[str] | None = None,
    trigger_finding_duration: float = 1,
    use_gstlal_cpu_upsample: bool = False,
    verbose: bool = False,
    zerolag_rank_stat_pdf_file: List[str] | None = None,
    zero_latency: bool = False,
    ):
    # Make pipeline properly
    pipeline, IS_ONLINE, banks = build_pipeline(
        data_source_info=data_source_info,
        condition_info=condition_info,
        svd_bank=svd_bank,
        aggregator_far_threshold=aggregator_far_threshold,
        aggregator_far_trials_factor=aggregator_far_trials_factor,
        all_triggers_to_background=all_triggers_to_background,
        analysis_tag=analysis_tag,
        coincidence_threshold=coincidence_threshold,
        compress_likelihood_ratio=compress_likelihood_ratio,
        compress_likelihood_ratio_threshold=compress_likelihood_ratio_threshold,
        event_config=event_config,
        fake_sink=fake_sink,
        search=search,
        far_trials_factor=far_trials_factor,
        gracedb_far_threshold=gracedb_far_threshold,
        gracedb_group=gracedb_group,
        gracedb_label=gracedb_label,
        gracedb_pipeline=gracedb_pipeline,
        gracedb_search=gracedb_search,
        gracedb_service_url=gracedb_service_url,
        graph_name=graph_name,
        impulse_bank=impulse_bank,
        impulse_bankno=impulse_bankno,
        impulse_ifo=impulse_ifo,
        injections=injections,
        injection_file=injection_file,
        input_likelihood_file=input_likelihood_file,
        job_tag=job_tag,
        min_instruments_candidates=min_instruments_candidates,
        nslice=nslice,
        nsubbank_pretend=nsubbank_pretend,
        output_kafka_server=output_kafka_server,
        output_likelihood_file=output_likelihood_file,
        process_params=process_params,
        rank_stat_pdf_file=rank_stat_pdf_file,
        reconstruct_inj_segments=reconstruct_inj_segments,
            # snapshot_delay: float = 0,
        snapshot_interval=snapshot_interval,
        snapshot_multiprocess=snapshot_multiprocess,
        snr_min=snr_min,
        snr_timeseries_output=snr_timeseries_output,
        torch_device=torch_device,
        torch_dtype=torch_dtype,
        trigger_output=trigger_output,
        trigger_finding_duration=trigger_finding_duration,
        use_gstlal_cpu_upsample=use_gstlal_cpu_upsample,
        verbose=verbose,
        zerolag_rank_stat_pdf_file=zerolag_rank_stat_pdf_file,
        zero_latency=zero_latency,
    )


    # Plot pipeline
    if graph_name:
        pipeline.visualize(graph_name)  # type: ignore[call-arg]

    # Run pipeline
    if IS_ONLINE:
        if job_tag and re.match(r"^\d{4}", job_tag):
            if injections:
                HTTPControl.port = int("6%s" % job_tag[:4])
            else:
                HTTPControl.port = int("5%s" % job_tag[:4])
        if analysis_tag:
            HTTPControl.tag = analysis_tag  # type: ignore[assignment]
        registry_file = "%s_registry.txt" % job_tag
        with SnapShotControl(registry_file=registry_file):
            pipeline.run()
    else:
        pipeline.run()

    #
    # Cleanup template bank temp files
    #

    print("shutdown: template bank cleanup", flush=True, file=sys.stderr)
    if nsubbank_pretend:
        for ifo in banks:
            bank = banks[ifo][0]
            if verbose:
                print("removing file: ", bank.template_bank_filename, file=sys.stderr)
            os.remove(bank.template_bank_filename)
    else:
        for ifo in banks:
            for bank in banks[ifo]:
                if verbose:
                    print(
                        "removing file: ", bank.template_bank_filename, file=sys.stderr
                    )
                os.remove(bank.template_bank_filename)

    print("shutdown: del bank", flush=True, file=sys.stderr)
    del bank


def main():
    # parse arguments
    options = parse_command_line()

    data_source_info = DataSourceInfo.from_options(options)
    condition_info = ConditionInfo.from_options(options)

    process_params = options.__dict__.copy()

    inspiral(
        data_source_info=data_source_info,
        condition_info=condition_info,
        svd_bank=options.svd_bank,
        aggregator_far_threshold=options.gracedb_aggregator_far_threshold,
        aggregator_far_trials_factor=options.gracedb_aggregator_far_trials_factor,
        all_triggers_to_background=options.all_triggers_to_background,
        analysis_tag=options.analysis_tag,
        coincidence_threshold=options.coincidence_threshold,
        compress_likelihood_ratio=options.compress_likelihood_ratio,
        compress_likelihood_ratio_threshold=options.compress_likelihood_ratio_threshold,
        event_config=options.event_config,
        fake_sink=options.fake_sink,
        search=options.search,
        far_trials_factor=options.far_trials_factor,
        gracedb_far_threshold=options.gracedb_far_threshold,
        gracedb_group=options.gracedb_group,
        gracedb_label=options.gracedb_label,
        gracedb_pipeline=options.gracedb_pipeline,
        gracedb_search=options.gracedb_search,
        gracedb_service_url=options.gracedb_service_url,
        graph_name=options.graph_name,
        impulse_bank=options.impulse_bank,
        impulse_bankno=options.impulse_bankno,
        impulse_ifo=options.impulse_ifo,
        injections=options.injections,
        injection_file=options.injection_file,
        input_likelihood_file=options.input_likelihood_file,
        job_tag=options.job_tag,
        min_instruments_candidates=options.min_instruments_candidates,
        nsubbank_pretend=options.nsubbank_pretend,
        nslice=options.nslice,
        output_kafka_server=options.output_kafka_server,
        output_likelihood_file=options.output_likelihood_file,
        process_params=process_params,
        rank_stat_pdf_file=options.rank_stat_pdf_file,
        reconstruct_inj_segments=options.reconstruct_inj_segments,
        # snapshot_delay=options.snapshot_delay,
        snapshot_interval=options.snapshot_interval,
        snapshot_multiprocess=options.snapshot_multiprocess,
        snr_min=options.snr_min,
        snr_timeseries_output=options.snr_timeseries_output,
        torch_device=options.torch_device,
        torch_dtype=options.torch_dtype,
        trigger_finding_duration=options.trigger_finding_duration,
        trigger_output=options.trigger_output,
        use_gstlal_cpu_upsample=options.use_gstlal_cpu_upsample,
        verbose=options.verbose,
        zerolag_rank_stat_pdf_file=options.zerolag_rank_stat_pdf_file,
        zero_latency=options.zero_latency,
    )


if __name__ == "__main__":
    main()
