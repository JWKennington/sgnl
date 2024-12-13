from __future__ import annotations

import math
import os
import sys
from argparse import ArgumentParser
from typing import List

import torch
from ligo.lw import ligolw, lsctables
from ligo.lw import utils as ligolw_utils
from sgn.apps import Pipeline
from sgn.sinks import NullSink
from sgnligo.sinks import KafkaSink
from sgnligo.sources import DataSourceInfo, datasource
from sgnligo.transforms import ConditionInfo, Latency, condition

from sgnl import simulation
from sgnl.sinks import ImpulseSink, StillSuitSink, StrikeSink
from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnl.transforms import HorizonDistanceTracker, Itacacac, StrikeTransform, lloid


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
        action="store",
        help="Set the name of the sqlite output file *.sqlite",
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
        "--output-likelihood-file",
        metavar="filename",
        action="append",
        help="Set the name of the LIKELIHOOD_RATIO file to which to write likelihood "
        "ratio data collected from triggers (optional).  Can be given more than once. "
        "If given, exactly as many must be provided as there are --svd-bank options "
        "and they will be writen to in order.",
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
        "--graph-name", metavar="filename", help="Plot pipieline graph to graph_name."
    )
    group.add_argument("--fake-sink", action="store_true", help="Connect to a NullSink")

    options = parser.parse_args()

    return options


def inspiral(
    data_source_info: DataSourceInfo,
    condition_info: ConditionInfo,
    svd_bank: List[str],
    trigger_finding_duration: float = 1,
    snr_min: float = 4,
    coincidence_threshold: float = 0.005,
    event_config: str = None,
    trigger_output: str = None,
    ranking_stat_output: List[str] = None,
    torch_dtype: str = "float32",
    torch_device: str = "cpu",
    injections: bool = False,
    injection_file: str = None,
    reconstruct_inj_segments: bool = False,
    analysis_tag: str = "test",
    output_kafka_server: str = None,
    fake_sink: bool = False,
    verbose: bool = False,
    graph_name: str = None,
    impulse_bank: str = None,
    impulse_bankno: int = None,
    impulse_ifo: str = None,
    nsubbank_pretend: int = None,
    nslice: int = -1,
    process_params: dict = None,
):
    #
    # Sanity check
    #

    if trigger_output is not None and os.path.exists(trigger_output):
        raise ValueError("output db exists")
    if ranking_stat_output is not None:
        for r in ranking_stat_output:
            if os.path.exists(r):
                raise ValueError("ranking stat output exists")

    if data_source_info.data_source == "impulse":
        if not impulse_bank:
            raise ValueError("Must specify impulse_bank when data_source='impulse'")
        elif impulse_bankno is None:
            raise ValueError("Must specify impulse_bankno when data_source='impulse'")
        elif not impulse_ifo:
            raise ValueError("Must specify impulse_ifo when data_source='impulse'")

    # check pytorch data type
    dtype = torch_dtype
    if dtype == "float64":
        dtype = torch.float64
    elif dtype == "float32":
        dtype = torch.float32
    elif dtype == "float16":
        dtype = torch.float16
    else:
        raise ValueError("Unknown data type")

    if (
        fake_sink is False and data_source_info.data_source not in ["devshm", "impulse"]
    ) and trigger_output is None:
        raise ValueError(
            "Must supply trigger_output when fake_sink is False and "
            "data_source != 'devshm' or 'impulse'"
        )
    elif trigger_output is not None and event_config is None:
        raise ValueError("Must supply event_config when trigger_output is specified")

    # FIXME: put in check once we decide whether to put multiple banks in the same
    #        file or not
    # if ranking_stat_output is not None and len(ranking_stat_output) != len(
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
    # if (
    #     likelihood_snapshot_interval and not ranking_stat_output
    # ) and not injections:
    #     raise ValueError(
    #         "must set --output-likelihood-file when --likelihood-snapshot-interval is"
    #         " set"
    #     )
    if ranking_stat_output and injections:
        raise ValueError(
            "Must not set --output-likelihood-file when --injections is set"
        )

    #
    # Build pipeline
    #
    pipeline = Pipeline()

    # Create data source
    source_out_links = datasource(
        pipeline=pipeline,
        info=data_source_info,
        verbose=verbose,
    )

    ifos = data_source_info.ifos

    # read in the svd banks
    banks = group_and_read_banks(
        svd_bank=svd_bank,
        source_ifos=ifos,
        nsubbank_pretend=nsubbank_pretend,
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
        nsubbank_pretend=nsubbank_pretend,
        nslice=nslice,
        verbose=True,
    )
    bank_metadata = sorted_bank.bank_metadata

    template_maxrate = bank_metadata["maxrate"]

    # Condition the data source if not doing an impulse test
    if data_source_info.data_source != "impulse":
        source_out_links, spectrum_out_links, whiten_latency_out_links = condition(
            pipeline=pipeline,
            condition_info=condition_info,
            ifos=ifos,
            data_source=data_source_info.data_source,
            input_sample_rate=data_source_info.input_sample_rate,
            input_links=source_out_links,
            whiten_latency=data_source_info.data_source == "devshm",
        )
    else:
        spectrum_out_links = None

    # connect LLOID
    lloid_output_source_link = lloid(
        pipeline,
        sorted_bank,
        source_out_links,
        nslice,
        torch_device,
        dtype,
        reconstruction_segment_list,
    )

    # make the sink
    if data_source_info.data_source == "impulse":
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
                    "imsink0:sink:" + ifo: link,
                }
            )
            if ifo == impulse_ifo:
                pipeline.insert(
                    link_map={"imsink0:sink:" + ifo + "_src": source_out_links[ifo]}
                )
    else:
        # connect itacacac

        itacacac_pads = ("stillsuit",)
        if ranking_stat_output is not None:
            strike_pad = "strike"
            itacacac_pads += (strike_pad,)
        else:
            strike_pad = None
        if data_source_info.data_source == "devshm":
            kafka_pad = "kafka"
            itacacac_pads += (kafka_pad,)
        else:
            kafka_pad = None

        pipeline.insert(
            Itacacac(
                name="Itacacac",
                sink_pad_names=tuple(ifos),
                sample_rate=template_maxrate,
                trigger_finding_duration=trigger_finding_duration,
                snr_min=snr_min,
                autocorrelation_banks=sorted_bank.autocorrelation_banks,
                template_ids=sorted_bank.template_ids,
                bankids_map=sorted_bank.bankids_map,
                end_time_delta=sorted_bank.end_time_delta,
                device=torch_device,
                coincidence_threshold=coincidence_threshold,
                stillsuit_pad="stillsuit",
                strike_pad=strike_pad,
                kafka_pad=kafka_pad,
            ),
        )
        for ifo in ifos:
            pipeline.insert(
                link_map={
                    "Itacacac:sink:" + ifo: lloid_output_source_link[ifo],
                }
            )
        if data_source_info.data_source == "devshm":
            pipeline.insert(
                Latency(
                    name="ItacacacLatency",
                    sink_pad_names=("data",),
                    source_pad_names=("latency",),
                    route="all_itacacac_latency",
                ),
                link_map={"ItacacacLatency:sink:data": "Itacacac:src:" + kafka_pad},
            )

        # Connect sink
        if fake_sink:
            pipeline.insert(
                NullSink(
                    name="Sink",
                    sink_pad_names=itacacac_pads,
                ),
                link_map={
                    "Sink:sink:" + snk: "Itacacac:src:" + snk for snk in itacacac_pads
                },
            )
            for ifo in ifos:
                pipeline.insert(
                    NullSink(
                        name="Null_" + ifo,
                        sink_pad_names=(ifo,),
                    ),
                    link_map={
                        "Null_" + ifo + ":sink:" + ifo: spectrum_out_links[ifo],
                    },
                )
        elif event_config is not None:
            if data_source_info.data_source == "devshm":
                #
                # Kafka Sink
                #
                pipeline.insert(
                    KafkaSink(
                        name="KafkaSnk",
                        sink_pad_names=(
                            "kafka",
                            "itacacac_latency",
                        )
                        + tuple(ifo + "_whiten_latency" for ifo in ifos),
                        output_kafka_server=output_kafka_server,
                        topics=[
                            "gstlal." + analysis_tag + "." + ifo + "_snr_history"
                            for ifo in ifos
                        ]
                        + [
                            "gstlal." + analysis_tag + ".latency_history_max",
                        ]
                        + [
                            "gstlal." + analysis_tag + ".latency_history_median",
                        ]
                        + [
                            "gstlal." + analysis_tag + ".all_itacacac_latency",
                        ]
                        + [
                            "gstlal." + analysis_tag + "." + ifo + "_whitening_latency"
                            for ifo in ifos
                        ],
                    ),
                    link_map={
                        "KafkaSnk:sink:kafka": "Itacacac:src:" + kafka_pad,
                        "KafkaSnk:sink:itacacac_latency": "ItacacacLatency:src:latency",
                    },
                )
                for ifo in ifos:
                    pipeline.insert(
                        link_map={
                            "KafkaSnk:sink:"
                            + ifo
                            + "_whiten_latency": whiten_latency_out_links[ifo],
                        }
                    )
            #
            # StillSuit - trigger output
            #
            pipeline.insert(
                StillSuitSink(
                    name="StillSuitSnk",
                    sink_pad_names=("trigs",)
                    + tuple(["segments_" + ifo for ifo in ifos]),
                    ifos=ifos,
                    config_name=event_config,
                    trigger_output=trigger_output,
                    template_ids=sorted_bank.template_ids.numpy(),
                    template_sngls=sorted_bank.sngls,
                    subbankids=sorted_bank.subbankids,
                    itacacac_pad_name="trigs",
                    segments_pad_map={"segments_" + ifo: ifo for ifo in ifos},
                    process_params=process_params,
                    program="sgnl-inspiral",
                    injection_list=injection_list,
                    is_online=data_source_info == "devshm",
                    nsubbank_pretend=bool(nsubbank_pretend),
                ),
            )
            if data_source_info.data_source in ["devshm", "white-realtime"]:
                # connect LR and FAR assignment
                pipeline.insert(
                    StrikeTransform(
                        name="StrikeTransform",
                        sink_pad_names=("trigs",),
                        source_pad_names=("trigs",),
                    ),
                    link_map={
                        "StrikeTransform:sink:trigs": "Itacacac:src:stillsuit",
                        "StillSuitSnk:sink:trigs": "StrikeTransform:src:trigs",
                    },
                )
            else:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:sink:trigs": "Itacacac:src:stillsuit",
                    },
                )

            for ifo in ifos:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:sink:segments_" + ifo: source_out_links[ifo],
                    }
                )

            #
            # Strike - background output
            #
            if ranking_stat_output is not None:
                pipeline.insert(
                    StrikeSink(
                        name="StrikeSnk",
                        ifos=data_source_info.all_analysis_ifos,
                        all_template_ids=sorted_bank.template_ids.numpy(),
                        bankids_map=sorted_bank.bankids_map,
                        ranking_stat_output=ranking_stat_output,
                        coincidence_threshold=coincidence_threshold,
                        background_pad="trigs",
                        horizon_pads=["horizon_" + ifo for ifo in ifos],
                    ),
                    link_map={
                        "StrikeSnk:sink:trigs": "Itacacac:src:strike",
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
                            ifo + "_Horizon:sink:" + ifo: spectrum_out_links[ifo],
                        },
                    )
                    pipeline.insert(
                        link_map={
                            "StrikeSnk:sink:horizon_"
                            + ifo: ifo
                            + "_Horizon:src:"
                            + ifo,
                        }
                    )
            else:
                pipeline.insert(
                    NullSink(
                        name="NullSink",
                        sink_pad_names=ifos,
                    ),
                    link_map={
                        "NullSink:sink:" + ifo: spectrum_out_links[ifo] for ifo in ifos
                    },
                )

    # Plot pipeline
    if graph_name:
        pipeline.visualize(graph_name)

    # Run pipeline
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
        trigger_finding_duration=options.trigger_finding_duration,
        snr_min=options.snr_min,
        coincidence_threshold=options.coincidence_threshold,
        event_config=options.event_config,
        trigger_output=options.trigger_output,
        ranking_stat_output=options.output_likelihood_file,
        torch_dtype=options.torch_dtype,
        torch_device=options.torch_device,
        injections=options.injections,
        injection_file=options.injection_file,
        reconstruct_inj_segments=options.reconstruct_inj_segments,
        analysis_tag=options.analysis_tag,
        output_kafka_server=options.output_kafka_server,
        fake_sink=options.fake_sink,
        verbose=options.verbose,
        graph_name=options.graph_name,
        impulse_bank=options.impulse_bank,
        impulse_bankno=options.impulse_bankno,
        impulse_ifo=options.impulse_ifo,
        nsubbank_pretend=options.nsubbank_pretend,
        nslice=options.nslice,
        process_params=process_params,
    )


if __name__ == "__main__":
    main()
