from __future__ import annotations

import os
import sys
from argparse import ArgumentParser
from typing import List

import torch
from sgn.apps import Pipeline
from sgn.sinks import NullSink
from sgnligo.sinks import KafkaSink
from sgnligo.sources import datasource, parse_command_line_datasource
from sgnligo.transforms import (  # Latency,
    HorizonDistance,
    condition,
    parse_command_line_condition,
)

from sgnl.sinks import ImpulseSink, StillSuitSink
from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnl.transforms import Itacacac, lloid


def parse_command_line(parser=None):
    if parser is None:
        parser = ArgumentParser()

    group = parser.add_argument_group(
        "Trigger Generator", "Adjust trigger generator behaviour"
    )
    group.add_argument(
        "--svd-bank",
        metavar="filename",
        action="append",
        default=[],
        help="Set the name of the LIGO light-weight XML file from which to load the "
        "svd bank for a given instrument.  To analyze multiple instruments, "
        "--svd-bank can be called multiple times for svd banks corresponding "
        "to different instruments.  If --data-source is lvshm or framexmit, "
        "then only svd banks corresponding to a single bin must be given. "
        "If given multiple times, the banks will be processed one-by-one, in "
        "order.  At least one svd bank for at least 2 detectors is required, "
        "but see also --svd-bank-cache.",
    )
    group.add_argument(
        "--nbank-pretend",
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
    group.add_argument(
        "--trigger-finding-duration",
        type=float,
        metavar="seconds",
        action="store",
        default=1,
        help="Produce triggers in blocks of this duration.",
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
        "--trigger-output",
        metavar="filename",
        action="store",
        help="Set the name of the sqlite output file *.sqlite",
    )
    group.add_argument(
        "--event-config",
        metavar="filename",
        action="store",
        help="Set the name of the config yaml file for event buffers",
    )

    group = parser.add_argument_group(
        "Ranking Statistic Options", "Adjust ranking statistic behaviour"
    )
    group.add_argument(
        "--ranking-stat-output",
        metavar="filename",
        action="append",
        default=[],
        help="Set the name of the file to which to write ranking statistic data "
        "collected from triggers (optional).  Can be given more than once.  If given, "
        "exactly as many must be provided as there are --svd-bank options and they will"
        " be writen to in order.",
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

    return parser


def inspiral(
    input_sample_rate: int,
    channel_name: List[str],
    data_source: str = "frames",
    nslice: int = -1,
    whitening_method: str = "gstlal",
    ht_gate_threshold: float = float("+inf"),
    impulse_bank: str = None,
    impulse_bankno: int = None,
    impulse_ifo: str = None,
    impulse_position: int = None,
    ranking_stat_output: List[str] = None,
    frame_cache: str = None,
    gps_start_time: int = None,
    gps_end_time: int = None,
    frame_segments_file: Optional[str] = None,
    frame_segments_name: Optional[str] = None,
    shared_memory_dir: str = None,
    reference_psd: str = None,
    torch_dtype: str = None,
    svd_bank=None,
    nbank_pretend=None,
    torch_device=None,
    trigger_finding_duration=None,
    fake_sink=None,
    verbose=None,
    analysis_tag=None,
    trigger_output=None,
    event_config=None,
    output_kafka_server=None,
    graph_name=None,
    state_channel_name=None,
    state_vector_on_bits=None,
    wait_time=None,
    psd_fft_length=None,
):
    # Mutable defaults
    if ranking_stat_output is None:
        ranking_stat_output = []

    if data_source == "impulse":
        if not impulse_bank:
            raise ValueError("Must specify impulse_bank when data_source='impulse'")
        elif impulse_bankno is None:
            raise ValueError("Must specify impulse_bankno when data_source='impulse'")
        elif not impulse_ifo:
            raise ValueError("Must specify impulse_ifo when data_source='impulse'")

    # FIXME: currently track psd is always enabled in whitener
    # if not reference_psd:
    #    track_psd = True

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

    #
    # Build pipeline
    #
    pipeline = Pipeline()

    # Create data source
    source_out_links, input_sample_rate = datasource(
        pipeline=pipeline,
        channel_name=channel_name,
        state_channel_name=state_channel_name,
        state_vector_on_bits=state_vector_on_bits,
        shared_memory_dir=shared_memory_dir,
        input_sample_rate=input_sample_rate,
        data_source=data_source,
        frame_cache=frame_cache,
        gps_start_time=gps_start_time,
        gps_end_time=gps_end_time,
        frame_segments_file=frame_segments_file,
        frame_segments_name=frame_segments_name,
        wait_time=wait_time,
        impulse_position=impulse_position,
        verbose=verbose,
    )

    ifos = list(source_out_links.keys())

    # read in the svd banks
    banks = group_and_read_banks(
        svd_bank=svd_bank,
        source_ifos=ifos,
        nbank_pretend=nbank_pretend,
        nslice=nslice,
        verbose=True,
    )

    # sort and group the svd banks by sample rate
    sorted_bank = SortedBank(
        banks=banks,
        device=torch_device,
        dtype=dtype,
        nbank_pretend=nbank_pretend,
        nslice=nslice,
        verbose=True,
    )
    bank_metadata = sorted_bank.bank_metadata

    template_maxrate = bank_metadata["maxrate"]

    # Condition the data source if not doing an impulse test
    if data_source != "impulse":
        source_out_links, spectrum_out_links, whiten_latency_out_links = condition(
            pipeline=pipeline,
            ifos=ifos,
            whiten_sample_rate=template_maxrate,
            input_links=source_out_links,
            data_source=data_source,
            input_sample_rate=input_sample_rate,
            psd_fft_length=psd_fft_length,
            whitening_method=whitening_method,
            reference_psd=reference_psd,
            ht_gate_threshold=ht_gate_threshold,
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
    )

    # make the sink
    if data_source == "impulse":
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
        pipeline.insert(
            Itacacac(
                name="itacacac",
                sink_pad_names=tuple(ifos),
                source_pad_names=("trigs",),
                sample_rate=template_maxrate,
                trigger_finding_duration=trigger_finding_duration,
                autocorrelation_banks=sorted_bank.autocorrelation_banks,
                template_ids=sorted_bank.template_ids,
                bankids_map=sorted_bank.bankids_map,
                end_times=sorted_bank.end_times,
                kafka=data_source == "devshm",
                device=torch_device,
                event_config=event_config,
            ),
        )
        for ifo in ifos:
            pipeline.insert(
                link_map={
                    "itacacac:sink:" + ifo: lloid_output_source_link[ifo],
                }
            )
        # if data_source == "devshm":
        #    pipeline.insert(
        #        Latency(
        #            name="itacacac_latency",
        #            sink_pad_names=("data",),
        #            source_pad_names=("latency",),
        #            route="all_itacacac_latency",
        #        ),
        #        link_map={"itacacac_latency:sink:data": "itacacac:src:trigs"},
        #    )

        # Connect sink
        if fake_sink:
            pipeline.insert(
                NullSink(
                    name="Sink",
                    sink_pad_names=("sink",),
                ),
                link_map={"Sink:sink:sink": "itacacac:src:trigs"},
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
        elif data_source == "devshm":
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
                        "gstlal." + analysis_tag + ".latency_history",
                    ]
                    + [
                        "gstlal." + analysis_tag + ".all_itacacac_latency",
                    ]
                    + [
                        "gstlal." + analysis_tag + "." + ifo + "_whitening_latency"
                        for ifo in ifos
                    ],
                    # topic="gstlal.greg_test.H1_snr_history",
                    routes=[ifo + "_snr_history" for ifo in ifos],
                    reduce_time=1,
                    verbose=verbose,
                ),
                link_map={
                    "KafkaSnk:sink:kafka": "itacacac:src:trigs",
                    "KafkaSnk:sink:itacacac_latency": "itacacac_latency:src:latency",
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
        elif event_config is not None:
            pipeline.insert(
                StillSuitSink(
                    name="StillSuitSnk",
                    sink_pad_names=("trigs",)
                    + tuple(["segments_" + ifo for ifo in ifos]),
                    config_name=event_config,
                    trigger_output=trigger_output,
                    template_ids=sorted_bank.template_ids.numpy(),
                    template_sngls=sorted_bank.sngls,
                    subbankids=sorted_bank.subbankids,
                    itacacac_pad_name="trigs",
                    segments_pad_map={"segments_" + ifo: ifo for ifo in ifos},
                ),
                link_map={
                    "StillSuitSnk:sink:trigs": "itacacac:src:trigs",
                },
            )
            # FIXME: horizons should connect to strike sink once strike sink is ready
            for ifo in ifos:
                pipeline.insert(
                    HorizonDistance(
                        name=ifo + "_Horizon",
                        source_pad_names=(ifo,),
                        sink_pad_names=(ifo,),
                        m1=1.4,
                        m2=1.4,
                        fmin=10.0,
                        fmax=1000.0,
                        delta_f=1 / 16.0,
                    ),
                    NullSink(
                        name="Null_" + ifo,
                        sink_pad_names=(ifo,),
                    ),
                    link_map={
                        ifo + "_Horizon:sink:" + ifo: spectrum_out_links[ifo],
                        "Null_" + ifo + ":sink:" + ifo: ifo + "_Horizon:src:" + ifo,
                    },
                )
            for ifo in ifos:
                pipeline.insert(
                    link_map={
                        "StillSuitSnk:sink:segments_" + ifo: source_out_links[ifo],
                    }
                )
        else:
            raise ValueError("Unknown sink option")
            # pipeline.insert(
            #    StrikeSink(
            #        name="StrikeSnk",
            #        sink_pad_names=("trigs",)
            #        + tuple(["horizon_" + ifo for ifo in ifos]),
            #        ifos=ifos,
            #        verbose=verbose,
            #        all_template_ids=sorted_bank.template_ids.numpy(),
            #        bankids_map=sorted_bank.bankids_map,
            #        subbankids=sorted_bank.subbankids,
            #        template_sngls=sorted_bank.sngls,
            #        trigger_output=trigger_output,
            #        ranking_stat_output=ranking_stat_output,
            #    ),
            #    link_map={
            #        "StrikeSnk:sink:trigs": "itacacac:src:trigs",
            #    },
            # )
            # for ifo in ifos:
            #    pipeline.insert(
            #        link_map={
            #            "StrikeSnk:sink:horizon_" + ifo: horizon_out_links[ifo],
            #        }
            #    )

    # Plot pipeline
    if graph_name:
        pipeline.visualize(graph_name)

    # Run pipeline
    pipeline.run()

    #
    # Cleanup template bank temp files
    #

    print("shutdown: template bank cleanup", flush=True, file=sys.stderr)
    for ifo in banks:
        for bank in banks[ifo]:
            if verbose:
                print("removing file: ", bank.template_bank_filename, file=sys.stderr)
            os.remove(bank.template_bank_filename)

    print("shutdown: del bank", flush=True, file=sys.stderr)
    del bank


def main():
    # parse arguments
    parser = parse_command_line_datasource()
    parser = parse_command_line_condition(parser)
    parser = parse_command_line(parser)
    options = parser.parse_args()

    inspiral(
        input_sample_rate=options.input_sample_rate,
        data_source=options.data_source,
        impulse_bank=options.impulse_bank,
        impulse_bankno=options.impulse_bankno,
        frame_cache=options.frame_cache,
        impulse_ifo=options.impulse_ifo,
        impulse_position=options.impulse_position,
        gps_start_time=options.gps_start_time,
        gps_end_time=options.gps_end_time,
        frame_segments_file=options.frame_segments_file,
        frame_segments_name=options.frame_segments_name,
        channel_name=options.channel_name,
        shared_memory_dir=options.shared_memory_dir,
        reference_psd=options.reference_psd,
        torch_dtype=options.torch_dtype,
        svd_bank=options.svd_bank,
        nbank_pretend=options.nbank_pretend,
        nslice=options.nslice,
        torch_device=options.torch_device,
        trigger_finding_duration=options.trigger_finding_duration,
        fake_sink=options.fake_sink,
        verbose=options.verbose,
        analysis_tag=options.analysis_tag,
        trigger_output=options.trigger_output,
        event_config=options.event_config,
        ranking_stat_output=options.ranking_stat_output,
        output_kafka_server=options.output_kafka_server,
        graph_name=options.graph_name,
        state_channel_name=options.state_channel_name,
        state_vector_on_bits=options.state_vector_on_bits,
        wait_time=options.wait_time,
        psd_fft_length=options.psd_fft_length,
        whitening_method=options.whitening_method,
        ht_gate_threshold=options.ht_gate_threshold,
    )


if __name__ == "__main__":
    main()
