import os
import sys
from optparse import OptionGroup, OptionParser
from typing import List

import torch

from sgn.apps import Pipeline
from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnligo.base.utils import parse_list_to_dict
from sgnligo.sinks import ImpulseSink, KafkaSink, StrikeSink
from sgnligo.sources import datasource
from sgnligo.transforms import Itacacac, Latency, condition, lloid
from sgnts.sinks import FakeSeriesSink


def parse_command_line():
    parser = OptionParser()

    group = OptionGroup(parser, "Data source", "Options for data source.")
    group.add_option(
        "--data-source",
        action="store",
        default="frames",
        help="The type of the input source. Supported sources: 'white', 'sin', "
        "'impulse', 'frames', 'devshm'",
    )
    group.add_option(
        "--channel-name",
        metavar="ifo=channel-name",
        action="append",
        help="Name of the data channel to analyze. Can be given multiple times as "
        "--channel-name=IFO=CHANNEL-NAME",
    )
    group.add_option(
        "--sample-rate",
        metavar="Hz",
        type=int,
        help="Requested sampling rate of the data.",
    )
    group.add_option(
        "--source-buffer-duration",
        type=float,
        metavar="seconds",
        action="store",
        help="Source elements will produce buffers in strides of this duration.",
    )
    group.add_option(
        "--frame-cache",
        metavar="file",
        help="Set the path to the frame cache file to analyze.",
    )
    group.add_option(
        "--gps-start-time",
        metavar="seconds",
        help="Set the start time of the segment to analyze in GPS seconds. "
        "For frame cache data source",
    )
    group.add_option(
        "--gps-end-time",
        metavar="seconds",
        help="Set the end time of the segment to analyze in GPS seconds. "
        "For frame cache data source",
    )
    group.add_option(
        "--shared-memory-dir",
        metavar="ifo=directory",
        action="append",
        help="Set the name of the shared memory directory. "
        "Can be given multiple times as --shared-memory-dir=IFO=DIR-NAME",
    )
    group.add_option(
        "--state-channel-name",
        metavar="ifo=channel-name",
        action="append",
        help="Set the state vector channel name. "
        "Can be given multiple times as --state-channel-name=IFO=CHANNEL-NAME",
    )
    group.add_option(
        "--state-vector-on-bits",
        metavar="ifo=number",
        action="append",
        help="Set the state vector on bits. "
        "Can be given multiple times as --state-vector-on-bits=IFO=NUMBER",
    )
    group.add_option(
        "--wait-time",
        metavar="seconds",
        type=int,
        default=60,
        help="Time to wait for new files in seconds before throwing an error. "
        "In online mode, new files should always arrive every second, unless "
        "there are problems. Default wait time is 60 seconds.",
    )
    group.add_option(
        "--num-buffers",
        type="int",
        action="store",
        default=10,
        help="Number of buffers the source element should produce when source is "
        "fake source",
    )
    group.add_option(
        "--impulse-position",
        action="store",
        default=-1,
        help="The sample point to put the impulse at. If -1, place randomly.",
    )
    group.add_option(
        "--impulse-ifo",
        action="store",
        help="Only do impulse test on data from this ifo.",
    )
    parser.add_option_group(group)

    group = OptionGroup(
        parser, "PSD Options", "Adjust noise spectrum estimation parameters"
    )
    group.add_option(
        "--whitening-method",
        metavar="algorithm",
        default="gstlal",
        help="Algorithm to use for whitening the data. Supported options are 'gwpy' "
        "or 'gstlal'. Default is gstlal.",
    )
    group.add_option(
        "--psd-fft-length",
        action="store",
        type=int,
        help="The fft length for psd estimation.",
    )
    group.add_option(
        "--reference-psd",
        metavar="file",
        help="load the spectrum from this LIGO light-weight XML file (optional).",
    )
    group.add_option(
        "--track-psd",
        action="store_true",
        help="Enable dynamic PSD tracking.  Always enabled if --reference-psd is not "
        "given.",
    )
    parser.add_option_group(group)

    group = OptionGroup(parser, "Data Qualtiy", "Adjust data quality handling")
    group.add_option(
        "--ht-gate-threshold",
        action="store",
        type=float,
        default=float("+inf"),
        help="The gating threshold. Data above this value will be gated out.",
    )
    parser.add_option_group(group)

    group = OptionGroup(
        parser, "Trigger Generator", "Adjust trigger generator behaviour"
    )
    group.add_option(
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
    group.add_option(
        "--nbank-pretend",
        type="int",
        action="store",
        default=0,
        help="Pretend we have this many subbanks by copying the first subbank "
        "this many times",
    )
    group.add_option(
        "--nslice",
        type="int",
        action="store",
        default=-1,
        help="Only filter this many timeslices. Default: -1, filter all timeslices.",
    )
    group.add_option(
        "--trigger-finding-length",
        type="int",
        metavar="samples",
        action="store",
        default=2048,
        help="Produce triggers in blocks of this many samples.",
    )
    group.add_option(
        "--impulse-bank",
        metavar="filename",
        action="store",
        default=None,
        help="The full original templates to compare the impulse response test with.",
    )
    group.add_option(
        "--impulse-bankno",
        metavar="index",
        action="store",
        help="The template bank index to perform the impulse test on.",
    )
    group.add_option(
        "--trigger-output",
        metavar="filename",
        action="append",
        default=[],
        help="Set the name of the LIGO light-weight XML output file *.{xml,xml.gz} "
        "or an SQLite database *.sqlite (required).  Can be given multiple times.  "
        "Exactly as many output files must be specified as svd-bank files will be "
        "processed (see --svd-bank).",
    )
    parser.add_option_group(group)

    group = OptionGroup(
        parser, "Ranking Statistic Options", "Adjust ranking statistic behaviour"
    )
    group.add_option(
        "--ranking-stat-output",
        metavar="filename",
        action="append",
        default=[],
        help="Set the name of the file to which to write ranking statistic data "
        "collected from triggers (optional).  Can be given more than once.  If given, "
        "exactly as many must be provided as there are --svd-bank options and they will"
        " be writen to in order.",
    )
    parser.add_option_group(group)

    group = OptionGroup(parser, "Program Behaviour")
    group.add_option(
        "--torch-device",
        action="store",
        default="cpu",
        help="The device to run LLOID and Trigger generation on.",
    )
    group.add_option(
        "--torch-dtype",
        action="store",
        type="str",
        default="float32",
        help="The data type to run LLOID and Trigger generation with.",
    )
    group.add_option(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    group.add_option(
        "--output-kafka-server",
        metavar="addr",
        help="Set the server address and port number for output data. Optional",
    )
    group.add_option(
        "--analysis-tag",
        metavar="tag",
        default="test",
        help="Set the string to identify the analysis in which this job is part of. "
        'Used when --output-kafka-server is set. May not contain "." nor "-". Default '
        "is test.",
    )
    group.add_option(
        "--graph-name", metavar="filename", help="Plot pipieline graph to graph_name."
    )
    group.add_option(
        "--fake-sink", action="store_true", help="Connect to a FakeSeriesSink"
    )
    parser.add_option_group(group)

    options, args = parser.parse_args()

    return options, args


def inspiral(
    source_buffer_duration: int,
    sample_rate: int,
    channel_name: List[str],
    data_source: str = "frames",
    num_buffers: int = 10,
    nslice: int = -1,
    whitening_method: str = "gstlal",
    ht_gate_threshold: float = float("+inf"),
    impulse_bank: str = None,
    impulse_bankno: int = None,
    impulse_ifo: str = None,
    ranking_stat_output: List[str] = None,

    frame_cache: str = None,
    gps_start_time: int = None,
    gps_end_time: int = None,
    shared_memory_dir: str = None,
    reference_psd: str = None,
    torch_dtype: str = None,
    svd_bank=None,
    nbank_pretend=None,
    torch_device=None,
    trigger_finding_length=None,
    fake_sink=None,
    verbose=None,
    analysis_tag=None,
    trigger_output=None,

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

    # sanity check options
    known_datasources = ["white", "sin", "impulse", "frames", "devshm"]
    if data_source in ["white", "sin", "impulse"]:
        if not num_buffers:
            raise ValueError(
                "Must specify num_buffers when data_source is one of 'white',"
                "'sin', 'impulse'"
            )
        elif not sample_rate:
            raise ValueError(
                "Must specify sample_rate when data_source is one of 'white',"
                "'sin', 'impulse'"
            )
        elif not source_buffer_duration:
            raise ValueError(
                "Must specify source_buffer_duration when data_source is one "
                "of 'white', 'sin', 'impulse'"
            )

        if data_source == "impulse":
            if not impulse_bank:
                raise ValueError("Must specify impulse_bank when data_source='impulse'")
            elif not impulse_bankno:
                raise ValueError(
                    "Must specify impulse_bankno when data_source='impulse'"
                )
            elif not impulse_ifo:
                raise ValueError("Must specify impulse_ifo when data_source='impulse'")

        if data_source == "white":
            channel_dict = parse_list_to_dict(channel_name)
            source_ifos = list(channel_dict.keys())
        else:
            source_ifos = None
    elif data_source == "frames":
        if not frame_cache:
            raise ValueError("Must specify frame_cache when data_source='frames'")
        elif not gps_start_time or not gps_end_time:
            raise ValueError(
                "Must specify gps_start_time and gps_end_time when "
                "data_source='frames'"
            )
        elif not channel_name:
            raise ValueError("Must specify channel_name when data_source='frames'")
        elif not sample_rate:
            # FIXME: shoud we just determine the sample rate from the gwf file?
            raise ValueError("Must specify sample_rate when data_source='frames'")
        elif not source_buffer_duration:
            raise ValueError("Must specify source_buffer_duration")
        channel_dict = parse_list_to_dict(channel_name)
        source_ifos = list(channel_dict.keys())
    elif data_source == "devshm":
        if not shared_memory_dir:
            raise ValueError("Must specify shared_memory_dir when data_source='devshm'")
        elif not channel_name:
            raise ValueError("Must specify channel_name when data_source='devshm'")
        source_buffer_duration = 1
        sample_rate = 16384
        channel_dict = parse_list_to_dict(channel_name)
        source_ifos = list(channel_dict.keys())
    else:
        raise ValueError(f"Unknown data source, must be one of {known_datasources}")

    # FIXME: currently track psd is always enabled in whitener
    if not reference_psd:
        track_psd = True

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

    # read in the svd banks
    banks = group_and_read_banks(
        svd_bank=svd_bank,
        source_ifos=source_ifos,
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

    ifos = [] if source_ifos is None else source_ifos
    maxrate = bank_metadata["maxrate"]

    #
    # Build pipeline
    #
    pipeline = Pipeline()

    # Create data source
    source_out_links = datasource(
        pipeline=pipeline,
        ifos=ifos,
        channel_name=channel_name,
        state_channel_name=state_channel_name,
        state_vector_on_bits=state_vector_on_bits,
        shared_memory_dir=shared_memory_dir,
        source_buffer_duration=source_buffer_duration,
        sample_rate=sample_rate,
        data_source=data_source,
        frame_cache=frame_cache,
        gps_start_time=gps_start_time,
        gps_end_time=gps_end_time,
        wait_time=wait_time,
        num_buffers=num_buffers,
        verbose=verbose,
    )

    # Condition the data source if not doing an impulse test
    if data_source != "impulse":
        source_out_links, horizon_out_links, whiten_latency_out_links = condition(
            pipeline=pipeline,
            ifos=ifos,
            maxrate=maxrate,
            input_links=source_out_links,
            data_source=data_source,
            sample_rate=sample_rate,
            psd_fft_length=psd_fft_length,
            whitening_method=whitening_method,
            reference_psd=reference_psd,
            ht_gate_threshold=ht_gate_threshold,
        )
    else:
        horizon_out_links = None

    num_samples = int(source_buffer_duration * maxrate)

    # connect LLOID
    lloid_output_source_link = lloid(
        pipeline,
        sorted_bank,
        source_out_links,
        num_samples,
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
                plotname="plots/response",
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
                trigger_finding_length=trigger_finding_length,
                autocorrelation_banks=sorted_bank.autocorrelation_banks,
                template_ids=sorted_bank.template_ids,
                bankids_map=sorted_bank.bankids_map,
                end_times=sorted_bank.end_times,
                kafka=data_source == "devshm",
                device=torch_device,
            ),
        )
        for ifo in ifos:
            pipeline.insert(
                link_map={
                    "itacacac:sink:" + ifo: lloid_output_source_link[ifo],
                }
            )
        if data_source == "devshm":
            pipeline.insert(
                Latency(
                    name="itacacac_latency",
                    sink_pad_names=("data",),
                    source_pad_names=("latency",),
                    route="all_itacacac_latency",
                ),
                link_map={"itacacac_latency:sink:data": "itacacac:src:trigs"},
            )

        # Connect sink
        if fake_sink:
            pipeline.insert(
                FakeSeriesSink(
                    name="Sink",
                    sink_pad_names=("sink",),
                    verbose=verbose,
                ),
                link_map={"Sink:sink:sink": "itacacac:src:trigs"},
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
        else:
            pipeline.insert(
                StrikeSink(
                    name="StrikeSnk",
                    sink_pad_names=("trigs",)
                    + tuple(["horizon_" + ifo for ifo in ifos]),
                    ifos=ifos,
                    verbose=verbose,
                    all_template_ids=sorted_bank.template_ids.numpy(),
                    bankids_map=sorted_bank.bankids_map,
                    subbankids=sorted_bank.subbankids,
                    template_sngls=sorted_bank.sngls,
                    trigger_output=trigger_output,
                    ranking_stat_output=ranking_stat_output,
                ),
                link_map={
                    "StrikeSnk:sink:trigs": "itacacac:src:trigs",
                },
            )
            for ifo in ifos:
                pipeline.insert(
                    link_map={
                        "StrikeSnk:sink:horizon_" + ifo: horizon_out_links[ifo],
                    }
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
    for ifo in banks:
        for bank in banks[ifo]:
            if verbose:
                print("removing file: ", bank.template_bank_filename, file=sys.stderr)
            os.remove(bank.template_bank_filename)

    print("shutdown: del bank", flush=True, file=sys.stderr)
    del bank


def main():
    # parse arguments
    options, args = parse_command_line()

    inspiral(
        source_buffer_duration=options.source_buffer_duration,
        sample_rate=options.sample_rate,
        data_source=options.data_source,
        num_buffers=options.num_buffers,
        impulse_bank=options.impulse_bank,
        impulse_bankno=options.impulse_bankno,
        frame_cache=options.frame_cache,
        impulse_ifo=options.impulse_ifo,
        gps_start_time=options.gps_start_time,
        gps_end_time=options.gps_end_time,
        channel_name=options.channel_name,
        shared_memory_dir=options.shared_memory_dir,
        reference_psd=options.reference_psd,
        torch_dtype=options.torch_dtype,
        svd_bank=options.svd_bank,
        nbank_pretend=options.nbank_pretend,
        nslice=options.nslice,
        torch_device=options.torch_device,
        trigger_finding_length=options.trigger_finding_length,
        fake_sink=options.fake_sink,
        verbose=options.verbose,
        analysis_tag=options.analysis_tag,
        trigger_output=options.trigger_output,
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
