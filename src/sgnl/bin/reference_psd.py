from argparse import ArgumentParser
from typing import List
from sgn.apps import Pipeline

from sgnts.sinks import FakeSeriesSink

from sgnts.sinks import FakeSeriesSink 
from sgnts.sources import FakeSeriesSrc
from sgnligo.transforms import (
    Whiten,
    Resampler,
)
from sgnligo.sources import datasource
from sgnligo.base.utils import parse_list_to_dict
from sgnl.sinks import PSDSink

def parse_command_line():
    parser = ArgumentParser(description=__doc__)

    # add our own options
    parser.add_argument(
        "--sample-rate",
        metavar="Hz",
        default=2048,
        type=int,
        help="Sample rate at which to generate the PSD, default 2048 Hz",
    )
    parser.add_argument(
        "--input-sample-rate",
        metavar="Hz",
        default=16384,
        type=int,
        help="Input sample rate. Default 16384 Hz",
    )
    parser.add_argument(
        "--psd-fft-length",
        metavar="s",
        default=8,
        type=int,
        help="FFT length, default 8s",
    )
    parser.add_argument(
        "--num-buffers",
        default=10,
        type=int,
        help="Number of buffers to process. Needed for white data.",
    )
    parser.add_argument(
        "--data-source",
        type=str,
        help="The data source to use.",
    )
    parser.add_argument(
        "--frame-cache",
        metavar="file",
        help="Set the path to the frame cache file to analyze.",
    )
    parser.add_argument(
        "--gps-start-time",
        metavar="seconds",
        help="Set the start time of the segment to analyze in GPS seconds. "
        "For frame cache data source",
    )
    parser.add_argument(
        "--gps-end-time",
        metavar="seconds",
        help="Set the end time of the segment to analyze in GPS seconds. "
        "For frame cache data source",
    )
    parser.add_argument(
        "--reference-psd",
        metavar="filename",
        help="Load spectrum from this LIGO light-weight XML file. The noise spectrum will be measured and tracked starting from this reference. (optional).",
    )
    parser.add_argument(
        "--whitening-method",
        metavar="algorithm",
        default="gstlal",
        help="Algorithm to use for whitening the data. Supported options are 'gwpy' or 'gstlal'. Default is gstlal.",
    )
    parser.add_argument(
        "--state-channel-name",
        metavar="ifo=channel-name",
        action="append",
        help="Set the state vector channel name. "
        "Can be given multiple times as --state-channel-name=IFO=CHANNEL-NAME",
    )
    parser.add_argument(
        "--state-vector-on-bits",
        metavar="ifo=number",
        action="append",
        help="Set the state vector on bits. "
        "Can be given multiple times as --state-vector-on-bits=IFO=NUMBER",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument(
        "--channel-name", metavar="ifo=channel-name", action="append", help="Name of the data channel to analyze."
    )
    parser.add_argument(
        "--shared-memory-dir",
        metavar="directory",
        help="Set the name of the shared memory directory.",
    )
    parser.add_argument(
        "--wait-time",
        metavar="seconds",
        type=int,
        default=60,
        help="Time to wait for new files in seconds before throwing an error. In online mode, new files should always arrive every second, unless there are problems. Default wait time is 60 seconds.",
    )
    parser.add_argument(
        "--output-name",
        metavar="filename",
        help="Set the output filename. If no filename is given, the output is dumped to stdout.",
    )

    options = parser.parse_args()

    return options

def reference_psd(
    sample_rate: int,
    channel_name: List[str],
    input_sample_rate: int = 16384,
    data_source: str = "frames",
    num_buffers: int = 10,
    whitening_method: str = "gstlal",
    frame_cache: str = None,
    gps_start_time: int = None,
    gps_end_time: int = None,
    shared_memory_dir: str = None,
    reference_psd: str = None,
    fake_sink=None,
    verbose=None,
    state_channel_name=None,
    state_vector_on_bits=None,
    wait_time=None,
    psd_fft_length=None,
    output_name=None,
):

    # sanity check options
    known_datasources = ["white", "sin", "impulse", "frames", "devshm"]
    if data_source in ["white", "sin", "impulse"]:
        if not num_buffers:
            raise ValueError(
                "Must specify num_buffers when data_source is one of 'white',"
                "'sin', 'impulse'"
            )
        elif not input_sample_rate:
            raise ValueError(
                "Must specify input_sample_rate when data_source is one of 'white',"
                "'sin', 'impulse'"
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
        elif not input_sample_rate:
            # FIXME: shoud we just determine the sample rate from the gwf file?
            raise ValueError("Must specify input_sample_rate when data_source='frames'")
        channel_dict = parse_list_to_dict(channel_name)
        source_ifos = list(channel_dict.keys())
    elif data_source == "devshm":
        if not shared_memory_dir:
            raise ValueError("Must specify shared_memory_dir when data_source='devshm'")
        elif not channel_name:
            raise ValueError("Must specify channel_name when data_source='devshm'")
        input_sample_rate = 16384
        channel_dict = parse_list_to_dict(channel_name)
        source_ifos = list(channel_dict.keys())
    else:
        raise ValueError(f"Unknown data source, must be one of {known_datasources}")

    # sanity check the whitening method given
    if whitening_method not in ("gwpy", "gstlal"):
        raise ValueError("Unknown whitening method, exiting.")

    channel_dict = parse_list_to_dict(channel_name)
    source_ifos = list(channel_dict.keys())
    ifos = [] if source_ifos is None else source_ifos

    pipeline = Pipeline()

    # Create data source
    source_out_links, input_sample_rate = datasource(
        pipeline=pipeline,
        ifos=ifos,
        channel_name=channel_name,
        state_channel_name=state_channel_name,
        state_vector_on_bits=state_vector_on_bits,
        shared_memory_dir=shared_memory_dir,
        sample_rate=input_sample_rate,
        data_source=data_source,
        frame_cache=frame_cache,
        gps_start_time=gps_start_time,
        gps_end_time=gps_end_time,
        wait_time=wait_time,
        num_buffers=num_buffers,
        verbose=verbose,
    )

    #
    #          -----------
    #         | DevShmSrc |
    #          -----------
    #         /
    #     H1 /
    #   ------------
    #  |  Resampler |
    #   ------------
    #       |
    #   ------------  hoft ----------
    #  |  Whiten    | --- | FakeSink |
    #   ------------       ----------
    #          |psd
    #   ------------
    #  |  PSDSink   |
    #   ------------
    #  

    for ifo in ifos:
        pipeline.insert(
            Resampler(
                name=ifo+"Resampler",
                source_pad_names=("resamp",),
                sink_pad_names=("frsrc",),
                inrate=input_sample_rate,
                outrate=sample_rate,
            ),
            Whiten(
                name=ifo+"Whitener",
                source_pad_names=("hoft",ifo),
                sink_pad_names=("resamp",),
                instrument=ifo,
                sample_rate=sample_rate,
                fft_length=psd_fft_length,
                whitening_method=whitening_method,
                reference_psd=reference_psd,
                psd_pad_name=ifo+"Whitener:src:"+ifo,
            ),
            FakeSeriesSink(
                name=ifo+"HoftSink",
                sink_pad_names=("hoft",),
                verbose=True,
            ),
            link_map={
                ifo+"Resampler:sink:frsrc": source_out_links[ifo],
                ifo+"Whitener:sink:resamp": ifo+"Resampler:src:resamp",
                ifo+"HoftSink:sink:hoft": ifo+"Whitener:src:hoft",
            }
        )
    pipeline.insert(
        PSDSink(
            fname = output_name,
            name="PSDSink",
            sink_pad_names=(tuple(ifo for ifo in ifos)),
        ),
        link_map={"PSDSink:sink:"+ifo: ifo+"Whitener:src:"+ifo for ifo in ifos}
        )
           
    pipeline.run()

def main():
    # parse arguments
    options = parse_command_line()

    reference_psd(
    sample_rate=options.sample_rate,
    input_sample_rate=options.input_sample_rate,
    channel_name=options.channel_name,
    data_source=options.data_source,
    num_buffers=options.num_buffers,
    whitening_method=options.whitening_method,
    frame_cache=options.frame_cache,
    gps_start_time=options.gps_start_time,
    gps_end_time=options.gps_end_time,
    shared_memory_dir=options.shared_memory_dir,
    reference_psd=options.reference_psd,
    fake_sink=None,
    verbose=None,
    state_channel_name=None,
    state_vector_on_bits=None,
    wait_time=None,
    psd_fft_length=options.psd_fft_length,
    output_name=options.output_name,
)

if __name__ == "__main__":
    main()
