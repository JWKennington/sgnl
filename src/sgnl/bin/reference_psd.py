from argparse import ArgumentParser
from typing import List
from sgn.apps import Pipeline

from sgnts.sinks import FakeSeriesSink

from sgnts.sinks import FakeSeriesSink 
from sgnts.sources import FakeSeriesSrc
from sgnts.transforms import Resampler
from sgnligo.transforms import Whiten
from sgnligo.sources import datasource
from sgnligo.base.utils import parse_list_to_dict
from sgnl.sinks import PSDSink

def parse_command_line():
    parser = ArgumentParser(description=__doc__)

    # add our own options
    parser.add_argument(
        "--psd-fft-length",
        metavar="s",
        default=8,
        type=int,
        help="FFT length, default 8s",
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
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument(
        "--output-sample-rate",
        metavar="Hz",
        default=2048,
        type=int,
        help="Sample rate at which to generate the PSD, default 2048 Hz",
    )
    parser.add_argument(
        "--output-name",
        metavar="filename",
        help="Set the output filename. If no filename is given, the output is dumped to stdout.",
    )

    options = parser.parse_args()

    return options

def reference_psd(
    whiten_sample_rate: int,
    channel_name: List[str],
    input_sample_rate: int = 16384,
    data_source: str = "frames",
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

    # sanity check the whitening method given
    if whitening_method not in ("gwpy", "gstlal"):
        raise ValueError("Unknown whitening method, exiting.")

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
        wait_time=wait_time,
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

    ifos = list(source_out_links.keys())
    for ifo in ifos:
        pipeline.insert(
            Resampler(
                name=ifo+"Resampler",
                source_pad_names=("resamp",),
                sink_pad_names=("frsrc",),
                inrate=input_sample_rate,
                outrate=whiten_sample_rate,
            ),
            Whiten(
                name=ifo+"Whitener",
                source_pad_names=("hoft",ifo),
                sink_pad_names=("resamp",),
                instrument=ifo,
                sample_rate=whiten_sample_rate,
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
