from __future__ import annotations

from argparse import ArgumentParser
from typing import List

from sgn.apps import Pipeline
from sgnligo.sources import datasource, parse_command_line_datasource
from sgnligo.transforms import condition, parse_command_line_condition
from sgnts.sinks import FakeSeriesSink

from sgnl.sinks import PSDSink


def parse_command_line(parser=None):
    if parser is None:
        parser = ArgumentParser(description=__doc__)

    # add our own options

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument(
        "--output-name",
        metavar="filename",
        required=True,
        help="Set the output filename. If no filename is given, the output is dumped"
        " to stdout.",
    )

    return parser


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

    condition_out_links, spectrum_out_links, _ = condition(
        pipeline=pipeline,
        ifos=ifos,
        whiten_sample_rate=whiten_sample_rate,
        input_links=source_out_links,
        data_source=data_source,
        input_sample_rate=input_sample_rate,
        psd_fft_length=psd_fft_length,
        whitening_method=whitening_method,
        reference_psd=reference_psd,
    )

    for ifo in ifos:
        pipeline.insert(
            FakeSeriesSink(
                name=ifo + "HoftSink",
                sink_pad_names=("hoft",),
                verbose=verbose,
            ),
            link_map={
                ifo + "HoftSink:sink:hoft": condition_out_links[ifo],
            },
        )
    pipeline.insert(
        PSDSink(
            fname=output_name,
            name="PSDSink",
            sink_pad_names=(tuple(ifo for ifo in ifos)),
        ),
        link_map={"PSDSink:sink:" + ifo: spectrum_out_links[ifo] for ifo in ifos},
    )

    pipeline.run()


def main():
    # parse arguments
    parser = parse_command_line_datasource()
    parser = parse_command_line_condition(parser)
    parser = parse_command_line(parser)
    options = parser.parse_args()

    reference_psd(
        whiten_sample_rate=options.whiten_sample_rate,
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
