"""An executable to measure the power spectral density of time-series data."""

# Copyright (C) 2010 Kipp Cannon, Chad Hanna, Leo Singer
# Copyright (C) 2024 Anushka Doke, Yun-Jing Huang, Ryan Magee, Shio Sakon

from __future__ import annotations

from argparse import ArgumentParser

from sgn.apps import Pipeline
from sgnligo.sources import DataSourceInfo, datasource
from sgnligo.transforms import ConditionInfo, condition
from sgnts.sinks import FakeSeriesSink

from sgnl.sinks import PSDSink


def parse_command_line():
    parser = ArgumentParser(description=__doc__)

    DataSourceInfo.append_options(parser)
    ConditionInfo.append_options(parser)

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

    options = parser.parse_args()

    return options


def reference_psd(
    data_source_info: DataSourceInfo,
    condition_info: ConditionInfo,
    output_name=None,
    verbose=None,
):

    pipeline = Pipeline()

    # Create data source
    source_out_links, _ = datasource(
        pipeline=pipeline,
        info=data_source_info,
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

    ifos = data_source_info.ifos

    condition_out_links, spectrum_out_links, _ = condition(
        pipeline=pipeline,
        condition_info=condition_info,
        ifos=ifos,
        data_source=data_source_info.data_source,
        input_sample_rate=data_source_info.input_sample_rate,
        input_links=source_out_links,
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
    options = parse_command_line()

    data_source_info = DataSourceInfo.from_options(options)
    condition_info = ConditionInfo.from_options(options)

    reference_psd(
        data_source_info=data_source_info,
        condition_info=condition_info,
        output_name=options.output_name,
        verbose=options.verbose,
    )


if __name__ == "__main__":
    main()
