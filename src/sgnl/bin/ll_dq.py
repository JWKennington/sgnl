from argparse import ArgumentParser

from sgn.apps import Pipeline
from sgnligo.sinks import KafkaSink
from sgnligo.sources import DataSourceInfo, datasource
from sgnligo.transforms import ConditionInfo, HorizonDistance, condition
from sgnts.sinks import FakeSeriesSink


def parse_command_line():
    parser = ArgumentParser(description=__doc__)

    DataSourceInfo.append_options(parser)

    parser.add_argument(
        "--scald-config",
        metavar="path",
        help="sets ligo-scald options based on yaml configuration.",
    )
    parser.add_argument(
        "--output-kafka-server",
        metavar="addr",
        help="Set the server address and port number for output data. Optional",
    )
    parser.add_argument(
        "--analysis-tag",
        metavar="tag",
        default="test",
        help="Set the string to identify the analysis in which this job is part of."
        ' Used when --output-kafka-server is set. May not contain "." nor "-". Default'
        " is test.",
    )
    parser.add_argument(
        "--horizon-approximant",
        type="string",
        default="IMRPhenomD",
        help="Specify a waveform approximant to use while calculating the horizon'\
        ' distance and range. Default is IMRPhenomD.",
    )
    parser.add_argument(
        "--horizon-f-min",
        metavar="Hz",
        type="float",
        default=15.0,
        help="Set the frequency at which the waveform model is to begin for the'\
        ' horizon distance and range calculation. Default is 15 Hz.",
    )
    parser.add_argument(
        "--horizon-f-max",
        metavar="Hz",
        type="float",
        default=900.0,
        help="Set the upper frequency cut off for the waveform model used in the'\
        ' horizon distance and range calculation. Default is 900 Hz.",
    )
    parser.add_argument(
        "--kafka-reduce-time",
        metavar="seconds",
        type=int,
        default=1,
        help="Time to reduce data to send to kafka.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )

    options = parser.parse_args()

    return options


def ll_dq(
    data_source_info,
    condition_info,
    scald_config,
    output_kafka_server,
    analysis_tag,
    horizon_approximant,
    horizon_f_min,
    horizon_f_max,
    kafka_reduce_time,
    verbose,
):
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
    #  |  Horizon   |
    #   ------------
    #          \
    #       H1  \
    #           -----------
    #          | KafkaSink |
    #           -----------
    #

    if len(data_source_info.ifos) > 1:
        raise ValueError("Only supports one ifo")

    ifo = data_source_info.ifos[0]

    pipeline = Pipeline()
    source_out_links = datasource(
        pipeline=pipeline,
        info=data_source_info,
    )

    condition_out_links, spectrum_out_links, _ = condition(
        pipeline=pipeline,
        condition_info=condition_info,
        ifos=data_source_info.ifos,
        data_source=data_source_info.data_source,
        input_sample_rate=data_source_info.input_sample_rate,
        input_links=source_out_links,
    )

    pipeline.insert(
        HorizonDistance(
            name="Horizon",
            source_pad_names=("horizon",),
            sink_pad_names=("spectrum",),
            m1=1.4,
            m2=1.4,
            fmin=horizon_f_min,
            fmax=horizon_f_max,
            range=True,
            delta_f=1 / 16.0,
        ),
        FakeSeriesSink(
            name="HoftSnk",
            sink_pad_names=("hoft",),
            verbose=verbose,
        ),
        KafkaSink(
            name="HorizonSnk",
            sink_pad_names=("horizon",),
            output_kafka_server=output_kafka_server,
            topic="gstlal." + analysis_tag + "." + ifo + "_range_history",
            route="range_history",
            metadata_key="range",
            tags=[
                ifo,
            ],
            reduce_time=kafka_reduce_time,
            verbose=verbose,
        ),
    )

    pipeline.insert(
        link_map={
            "Horizon:sink:spectrum": spectrum_out_links[ifo],
            "HorizonSnk:sink:horizon": "Horizon:src:horizon",
            "HoftSnk:sink:hoft": condition_out_links[ifo],
        }
    )

    pipeline.run()


def main():
    # parse arguments
    options = parse_command_line()

    data_source_info = DataSourceInfo.from_options(options)
    condition_info = ConditionInfo.from_options(options)

    ll_dq(
        data_source_info,
        condition_info,
        options.scald_config,
        options.otuput_kafka_server,
        options.analysis_tag,
        options.horizon_approximant,
        options.horizon_f_min,
        options.horizon_f_max,
        options.kafka_reduce_time,
        options.verbose,
    )


if __name__ == "__main__":
    main()
