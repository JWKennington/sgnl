# Copyright (C) 2024  Cort Posnansky (cort.posnansky@ligo.org)
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


import argparse
import pathlib
import sys

from ezdag import DAG

from sgnl.dags import layers
from sgnl.dags.config import build_config, create_time_bins
from sgnl.dags.util import DataCache, DataType, load_svd_options, mchirp_range_to_bins


def parse_command_line(args: list[str] = None) -> argparse.Namespace:
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Generate a sgnl dag from a config file"
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True, help="The config file to load"
    )
    parser.add_argument(
        "-w", "--workflow", type=str, required=True, help="The type of dag to generate"
    )
    parser.add_argument(
        "--dag-dir",
        type=str,
        default=".",
        help="The directory in which to write the dag",
    )
    parser.add_argument("--dag-name", type=str, default=None, help="A name for the dag")
    args = parser.parse_args(args)

    if args.dag_name:
        if "/" in args.dag_name:
            raise ValueError(
                "The dag name must not be a path. Use --dag-dir to put the dag in a"
                " different directory."
            )
        if args.dag_name.endswith(".dag"):
            raise ValueError(
                'The given dag name ends with ".dag". This is unnecessary because it'
                " will be appended to the file name automatically."
            )

    return args


def main():
    args = parse_command_line()
    config = build_config(args.config, args.dag_dir)

    if args.dag_name:
        dag_name = args.dag_name
    else:
        dag_name = f"sgnl_{args.workflow}"

    # Start building the dag
    dag = DAG(dag_name)
    if args.workflow == "test":
        # FIXME Delete this workflow
        dag.attach(layers.test(config.echo, config.condor))

    if args.workflow == "psd":
        # Reference PSD layer
        dag.attach(layers.reference_psd(config.psd, config.condor))

        # Median PSD layer
        dag.attach(layers.median_psd(config.psd, config.condor))

    if args.workflow == "svd":
        pass

    if args.workflow == "filter":
        if not config.paths.filter_dir:
            config.paths.filter_dir = config.paths.storage

        ref_psd_cache = DataCache.find(
            DataType.REFERENCE_PSD, root=config.paths.input_data
        )
        svd_bank_cache = DataCache.find(
            DataType.SVD_BANK, root=config.paths.input_data, svd_bins="*"
        )

        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)

        max_duration = max(svd_bin["max_dur"] for svd_bin in svd_stats.bins.values())
        filter_start_pad = 16 * config.psd.fft_length + max_duration
        create_time_bins(config, start_pad=filter_start_pad)

        dist_stat_cache = DataCache.generate(
            DataType.DIST_STATS,
            config.ifo_combos,
            config.time_bins,
            svd_bins=svd_bins,
            root=config.paths.filter_dir,
        )

        trigger_cache = DataCache.generate(
            DataType.TRIGGERS,
            config.ifo_combos,
            config.time_bins,
            svd_bins=svd_bins,
            root=config.paths.filter_dir,
        )

        layer = layers.filter(
            config.psd,
            config.svd,
            config.filter,
            config.source,
            config.condor,
            ref_psd_cache,
            svd_bank_cache,
            dist_stat_cache,
            trigger_cache,
            svd_stats,
        )

        dag.attach(layer)

        clustered_trigger_cache = DataCache.generate(
            DataType.CLUSTERED_TRIGGERS,
            config.ifo_combos,
            config.time_bins,
            svd_bins=svd_bins,
            root=config.paths.filter_dir,
        )
        layer = layers.aggregate(
            config.filter, config.condor, trigger_cache, clustered_trigger_cache
        )

        dag.attach(layer)

        marg_dist_stat_cache = DataCache.generate(
            DataType.MARG_DIST_STATS,
            config.all_ifos,
            config.span,
            svd_bins=dist_stat_cache.groupby("bin").keys(),
            root=config.paths.filter_dir,
        )
        layer = layers.marginalize_dist_stats(
            config.filter, config.condor, dist_stat_cache, marg_dist_stat_cache
        )

        dag.attach(layer)

    if args.workflow == "injection-filter":
        if not config.paths.injection_dir:
            config.paths.injection_dir = config.paths.storage

        ref_psd_cache = DataCache.find(
            DataType.REFERENCE_PSD, root=config.paths.input_data
        )
        svd_bank_cache = DataCache.find(
            DataType.SVD_BANK, root=config.paths.input_data, svd_bins="*"
        )

        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)

        max_duration = max(svd_bin["max_dur"] for svd_bin in svd_stats.bins.values())
        filter_start_pad = 16 * config.psd.fft_length + max_duration
        create_time_bins(config, start_pad=filter_start_pad)

        trigger_cache = DataCache(DataType.TRIGGERS)

        for inj_name, inj_args in config.injections.filter.items():
            min_mchirp, max_mchirp = map(float, inj_args["range"].split(":"))
            svd_bins = mchirp_range_to_bins(min_mchirp, max_mchirp, svd_stats)
            trigger_cache += DataCache.generate(
                DataType.TRIGGERS,
                config.ifo_combos,
                config.time_bins,
                svd_bins=svd_bins,
                subtype=inj_name,
                root=config.paths.injection_dir,
            )

        layer = layers.injection_filter(
            config.psd,
            config.svd,
            config.filter,
            config.injections,
            config.source,
            config.condor,
            ref_psd_cache,
            svd_bank_cache,
            trigger_cache,
            svd_stats,
        )

        dag.attach(layer)

        clustered_trigger_cache = DataCache.generate(
            DataType.CLUSTERED_TRIGGERS,
            config.ifo_combos,
            config.time_bins,
            svd_bins=svd_bins,
            root=config.paths.filter_dir,
        )
        layer = layers.aggregate(
            config.filter, config.condor, trigger_cache, clustered_trigger_cache
        )

        dag.attach(layer)

    # Write dag and script to disk
    dag.write(pathlib.Path(args.dag_dir), write_script=True)
    dag.create_log_dir()


if __name__ == "__main__":
    main()
