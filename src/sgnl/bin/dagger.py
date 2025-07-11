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
import os
import pathlib
import sys

from ezdag import DAG

from sgnl.bin import inspiral_bank_splitter, inspiral_set_svdbin_option
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
        "-i",
        "--init",
        action="store_true",
        help="Run initialization steps needed before making a dag. Cannot be "
        "provided at same time as --workflow.",
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True, help="The config file to load"
    )
    parser.add_argument(
        "-w", "--workflow", type=str, help="The type of dag to generate"
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

    if args.init is True and args.workflow is not None:
        raise ValueError("Cannot specify both --init and --workflow.")
    elif args.init is False and args.workflow is None:
        raise ValueError("Must specify either --init or --workflow.")

    return args


def main():
    args = parse_command_line()
    config = build_config(args.config, args.dag_dir)
    if args.init:
        # NOTE bank_name, output_full_bank_file, psd, psd_xml, and
        # psdinterp bank-splitter options are currently not implemented
        # options here. User will need to manually run bank splitter for
        # this functionality at this time
        arguments = {
            "bank_name": None,
            "f_final": config.svd.max_f_final,
            "f_low": config.svd.f_low,
            "filename": config.paths.template_bank,
            "group_by_chi": (
                config.svd.num_chi_bins if config.svd.sort_by == "chi" else None
            ),
            "group_by_mu": (
                config.svd.num_mu_bins if config.svd.sort_by == "mu" else None
            ),
            "instrument": config.instruments,
            "n": config.svd.num_split_templates,
            "num_banks": (
                config.svd.num_banks
                if isinstance(config.svd.num_banks, list)
                else [config.svd.num_banks]
            ),
            "output_path": os.path.join(config.paths.input_data, "split_bank"),
            "overlap": config.svd.overlap,
            "sort_by": config.svd.sort_by,
            "stats_file": config.svd.option_file,
            "verbose": True,
        }
        inspiral_bank_splitter.split_bank(
            **arguments,
            approximants=inspiral_bank_splitter.split_approximant_strings(
                config.svd.approximant
            ),
            argument_dict=arguments
            | {
                "approximants": config.svd.approximant
            },  # argument dict needs the approx strings
        )

        inspiral_set_svdbin_option.set_svdbin_option(config)
        return

    svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)
    max_duration = max(svd_bin["max_dur"] for svd_bin in svd_stats.bins.values())
    filter_start_pad = 16 * config.psd.fft_length + max_duration
    create_time_bins(config, start_pad=filter_start_pad)

    if args.dag_name:
        dag_name = args.dag_name
    else:
        dag_name = f"sgnl_{args.workflow}"

    # Start building the dag
    dag = DAG(dag_name)
    if args.workflow == "test":
        # FIXME Delete this workflow
        dag.attach(layers.test(config.echo, config.condor))

    elif args.workflow == "psd":
        # Reference PSD layer
        ref_psd_cache = DataCache.generate(
            DataType.REFERENCE_PSD,
            config.ifo_combos,
            config.time_bins,
            root=config.paths.input_data,
        )

        dag.attach(
            layers.reference_psd(
                filter_config=config.filter,
                psd_config=config.psd,
                source_config=config.source,
                condor_config=config.condor,
                ref_psd_cache=ref_psd_cache,
            )
        )

        # Median PSD layer
        median_psd_cache = DataCache.generate(
            DataType.MEDIAN_PSD,
            config.all_ifos,
            config.span,
            root=config.paths.input_data,
        )

        dag.attach(
            layers.median_psd(
                psd_config=config.psd,
                condor_config=config.condor,
                ref_psd_cache=ref_psd_cache,
                median_psd_cache=median_psd_cache,
            )
        )

    elif args.workflow == "svd":
        split_bank_cache = DataCache.find(
            DataType.SPLIT_BANK, svd_bins="*", subtype="*", root=config.paths.input_data
        )
        median_psd_cache = DataCache.find(
            DataType.MEDIAN_PSD, root=config.paths.input_data
        )

        svd_cache = DataCache.generate(
            DataType.SVD_BANK,
            config.ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.input_data,
        )

        dag.attach(
            layers.svd_bank(
                svd_config=config.svd,
                condor_config=config.condor,
                all_ifos=list(sorted(config.all_ifos)),
                split_bank_cache=split_bank_cache,
                median_psd_cache=median_psd_cache,
                svd_cache=svd_cache,
                svd_bins=svd_bins,
                svd_stats=svd_stats,
            )
        )

    elif args.workflow == "filter":
        if not config.paths.filter_dir:
            config.paths.filter_dir = config.paths.storage

        ref_psd_cache = DataCache.find(
            DataType.REFERENCE_PSD, root=config.paths.input_data
        )
        svd_bank_cache = DataCache.find(
            DataType.SVD_BANK, root=config.paths.input_data, svd_bins="*"
        )

        lr_cache = DataCache.generate(
            DataType.LIKELIHOOD_RATIO,
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
            psd_config=config.psd,
            svd_config=config.svd,
            filter_config=config.filter,
            source_config=config.source,
            condor_config=config.condor,
            ref_psd_cache=ref_psd_cache,
            svd_bank_cache=svd_bank_cache,
            lr_cache=lr_cache,
            trigger_cache=trigger_cache,
            svd_stats=svd_stats,
            min_instruments=config.filter.min_instruments_candidates,
        )

        dag.attach(layer)

        marg_lr_cache = DataCache.generate(
            DataType.MARG_LIKELIHOOD_RATIO,
            config.all_ifos,
            config.span,
            svd_bins=lr_cache.groupby("bin").keys(),
            root=config.paths.filter_dir,
        )
        layer = layers.marginalize_likelihood_ratio(
            condor_config=config.condor,
            lr_cache=lr_cache,
            marg_lr_cache=marg_lr_cache,
        )

        dag.attach(layer)

    elif args.workflow == "injection-filter":
        if not config.paths.injection_dir:
            config.paths.injection_dir = config.paths.storage

        ref_psd_cache = DataCache.find(
            DataType.REFERENCE_PSD, root=config.paths.input_data
        )
        svd_bank_cache = DataCache.find(
            DataType.SVD_BANK, root=config.paths.input_data, svd_bins="*"
        )

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
            psd_config=config.psd,
            svd_config=config.svd,
            filter_config=config.filter,
            injection_config=config.injections,
            source_config=config.source,
            condor_config=config.condor,
            ref_psd_cache=ref_psd_cache,
            svd_bank_cache=svd_bank_cache,
            trigger_cache=trigger_cache,
            svd_stats=svd_stats,
            min_instruments=config.filter.min_instruments_candidates,
        )

        dag.attach(layer)

    elif args.workflow == "rank":
        # FIXME: add online chunks

        # initialize empty caches, to which we will
        # add discovered data products
        marg_lr_cache = DataCache(DataType.MARG_LIKELIHOOD_RATIO)
        triggers_cache = DataCache(DataType.TRIGGERS)
        if config.injections:
            inj_triggers_cache = DataCache(DataType.TRIGGERS)
        prior_cache = DataCache(DataType.PRIOR_LIKELIHOOD_RATIO)

        svd_bank_cache = DataCache.find(
            DataType.SVD_BANK, root=config.paths.input_data, svd_bins="*"
        )
        marg_lr_cache += DataCache.find(
            DataType.MARG_LIKELIHOOD_RATIO, root=config.paths.filter_dir, svd_bins="*"
        )
        triggers_cache += DataCache.find(
            DataType.TRIGGERS, root=config.paths.filter_dir, svd_bins="*"
        )
        if config.injections:
            inj_triggers_cache += DataCache.find(
                DataType.TRIGGERS,
                root=config.paths.injection_dir,
                svd_bins="*",
                subtype="*",
            )

        prior_cache = DataCache.generate(
            DataType.PRIOR_LIKELIHOOD_RATIO,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.rank_dir,
        )

        layer = layers.create_prior(
            filter_config=config.filter,
            condor_config=config.condor,
            prior_config=config.prior,
            coincidence_threshold=config.filter.coincidence_threshold,
            svd_bank_cache=svd_bank_cache,
            prior_cache=prior_cache,
            ifos=config.ifos,
            min_instruments=config.filter.min_instruments_candidates,
            svd_stats=svd_stats,
        )
        dag.attach(layer)

        marg_lr_prior_cache = DataCache.generate(
            DataType.MARG_LIKELIHOOD_RATIO_PRIOR,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.rank_dir,
        )

        layer = layers.marginalize_likelihood_ratio(
            condor_config=config.condor,
            lr_cache=marg_lr_cache,
            marg_lr_cache=marg_lr_prior_cache,
            prior_cache=prior_cache,
        )
        dag.attach(layer)

        time_clustered_triggers_cache = DataCache(DataType.CLUSTERED_TRIGGERS)
        time_clustered_triggers_cache += DataCache.generate(
            DataType.CLUSTERED_TRIGGERS,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.rank_dir,
        )
        if config.injections:
            for inj_name, inj_args in config.injections.filter.items():
                min_mchirp, max_mchirp = map(float, inj_args["range"].split(":"))
                svd_bins_inj = mchirp_range_to_bins(min_mchirp, max_mchirp, svd_stats)
                time_clustered_triggers_cache += DataCache.generate(
                    DataType.CLUSTERED_TRIGGERS,
                    config.all_ifos,
                    config.span,
                    svd_bins=svd_bins_inj,
                    subtype=inj_name,
                    root=config.paths.rank_dir,
                )

        all_clustered_triggers_cache = (
            triggers_cache + inj_triggers_cache if config.injections else triggers_cache
        )
        layer = layers.add_trigger_dbs(
            condor_config=config.condor,
            filter_config=config.filter,
            trigger_cache=all_clustered_triggers_cache,
            clustered_trigger_cache=time_clustered_triggers_cache,
            column="network_chisq_weighted_snr",
            window=0.1,
        )
        dag.attach(layer)

        lr_triggers_cache = DataCache(DataType.LR_TRIGGERS)
        lr_triggers_cache += DataCache.generate(
            DataType.LR_TRIGGERS,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.rank_dir,
        )
        if config.injections:
            for inj_name, inj_args in config.injections.filter.items():
                min_mchirp, max_mchirp = map(float, inj_args["range"].split(":"))
                svd_bins_inj = mchirp_range_to_bins(min_mchirp, max_mchirp, svd_stats)
                lr_triggers_cache += DataCache.generate(
                    DataType.LR_TRIGGERS,
                    config.all_ifos,
                    config.span,
                    svd_bins=svd_bins_inj,
                    subtype=inj_name,
                    root=config.paths.rank_dir,
                )
        layer = layers.assign_likelihood(
            condor_config=config.condor,
            filter_config=config.filter,
            prior_config=config.prior,
            trigger_cache=time_clustered_triggers_cache,
            lr_cache=marg_lr_prior_cache,
            lr_trigger_cache=lr_triggers_cache,
            svd_stats=svd_stats,
        )
        dag.attach(layer)

        clustered_lr_triggers_cache = DataCache.generate(
            DataType.LR_TRIGGERS,
            config.all_ifos,
            config.span,
            svd_bins=f"{min(svd_bins)}_{max(svd_bins)}",
            root=config.paths.rank_dir,
        )
        if config.injections:
            for inj_name, inj_args in config.injections.filter.items():
                min_mchirp, max_mchirp = map(float, inj_args["range"].split(":"))
                svd_bins_inj = mchirp_range_to_bins(min_mchirp, max_mchirp, svd_stats)
                clustered_lr_triggers_cache += DataCache.generate(
                    DataType.LR_TRIGGERS,
                    config.all_ifos,
                    config.span,
                    svd_bins=f"{min(svd_bins_inj)}_{max(svd_bins_inj)}",
                    subtype=inj_name,
                    root=config.paths.rank_dir,
                )
        layer = layers.merge_and_reduce(
            condor_config=config.condor,
            filter_config=config.filter,
            trigger_cache=lr_triggers_cache,
            clustered_trigger_cache=clustered_lr_triggers_cache,
            column="likelihood",
            window=4,
        )
        dag.attach(layer)

        num_jobs = (
            config.rank.calc_pdf_jobs if config.rank.calc_pdf_jobs else 1
        )  # jobs per bin
        if num_jobs == 1:
            cache_svd_bins = svd_bins
        else:
            cache_svd_bins = []
            for svd_bin in svd_bins:
                for i in range(num_jobs):
                    cache_svd_bins.append(svd_bin + "_" + str(i))
        pdf_cache = DataCache.generate(
            DataType.RANK_STAT_PDFS,
            config.all_ifos,
            config.span,
            svd_bins=cache_svd_bins,
            root=config.paths.rank_dir,
        )
        layer = layers.calc_pdf(
            condor_config=config.condor,
            prior_config=config.prior,
            rank_config=config.rank,
            config_svd_bins=svd_bins,
            lr_cache=marg_lr_prior_cache,
            pdf_cache=pdf_cache,
            svd_stats=svd_stats,
        )
        dag.attach(layer)

        extinct_pdf_cache = DataCache.generate(
            DataType.RANK_STAT_PDFS,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
            root=config.paths.rank_dir,
        )
        layer = layers.extinct_bin(
            condor_config=config.condor,
            event_config_file=config.filter.event_config_file,
            pdf_cache=pdf_cache,
            trigger_cache=lr_triggers_cache,
            extinct_cache=extinct_pdf_cache,
        )
        dag.attach(layer)

        marg_pdf_cache = DataCache.generate(
            DataType.RANK_STAT_PDFS,
            config.all_ifos,
            config.span,
            root=config.paths.rank_dir,
        )
        layer_list = layers.marginalize_pdf(
            condor_config=config.condor,
            rank_config=config.rank,
            rank_dir=config.paths.rank_dir,
            all_ifos=config.all_ifos,
            span=config.span,
            pdf_cache=extinct_pdf_cache,
            marg_pdf_cache=marg_pdf_cache,
        )
        for layer in layer_list:
            dag.attach(layer)

        post_pdf_cache = DataCache.generate(
            DataType.POST_RANK_STAT_PDFS,
            config.all_ifos,
            config.span,
            root=config.paths.rank_dir,
        )
        far_trigger_cache = DataCache.generate(
            DataType.FAR_TRIGGERS,
            config.all_ifos,
            config.span,
            svd_bins=f"{min(svd_bins)}_{max(svd_bins)}",
            root=config.paths.rank_dir,
        )
        if config.injections:
            for inj_name in config.injections.filter:
                far_trigger_cache += DataCache.generate(
                    DataType.FAR_TRIGGERS,
                    config.all_ifos,
                    config.span,
                    svd_bins=f"{min(svd_bins)}_{max(svd_bins)}",
                    subtype=inj_name,
                    root=config.paths.rank_dir,
                )

        layer_list = layers.assign_far(
            condor_config=config.condor,
            event_config_file=config.filter.event_config_file,
            trigger_cache=clustered_lr_triggers_cache,
            marg_pdf_cache=marg_pdf_cache,
            post_pdf_cache=post_pdf_cache,
            far_trigger_cache=far_trigger_cache,
        )
        for layer in layer_list:
            dag.attach(layer)

        seg_far_trigger_cache = DataCache.generate(
            DataType.SEGMENTS_FAR_TRIGGERS,
            config.all_ifos,
            config.span,
            svd_bins=f"{min(svd_bins)}_{max(svd_bins)}",
            root=config.paths.rank_dir,
        )
        if config.injections:
            for inj_name in config.injections.filter:
                seg_far_trigger_cache += DataCache.generate(
                    DataType.SEGMENTS_FAR_TRIGGERS,
                    config.all_ifos,
                    config.span,
                    svd_bins=f"{min(svd_bins)}_{max(svd_bins)}",
                    subtype=inj_name,
                    root=config.paths.rank_dir,
                )

        if not os.path.exists(config.summary.webdir):
            os.makedirs(config.summary.webdir)

        layer_list = layers.summary_page(
            condor_config=config.condor,
            event_config_file=config.filter.event_config_file,
            segments_file=config.source.frame_segments_file,
            segments_name=config.source.frame_segments_name,
            webdir=config.summary.webdir,
            far_trigger_cache=far_trigger_cache,
            seg_far_trigger_cache=seg_far_trigger_cache,
            post_pdf_cache=post_pdf_cache,
            marg_lr_prior_cache=marg_lr_prior_cache,
            mass_model_file=config.prior.mass_model,
            injections=bool(config.injections),
        )
        for layer in layer_list:
            dag.attach(layer)

        # triggers = dag.add_sim_inspiral_table(triggers)
        # merged_triggers, merged_inj_triggers =
        #           dag.find_injections_lite(merged_triggers)
        # dag.plot_summary(merged_triggers, pdfs)
        # dag.plot_background(merged_triggers, pdfs)
        # dag.plot_bin_background(dist_stats)
        # dag.plot_sensitivity(merged_triggers)
    else:
        raise ValueError(f"Unrecognized workflow: {args.workflow}")

    # Write dag and script to disk
    dag.write(pathlib.Path(args.dag_dir), write_script=True)
    dag.create_log_dir()


if __name__ == "__main__":
    main()
