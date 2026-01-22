# Copyright (C) 2020  Patrick Godwin (patrick.godwin@ligo.org)
# Copyright (C) 2025  Yun-Jing Huang (yun-jing.huang@ligo.org)

import argparse
import pathlib
import sys

from ezdag import DAG

from sgnl.bin import inspiral_bank_splitter, inspiral_set_svdbin_option
from sgnl.dags import layers
from sgnl.dags.config import build_config
from sgnl.dags.util import DataCache, DataType, load_svd_options


def parse_command_line(args: list[str] | None = None) -> argparse.Namespace:
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
        "-w", "--workflow", type=str, required=False, help="The type of dag to generate"
    )
    parser.add_argument(
        "--dag-dir",
        type=str,
        default=".",
        help="The directory in which to write the dag",
    )
    parser.add_argument("--dag-name", type=str, default=None, help="A name for the dag")
    parsed_args = parser.parse_args(args)

    if parsed_args.dag_name:
        if "/" in parsed_args.dag_name:
            raise ValueError(
                "The dag name must not be a path. Use --dag-dir to put the dag in a"
                " different directory."
            )
        if parsed_args.dag_name.endswith(".dag"):
            raise ValueError(
                'The given dag name ends with ".dag". This is unnecessary because it'
                " will be appended to the file name automatically."
            )

    if parsed_args.init is True and parsed_args.workflow is not None:
        raise ValueError("Cannot specify both --init and --workflow.")
    elif parsed_args.init is False and parsed_args.workflow is None:
        raise ValueError("Must specify either --init or --workflow.")

    return parsed_args


def main():
    args = parse_command_line()
    config = build_config(args.config, args.dag_dir)

    if args.init:
        arguments = {
            "bank_name": None,
            "f_final": config.svd.max_f_final,
            "f_low": config.svd.f_low,
            "filename": config.paths.template_bank,
            "instrument": config.instruments,
            "n": config.svd.num_split_templates,
            "num_banks": (
                config.svd.num_banks
                if isinstance(config.svd.num_banks, list)
                else [config.svd.num_banks]
            ),
            "output_path": str(pathlib.Path(args.dag_dir) / "split_bank"),
            "stats_file": config.svd.option_file,
            "verbose": True,
        }

        if config.svd.sort_by:
            arguments["sort_by"] = config.svd.sort_by

        if config.svd.sort_by == "chi":
            arguments["group_by_chi"] = config.svd.num_chi_bins

        if config.svd.sort_by == "mu":
            arguments["group_by_mu"] = config.svd.num_mu_bins

        if config.svd.overlap:
            arguments["overlap"] = config.svd.overlap

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

    if args.dag_name:
        dag_name = args.dag_name
    else:
        dag_name = f"{config.tag}_online_{args.workflow}"

    # Start building the dag
    dag = DAG(dag_name)

    if args.workflow == "setup":
        ref_psd_cache = DataCache.from_files(
            DataType.REFERENCE_PSD, config.paths.reference_psd
        )
        split_bank_cache = DataCache.find(
            DataType.SPLIT_BANK, svd_bins="*", subtype="*"
        )

        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)
        svd_bank_cache = DataCache.generate(
            DataType.SVD_BANK,
            config.ifos,
            config.span,
            svd_bins=svd_bins,
            root="filter",
        )

        dag.attach(
            layers.svd_bank(
                svd_config=config.svd,
                condor_config=config.condor,
                all_ifos=list(sorted(config.all_ifos)),
                split_bank_cache=split_bank_cache,
                median_psd_cache=ref_psd_cache,
                svd_cache=svd_bank_cache,
                svd_bins=svd_bins,
                svd_stats=svd_stats,
            )
        )
        # FIXME when is this used?
        # if config.svd.checkerboard:
        #    svd_bank = dag.checkerboard(ref_psd, svd_bank)

        prior_cache = DataCache.generate(
            DataType.LIKELIHOOD_RATIO,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
        )

        zerolag_pdf_cache = DataCache.generate(
            DataType.ZEROLAG_RANK_STAT_PDFS,
            config.all_ifos,
            svd_bins=svd_bins,
        )

        marg_zerolag_pdf_cache = DataCache.generate(
            DataType.ZEROLAG_RANK_STAT_PDFS,
            config.all_ifos,
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
            write_empty_zerolag=zerolag_pdf_cache,
            write_empty_marg_zerolag=marg_zerolag_pdf_cache,
        )
        dag.attach(layer)

    elif args.workflow == "setup-prior":
        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)
        svd_bank_cache = DataCache.find(DataType.SVD_BANK, root="filter", svd_bins="*")

        prior_cache = DataCache.generate(
            DataType.LIKELIHOOD_RATIO,
            config.all_ifos,
            config.span,
            svd_bins=svd_bins,
        )

        zerolag_pdf_cache = DataCache.generate(
            DataType.ZEROLAG_RANK_STAT_PDFS,
            config.all_ifos,
            svd_bins=svd_bins,
        )

        marg_zerolag_pdf_cache = DataCache.generate(
            DataType.ZEROLAG_RANK_STAT_PDFS,
            config.all_ifos,
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
            write_empty_zerolag=zerolag_pdf_cache,
            write_empty_marg_zerolag=marg_zerolag_pdf_cache,
        )
        dag.attach(layer)

    elif args.workflow == "inspiral":
        # input data products
        ref_psd_cache = config.paths.reference_psd
        svd_banks = DataCache.find(DataType.SVD_BANK, root="filter", svd_bins="*")
        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)
        lrs = DataCache.find(DataType.LIKELIHOOD_RATIO, svd_bins="*")
        marg_zerolag_pdf = DataCache.find(
            DataType.ZEROLAG_RANK_STAT_PDFS
        )  # empty file created by create_prior layer
        assert (
            len(marg_zerolag_pdf) == 1
        ), "Exactly 1 marginalized zerolag pdf must be created by the setup dag. "
        f"Currently found {len(marg_zerolag_pdf)}"

        zerolag_pdfs = DataCache.find(
            DataType.ZEROLAG_RANK_STAT_PDFS,
            svd_bins="*",
        )  # empty files

        marg_pdf = DataCache.generate(DataType.RANK_STAT_PDFS, config.all_ifos)

        # generate dag layers
        if config.filter.injections:
            layer = layers.injection_filter_online(
                psd_config=config.psd,
                filter_config=config.filter,
                upload_config=config.upload,
                services_config=config.services,
                source_config=config.source,
                condor_config=config.condor,
                ref_psd_cache=ref_psd_cache,
                svd_bank_cache=svd_banks,
                lr_cache=lrs,
                svd_stats=svd_stats,
                marg_pdf_cache=marg_pdf,
                ifos=config.ifos,
                tag=config.tag,
                min_instruments=config.filter.min_instruments_candidates,
            )
            dag.attach(layer)

        layer = layers.filter_online(
            psd_config=config.psd,
            filter_config=config.filter,
            upload_config=config.upload,
            services_config=config.services,
            source_config=config.source,
            condor_config=config.condor,
            ref_psd_cache=ref_psd_cache,
            svd_bank_cache=svd_banks,
            lr_cache=lrs,
            svd_stats=svd_stats,
            zerolag_pdf_cache=zerolag_pdfs,
            marg_pdf_cache=marg_pdf,
            ifos=config.ifos,
            tag=config.tag,
            min_instruments=config.filter.min_instruments_candidates,
        )
        dag.attach(layer)

        layer = layers.marginalize_online(
            condor_config=config.condor,
            filter_config=config.filter,
            services_config=config.services,
            lr_cache=lrs,
            tag=config.tag,
            marg_pdf_cache=marg_pdf,
            extinct_percent=config.rank.extinct_percent,
            fast_burnin=config.rank.fast_burnin,
            calc_pdf_cores=config.rank.calc_pdf_cores,
        )
        dag.attach(layer)

        layer = layers.track_noise(
            condor_config=config.condor,
            source_config=config.source,
            filter_config=config.filter,
            psd_config=config.psd,
            metrics_config=config.metrics,
            services_config=config.services,
            ifos=config.ifos,
            tag=config.tag,
            ref_psd=ref_psd_cache,
        )
        dag.attach(layer)

        if config.filter.injections:
            layer = layers.track_noise(
                condor_config=config.condor,
                source_config=config.source,
                filter_config=config.filter,
                psd_config=config.psd,
                metrics_config=config.metrics,
                services_config=config.services,
                ifos=config.ifos,
                tag=config.tag,
                ref_psd=ref_psd_cache,
                injection=True,
            )
            dag.attach(layer)

        if config.services.kafka_server:
            layer = layers.count_events(
                condor_config=config.condor,
                services_config=config.services,
                upload_config=config.upload,
                tag=config.tag,
                zerolag_pdf=marg_zerolag_pdf,
            )
            dag.attach(layer)

            layer = layers.upload_events(
                condor_config=config.condor,
                upload_config=config.upload,
                services_config=config.services,
                metrics_config=config.metrics,
                svd_bins=svd_bins,
                tag=config.tag,
            )
            dag.attach(layer)

            # if config.snr_optimizer:
            #     dag.optimize_snr()
            # if config.skymap_optimizer:
            #     dag.optimizer_add_skymap()

            # FIXME: uncomment once we have pastro working
            layer = layers.upload_pastro(
                condor_config=config.condor,
                services_config=config.services,
                upload_config=config.upload,
                pastro_config=config.pastro,
                tag=config.tag,
                marg_pdf_cache=marg_pdf,
            )
            dag.attach(layer)

            layer = layers.plot_events(
                condor_config=config.condor,
                upload_config=config.upload,
                services_config=config.services,
                tag=config.tag,
            )
            dag.attach(layer)

            layer = layers.collect_metrics(
                condor_config=config.condor,
                metrics_config=config.metrics,
                services_config=config.services,
                filter_config=config.filter,
                tag=config.tag,
                ifos=config.ifos,
                svd_bins=svd_bins,
            )
            dag.attach(layer)

            layer = layers.collect_metrics_event(
                condor_config=config.condor,
                metrics_config=config.metrics,
                services_config=config.services,
                filter_config=config.filter,
                tag=config.tag,
            )
            dag.attach(layer)
    else:
        raise ValueError(f"Unrecognized workflow: {args.workflow}")

    # Write dag and script to disk
    dag.write(pathlib.Path(args.dag_dir), write_script=True)
    dag.create_log_dir()


if __name__ == "__main__":
    main()
