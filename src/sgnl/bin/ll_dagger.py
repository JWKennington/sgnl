# Copyright (C) 2020  Patrick Godwin (patrick.godwin@ligo.org)
# Copyright (C) 2025  Yun-Jing Huang (yun-jing.huang@ligo.org)

import argparse
import pathlib
import sys

from ezdag import DAG

from sgnl.dags import layers
from sgnl.dags.config import build_config
from sgnl.dags.util import DataCache, DataType, load_svd_options


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
        dag_name = f"{config.tag}_online_{args.workflow}"

    # Start building the dag
    dag = DAG(dag_name)

    if args.workflow == "setup":
        # Reference PSD layer
        # dag.attach(layers.reference_psd(config.psd, config.condor))

        # Median PSD layer
        # dag.attach(layers.median_psd(config.psd, config.condor))
        # ref_psd = DataCache.from_files(DataType.REFERENCE_PSD,
        # config.paths.reference_psd)
        # split_bank = DataCache.find(DataType.SPLIT_BANK, svd_bins="*", subtype="*")

        # FIXME: use svds from gstlal for now
        svd_bank_cache = DataCache.find(DataType.SVD_BANK, root="filter", svd_bins="*")
        svd_bins, svd_stats = load_svd_options(config.svd.option_file, config.svd)

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
            config.condor,
            config.filter.coincidence_threshold,
            config.prior.mass_model,
            svd_bank_cache,
            prior_cache,
            config.ifos,
            config.filter.min_instruments_candidates,
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
                config.psd,
                config.filter,
                config.upload,
                config.services,
                config.source,
                config.condor,
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
            config.psd,
            config.filter,
            config.upload,
            config.services,
            config.source,
            config.condor,
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
            config.condor,
            config.filter,
            config.services,
            lrs,
            config.tag,
            marg_pdf,
            config.rank.extinct_percent,
            config.rank.fast_burnin,
            config.rank.calc_pdf_cores,
        )
        dag.attach(layer)

        layer = layers.track_noise(
            config.condor,
            config.source,
            config.filter,
            config.psd,
            config.metrics,
            config.services,
            config.ifos,
            config.tag,
            ref_psd_cache,
        )
        dag.attach(layer)

        if config.filter.injections:
            layer = layers.track_noise(
                config.condor,
                config.source,
                config.filter,
                config.psd,
                config.metrics,
                config.services,
                config.ifos,
                config.tag,
                ref_psd_cache,
                injection=True,
            )
            dag.attach(layer)

        if config.services.kafka_server:
            layer = layers.count_events(
                config.condor,
                config.services,
                config.upload,
                config.tag,
                marg_zerolag_pdf,
            )
            dag.attach(layer)

            layer = layers.upload_events(
                config.condor,
                config.upload,
                config.services,
                config.metrics,
                svd_bins,
                config.tag,
            )
            dag.attach(layer)

            # if config.snr_optimizer:
            #     dag.optimize_snr()
            # if config.skymap_optimizer:
            #     dag.optimizer_add_skymap()

            # FIXME: uncomment once we have pastro working
            layer = layers.upload_pastro(
                config.condor,
                config.services,
                config.upload,
                config.pastro,
                config.tag,
                marg_pdf,
            )
            dag.attach(layer)

            layer = layers.plot_events(
                config.condor, config.upload, config.services, config.tag
            )
            dag.attach(layer)

            layer = layers.collect_metrics(
                dag,
                config.condor,
                config.metrics,
                config.services,
                config.filter,
                config.tag,
                config.ifos,
                svd_bins,
            )
            dag.attach(layer)

            layer = layers.collect_metrics_event(
                config.condor,
                config.metrics,
                config.services,
                config.filter,
                config.tag,
            )
            dag.attach(layer)
    else:
        raise ValueError(f"Unrecognized workflow: {args.workflow}")

    # Write dag and script to disk
    dag.write(pathlib.Path(args.dag_dir), write_script=True)
    dag.create_log_dir()


if __name__ == "__main__":
    main()
