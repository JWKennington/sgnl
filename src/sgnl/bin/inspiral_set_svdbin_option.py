"""An executable to add additional metadata to svd options file."""

# Copyright (C) 2023  Leo Tsukada (leo.tsukada@ligo.org)
# Copyright (C) 2025  Cody Messick (cody.messick@ligo.org)

import glob
import json
import os
from argparse import ArgumentParser
from collections.abc import Mapping
from typing import Iterable

import numpy
from lal import rate

from sgnl.dags.config import build_config
from sgnl.dags.util import load_svd_options


def parse_command_line():
    parser = ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="Sets the path to read configuration from."
    )
    parser.add_argument(
        "-w",
        "--workflow",
        default="full",
        help="Sets the type of workflow for which to validate config file.",
    )
    parser.add_argument(
        "--dag-dir",
        type=str,
        default=".",
        help="The directory in which to write the dag",
    )

    return parser.parse_args()


def calc_gate_threshold(config, svd_bin, svd_stats, aggregate="max"):
    """
    Given a configuration, svd bin and aggregate, this calculates
    the h(t) gate threshold used for a given svd bin.
    """
    if isinstance(config.filter.ht_gate_threshold, str):
        bank_mchirp = svd_stats["bins"][svd_bin][f"{aggregate}_mchirp"]
        min_mchirp, min_threshold, max_mchirp, max_threshold = [
            float(y)
            for x in config.filter.ht_gate_threshold.split("-")
            for y in x.split(":")
        ]
        gate_mchirp_ratio = (max_threshold - min_threshold) / (max_mchirp - min_mchirp)
        threshold = round(
            gate_mchirp_ratio * (bank_mchirp - min_mchirp) + min_threshold, 3
        )
    else:  # uniform threshold
        threshold = config.filter.ht_gate_threshold
    svd_stats.bins[svd_bin]["ht_gate_threshold"] = threshold
    return threshold


def autocorrelation_length_map(ac_length_range):
    """
    Given autocorrelation length ranges (e.g. 0:15:701)
    or a single autocorrelation value, returns a function that
    maps a given chirp mass to an autocorrelation length.
    """
    if isinstance(ac_length_range, str):
        ac_length_range = [ac_length_range]

    # handle case with AC length ranges
    if isinstance(ac_length_range, Iterable):
        ac_lengths = []
        min_mchirps = []
        max_mchirps = []
        for this_range in ac_length_range:
            min_mchirp, max_mchirp, ac_length = this_range.split(":")
            min_mchirps.append(float(min_mchirp))
            max_mchirps.append(float(max_mchirp))
            ac_lengths.append(int(ac_length))

        # sanity check inputs
        for bound1, bound2 in zip(min_mchirps[1:], max_mchirps[:-1]):
            assert bound1 == bound2, "gaps not allowed in autocorrelation length ranges"

        # convert to binning
        bins = rate.IrregularBins([min_mchirps[0]] + max_mchirps)

    # handle single value case
    else:
        ac_lengths = [ac_length_range]
        bins = rate.IrregularBins([0.0, numpy.inf])

    # create mapping
    def mchirp_to_ac_length(mchirp):
        idx = bins[mchirp]
        return ac_lengths[idx]

    return mchirp_to_ac_length


def svd_bin_to_dtdphi_file(config, svd_bin, stats_bin, aggregate="mean"):
    category_condition = {
        "IMBH": lambda stats_bin: stats_bin[f"{aggregate}_mtotal"] > 100
        and stats_bin[f"{aggregate}_mratio"] < 10,
        "others": lambda stats_bin: True,
    }

    if isinstance(config.prior.dtdphi_file, Mapping):
        if "bank_name" in stats_bin:
            sub_bank = stats_bin["bank_name"]
            dtdphi_file = config.prior.dtdphi_file[sub_bank]
        else:
            assert all(
                category in category_condition
                for category in config.prior.dtdphi_file
                if category != "others"
            ), (
                "At least one of the categories set in config, [%s], is not "
                "defined in those in the source code, [%s]."
                % (
                    ",".join(config.prior.dtdphi_file.keys()),
                    ",".join(category_condition.keys()),
                )
            )
            dtdphi_files = {
                category: filename
                for category, filename in config.prior.dtdphi_file.items()
                if category_condition[category](stats_bin)
            }
            if not len(dtdphi_files):
                raise ValueError(
                    "SVD bin %s does not meet a condition of any category "
                    "given in config.prior.dtdphi_file option. Add 'others' "
                    "category explicitly and point that to the default dtdphi "
                    "file to catch such bins." % (svd_bin,)
                )
            elif "others" in dtdphi_files:
                # pick the default dtdphi file from 'others' category
                dtdphi_file = dtdphi_files.pop("others")
            if len(dtdphi_files) >= 2:
                raise ValueError(
                    "SVD bin id %s falls onto the multiple categories (%s). "
                    "It needs to be assigned to only one or none."
                    % (svd_bin, ",".join([tpl[0] for tpl in dtdphi_files]))
                )
            elif len(dtdphi_files):
                # pick the dtdphi file specified in the config
                dtdphi_file = [*dtdphi_files.values()][0]
            assert (
                "dtdphi_file" in locals()
            ), "dtdphi_file is not defined even after the validation"
    else:
        dtdphi_file = config.prior.dtdphi_file

    return dtdphi_file


def main():
    args = parse_command_line()
    config = build_config(args.config, args.dag_dir)
    _, svd_stats = load_svd_options(config.svd.option_file, config.svd)

    # set up autocorrelation mapping
    mchirp_to_ac_length = autocorrelation_length_map(config.svd.autocorrelation_length)

    for svd_bin, stats_bin in svd_stats.bins.items():
        stats_bin["ac_length"] = mchirp_to_ac_length(stats_bin["mean_mchirp"])
        stats_bin["ht_gate_threshold"] = calc_gate_threshold(config, svd_bin, svd_stats)
        stats_bin["mass_model_file"] = config.prior.mass_model
        if config.prior.idq_timeseries:
            stats_bin["idq_file"] = config.prior.idq_timeseries
        if config.prior.dtdphi_file:
            stats_bin["dtdphi_file"] = svd_bin_to_dtdphi_file(
                config, svd_bin, stats_bin
            )
        if config.filter.reconstruction_segment:
            # FIXME : find a more elegant way to find the files.
            segment_file = glob.glob(
                os.path.join(
                    config.filter.reconstruction_segment,
                    f"*-{svd_bin}_*SEGMENTS-*.xml.gz",
                )
            )
            if len(segment_file) == 0:
                raise ValueError(
                    "No segment file is found in %s"
                    % config.filter.reconstruction_segment
                )
            elif len(segment_file) > 1:
                raise ValueError(
                    "more than one segment file are found in %s"
                    % config.filter.reconstruction_segment
                )
            elif len(segment_file) == 1:
                stats_bin["reconstruction_segment"] = segment_file[0]

        # overwrite options given in seed_likelihood for online rerank
        if config.prior.seed_likelihood:
            for key, option in config.prior.seed_likelihood.items():
                if key != "mass_model":
                    stats_bin[key] = option
                elif config.prior.seed_likelihood.mass_model:
                    stats_bin["mass_model_file"] = (
                        config.prior.seed_likelihood.mass_model
                    )
                    stats_bin["old_mass_model_file"] = config.prior.mass_model

    with open(config.svd.option_file, "w") as jsf:
        jsf.write(json.dumps(svd_stats, sort_keys=True, indent=4))
