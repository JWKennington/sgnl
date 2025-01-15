"""A program to create some prior likelihood data to seed an offline analysis"""

# Copyright (C) 2010--2014  Kipp Cannon, Chad Hanna
# Copyright (C) 2024 Leo Tsukada


#
# =============================================================================
#
#                                   Preamble
#
# =============================================================================
#


import warnings
from argparse import ArgumentParser
from collections import defaultdict

import numpy
from lal.utils import CacheEntry
from ligo.lw import array as ligolw_array
from ligo.lw import ligolw, lsctables
from ligo.lw import param as ligolw_param
from strike.stats import far, likelihood_ratio

from sgnl import svd_bank


@ligolw_array.use_in
@ligolw_param.use_in
@lsctables.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


#
# =============================================================================
#
#                                 Command Line
#
# =============================================================================
#


def parse_command_line():
    parser = ArgumentParser()
    parser.add_argument(
        "--coincidence-threshold",
        metavar="value",
        type=float,
        default=0.005,
        help="Set the coincidence window in seconds (default = 0.005).  The "
        "light-travel time between instruments will be added automatically in the "
        "coincidence test.",
    )
    parser.add_argument(
        "--min-instruments",
        metavar="count",
        type=int,
        default=2,
        help="Set the minimum number of instruments that must contribute triggers to "
        "form a candidate (default = 2).",
    )
    parser.add_argument(
        "--output-likelihood-file",
        metavar="filename",
        action="append",
        help="Write merged raw likelihood data to this LIKELIHOOD_RATIO file.",
    )
    parser.add_argument(
        "--seed-likelihood",
        metavar="filename",
        help="Start with a likelihood file and only update certain components. This is "
        "incompatible with --coincidence-threshold, --min-instruments, --instrument, "
        "and --background-prior so these options will be ignored",
    )
    parser.add_argument(
        "--instrument",
        action="append",
        help="Append to a list of instruments to create likelihood ratio class for. "
        "List must be whatever instruments you intend to analyze. This can be given "
        "multiple times, being separated by a space, e.g., --instrument ifo1 ifo2 ifo3",
    )
    parser.add_argument(
        "--svd-file",
        metavar="filename",
        action="append",
        default=[],
        help="The SVD file to read the template ids from. This can be given multiple "
        "times, being separated by a space, e.g., --svd-file filename1 filename2 "
        "filename3.",
    )
    parser.add_argument(
        "--mass-model-file",
        metavar="filename",
        help="The mass model file to read from (hdf5 format)",
    )
    parser.add_argument(
        "--dtdphi-file",
        metavar="filename",
        help="dtdphi snr ratio pdfs to read from (hdf5 format). Default : the one "
        "stored in the build.",
    )
    parser.add_argument(
        "--idq-file", metavar="filename", help="idq glitch file (hdf5 format)"
    )
    parser.add_argument(
        "--mismatch-min",
        metavar="float",
        default=0.001,
        type=float,
        help="Set minimum mismatch factor for chisq signal model. (default = 0.001)",
    )
    parser.add_argument(
        "--mismatch-max",
        metavar="float",
        default=0.3,
        type=float,
        help="Set maximum mismatch factor for chisq signal model. (default = 0.3)",
    )
    parser.add_argument(
        "--write-empty-rankingstatpdf",
        metavar="filename",
        action="append",
        help="If provided, the rankingstat created in this script will be used to "
        "bootstrap the creation of an empty rankingstatpdf. This option is meant to be "
        "used during the setup stage of an online analysis to create the zerolag pdf "
        "file.",
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    process_params = dict(options.__dict__)

    template_ids = []
    horizon_factors = {}
    lambda_eta_sums_label = (
        "lambda_sum",
        "lambdasq_sum",
        "lambda_etasq_sum",
        "lambdasq_etasq_sum",
    )
    lambda_eta_sums_median = {
        label: defaultdict(dict) for label in lambda_eta_sums_label
    }

    for svd_file in options.svd_file:
        if svd_file.endswith("xml") or svd_file.endswith("xml.gz"):
            svd_ifo = CacheEntry.from_T050017(svd_file).observatory
            horizon_factors_ifo = {}
            lambda_eta_sums = {
                "chisq": defaultdict(list),
                "combochisq": defaultdict(list),
            }
            for bank in svd_bank.read_banks(
                svd_file,
                contenthandler=LIGOLWContentHandler,
                verbose=options.verbose,
            ):
                template_ids += [row.template_id for row in bank.sngl_inspiral_table]
                for i, chisq in enumerate(lambda_eta_sums.keys()):
                    for j, label in enumerate(lambda_eta_sums_label):
                        col_index = str(int(4 * i + j + 2))
                        col = f"Gamma{col_index}"
                        lambda_eta_list = [
                            getattr(row, col) for row in bank.sngl_inspiral_table
                        ]
                        if numpy.all(
                            numpy.array(lambda_eta_list == 0)
                        ) and col_index in ("2", "3", "4", "5"):
                            raise ValueError(
                                "Column '%s' in %s is all zero. Check the svd bank "
                                % (col, svd_file),
                                "config or the column name.",
                            )
                        else:
                            lambda_eta_sums[chisq][label] += lambda_eta_list
                horizon_factors_ifo.update(bank.horizon_factors)
            for chisq, lambda_eta_sums_dict in lambda_eta_sums.items():
                for label, lambda_eta_sum in lambda_eta_sums_dict.items():
                    lambda_eta_sums_median[label][chisq][svd_ifo] = numpy.median(
                        lambda_eta_sum
                    )
            horizon_factors.update({svd_ifo: horizon_factors_ifo})
        else:
            raise ValueError("svd file cannot be read")

    if options.seed_likelihood:
        warnings.warn(
            "--seed-likelihood given, the following options  will be ignored: "
            + "--coincidence-threshold, --min-instruments, --instrument, and "
            + "--background-prior",
            stacklevel=2,
        )
        options.coincidence_threshold = None
        options.min_instruments = None
        options.instrument = None
    else:
        if not options.instrument:
            raise ValueError("must specify at least one --instrument")
        options.instrument = set(options.instrument)
        if options.min_instruments < 1:
            raise ValueError("--min-instruments must be >= 1")
        if options.min_instruments > len(options.instrument):
            raise ValueError(
                "--min-instruments is greater than the number of unique --instrument's"
            )

    return (
        options,
        process_params,
        template_ids,
        horizon_factors,
        lambda_eta_sums_median,
    )


#
# =============================================================================
#
#                                     Main
#
# =============================================================================
#


def main():
    #
    # command line
    #

    (
        options,
        process_params,
        template_ids,
        horizon_factors,
        lambda_eta_sums_median,
    ) = parse_command_line()

    #
    # Either make a new file or use a seed which contains background data from a real
    # analysis
    #

    if not options.seed_likelihood:
        #
        # add the denominator if starting from scratch
        #
        rankingstat = likelihood_ratio.LnLikelihoodRatio(
            template_ids=template_ids,
            instruments=options.instrument,
            min_instruments=options.min_instruments,
            population_model_file=options.mass_model_file,
            dtdphi_file=options.dtdphi_file,
            delta_t=options.coincidence_threshold,
            horizon_factors=horizon_factors,
            idq_file=options.idq_file,
        )
    else:
        #
        # Otherwise we have a seed and we will only override signal options for e.g., a
        # rerank
        #
        rankingstat = likelihood_ratio.LnLikelihoodRatio.load(
            url=options.seed_likelihood,
            verbose=options.verbose,
        )
        if options.mass_model_file is not None:
            rankingstat.population_model_file = options.mass_model_file
        if options.dtdphi_file is not None:
            rankingstat.dtdphi_file = options.dtdphi_file
        if options.idq_file is not None:
            rankingstat.idq_file = options.idq_file

    #
    # Add the signal model
    #

    mismatch_range = (options.mismatch_min, options.mismatch_max)
    rankingstat.terms["P_of_SNR_chisq"].add_snr_chisq_signal_model(
        lambda_sum=lambda_eta_sums_median["lambda_sum"],
        lambdasq_sum=lambda_eta_sums_median["lambdasq_sum"],
        lambda_etasq_sum=lambda_eta_sums_median["lambda_etasq_sum"],
        lambdasq_etasq_sum=lambda_eta_sums_median["lambdasq_etasq_sum"],
        mismatch_range=mismatch_range,
        verbose=options.verbose,
    )

    #
    # if provided, create an empty rankingstatpdf too
    #

    for filename in options.write_empty_rankingstatpdf:
        if options.write_empty_rankingstatpdf:
            rankingstatpdf = far.RankingStatPDF(rankingstat, nsamples=0)
            rankingstatpdf.save(
                filename,
                process_name="sgnl-create-prior-diststats",
                process_params=process_params,
                verbose=options.verbose,
            )

    #
    # record results in output file
    #

    for filename in options.output_likelihood_file:
        rankingstat.save(
            url=filename,
            process_name="sgnl-create-prior-diststats",
            process_params=process_params,
            verbose=options.verbose,
        )
