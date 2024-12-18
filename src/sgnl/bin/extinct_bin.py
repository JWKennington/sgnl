"""Populate zerolag pdf and apply the extinction model"""

# Copyright (C) 2023 Prathamesh Joshi
# Copyright (C) 2024 Leo Tsukada


#
# =============================================================================
#
#                                   Preamble
#
# =============================================================================
#


import sys
from argparse import ArgumentParser

import numpy
import scipy.optimize
import stillsuit
from strike.stats import far

#
# =============================================================================
#
#                                 Command Line
#
# =============================================================================
#


def parse_command_line():
    parser = ArgumentParser()
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument(
        "--input-database-file",
        metavar="filename",
        help="Input trigger database from which to add zerolag. Optional",
    )
    parser.add_argument(
        "--input-rankingstatpdf-file",
        metavar="filename",
        action="append",
        nargs="+",
        default=[],
        help="Set the name of the input RANK_STAT_PDF file. This can be given multiple times, being separated by a space, e.g., --input-rankingstatpdf-file filename1 filename2 filename3.",
    )
    parser.add_argument(
        "--output-rankingstatpdf-file",
        metavar="filename",
        help="Set the name of the output RANK_STAT_PDF file. If none is provided, the input file will be overwritten",
    )
    parser.add_argument(
        "--reset-zerolag",
        action="store_true",
        default=False,
        help="Reset the zerolag to zero after performing the extinction",
    )
    parser.add_argument(
        "--initial-exponent",
        default=1.0,
        help="Set the initial guess for the exponent during extinction fitting",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    options.input_rankingstatpdf_file = [
        url for sublist in options.input_rankingstatpdf_file for url in sublist
    ]

    if options.output_rankingstatpdf_file is None:
        options.output_rankingstatpdf_file = options.input_rankingstatpdf_file[0]
    return options


#
# =============================================================================
#
#                                     Main
#
# =============================================================================
#


def main():

    options = parse_command_line()

    # load and marginalize the rankingstatpdf
    rankingstatpdf = far.marginalize_pdf_urls(
        options.input_rankingstatpdf_file, which="RankingStatPDF"
    )

    if options.input_database_file is not None:
        db = stillsuit.StillSuit(
            config=options.config_schema, dbname=options.input_database_file
        )
        rankingstatpdf.collect_zero_lag_rates(db)

    bg = rankingstatpdf.noise_lr_lnpdf.array
    fg = rankingstatpdf.zero_lag_lr_lnpdf.array

    # zero out the beginning bins of each because they are notoriously bad and should just be ignored
    bg[:10] = 0.0
    fg[:10] = 0.0

    # fitting is done between ix_min and ix_max
    fg_ccdf = numpy.cumsum(fg[::-1])[::-1]
    ix_min = (fg_ccdf < fg_ccdf[0] / 2.0).argmax()
    ix_max = (fg_ccdf < fg_ccdf[0] / 100.0).argmax()

    bgtotal = bg[ix_min : ix_max + 1].sum()
    bg_ccdf = numpy.cumsum(bg[::-1])[::-1] / bgtotal

    # define a function for the extincted bg for scipy.optimize.curve_fit to call
    def bg_ccdf_extinct_func(idx, c, A):
        return numpy.log(A * bgtotal / c) + numpy.log1p(
            -numpy.exp(-1 * bg_ccdf[idx] * c)
        )

    # find the best fit c for extinction
    c = scipy.optimize.curve_fit(
        bg_ccdf_extinct_func,
        range(ix_min, ix_max + 1),
        numpy.log(fg_ccdf[ix_min : ix_max + 1]),
        p0=[float(options.initial_exponent), 1],
        bounds=[0, numpy.inf],
        sigma=numpy.sqrt(1 / (bg_ccdf[ix_min : ix_max + 1] * bgtotal)),
    )  # , maxfev = 5000)
    print(
        f"Best value of c is {c[0][0]}, A is {c[0][1]} with covariance {c[1]}",
        file=sys.stderr,
    )

    # calculate the extincted PDF
    bg_pdf_extinct = c[0][1] * bg * numpy.exp(-1 * bg_ccdf * c[0][0])

    rankingstatpdf.noise_lr_lnpdf.array = bg_pdf_extinct
    rankingstatpdf.noise_lr_lnpdf.normalize()

    if options.reset_zerolag:
        for i in range(len(rankingstatpdf.zero_lag_lr_lnpdf.array)):
            rankingstatpdf.zero_lag_lr_lnpdf.array[i] = 0.0

    rankingstatpdf.save(
        options.output_rankingstatpdf_file,
        process_name="sgnl-extinct-bin",
        process_params=options.__dict__,
    )


if __name__ == "__main__":
    main()
