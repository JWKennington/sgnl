#!/usr/bin/env python3
#
# Copyright (C) 2024 Leo Tsukada
# Copyright (C) 2011--2013 Kipp Cannon, Chad Hanna, Drew Keppel
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

### Compute FAR and FAP distributions from the likelihood CCDFs.

#
# =============================================================================
#
#                                   Preamble
#
# =============================================================================
#


import os
import sys
from argparse import ArgumentParser

import stillsuit
from lal.utils import CacheEntry
from strike.stats import far

process_name = "sgnl-assign-far"


def convert_nanosec2sec(event):
    for trigger in event["trigger"]:
        trigger["time"] *= 1e-9
        trigger["epoch_start"] *= 1e-9
        trigger["epoch_end"] *= 1e-9
    event["event"]["time"] *= 1e-9
    # event["segment"]["start_time"] *= 1e-9
    # event["segment"]["end_time"] *= 1e-9
    return event


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
        "--tmp-space",
        metavar="dir",
        help="Set the name of the tmp space if working with sqlite.",
    )
    parser.add_argument(
        "-i",
        "--input-database-file",
        metavar="filename",
        default=[],
        action="append",
        nargs="+",
        help="Provide the name of an input trigger database. This can be given multiple times, being separated by a space, e.g., --input-database-file filename1 filename2 filename3.",
    )
    parser.add_argument(
        "-o",
        "--output-database-file",
        metavar="filename",
        default=[],
        action="append",
        nargs="+",
        help="Provide the name of an output trigger database. This can be given multiple times, being separated by a space, e.g., --output-database-file filename1 filename2 filename3.",
    )
    parser.add_argument(
        "--input-database-cache",
        metavar="filename",
        help="Also process the files named in this LAL cache.  See \
            lalapps_path2cache for information on how to produce a LAL cache file.",
    )
    parser.add_argument(
        "--output-database-cache",
        metavar="filename",
        help="Also write into the files named in this LAL cache.  See \
            lalapps_path2cache for information on how to produce a LAL cache file.",
    )
    parser.add_argument(
        "-l",
        "--input-likelihood-file",
        metavar="filename",
        help="Set the name of the xml file containing the marginalized likelihood (required).",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force script to re-evaluate FARs and FAPs.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    process_params = options.__dict__.copy()

    if options.input_likelihood_file is None:
        raise ValueError("must set --input-likelihood-file")

    if not options.input_database_file:
        raise ValueError("must provide at least one database file to process")

    if options.input_database_cache:
        options.input_database_file += [
            CacheEntry(line).path for line in open(options.input_database_cache)
        ]
    options.input_database_file = [
        url for sublist in options.input_database_file for url in sublist
    ]

    if options.output_database_cache:
        options.output_database_file += [
            CacheEntry(line).path for line in open(options.output_database_cache)
        ]
    options.output_database_file = [
        url for sublist in options.output_database_file for url in sublist
    ]

    for filename in options.output_database_file:
        if os.path.exists(filename):
            raise ValueError("output database %s already exists" % filename)

    if not len(options.input_database_file) == len(options.output_database_file):
        raise ValueError(
            "The number of each given database are different. There must be one-to-one mapping between input and output dabases."
        )

    return options, process_params


#
# =============================================================================
#
#                                     Main
#
# =============================================================================
#


def main():
    #
    # Parse command line
    #

    options, process_params = parse_command_line()

    #
    # Retrieve distribution data
    #

    rankingstatpdf = far.RankingStatPDF.load(
        options.input_likelihood_file, verbose=options.verbose
    )
    if (rankingstatpdf.zero_lag_lr_lnpdf.array == 0).all():
        raise ValueError(
            "A zerolag histogram is not stored in %s. Make sure to run extinct-bin program in advance and point to post-extinction dist-stat-pdf file."
            % options.input_likelihood_file
        )

    #
    # Apply density estimation to zero-lag rates
    #

    rankingstatpdf.density_estimate_zero_lag_rates()

    #
    # initialize the FAP & FAR assignment machine
    #

    fapfar = far.FAPFAR(rankingstatpdf)

    #
    # Iterate over database
    #

    if options.verbose:
        print("assigning FARs ...", file=sys.stderr)

    for n, (input_database, output_database) in enumerate(
        zip(options.input_database_file, options.output_database_file), start=1
    ):

        if options.verbose:
            print(
                "%d/%d: %s" % (n, len(options.input_database_file), input_database),
                file=sys.stderr,
            )
        indb = stillsuit.StillSuit(config=options.config_schema, dbname=input_database)

        #
        # Check if the FARs have already been populated in the input database
        #
        if (
            not options.force
            and indb.default_cursor.execute(
                """SELECT EXISTS(SELECT * FROM process WHERE program == ?);""",
                (process_name,),
            ).fetchone()[0]
        ):
            if options.verbose:
                print(
                    "already processed, skipping %s" % input_database, file=sys.stderr
                )
            continue

        #
        # record our passage
        #

        indb.default_cursor.execute(
            """UPDATE process SET program = ?;""",
            (process_name,),
        )
        for i, (name, val) in enumerate(process_params.items()):
            indb.default_cursor.execute(
                """
            INSERT INTO process_params (param, program, value)
            VALUES (?, ?, ?);
            """,
                (name, process_name, str(val)),
            )

        #
        # assign FARs
        #

        # FIXME : assign_fapfars is no longer used. this might harm the computation efficiency (?), in which case we might want to revisit this.
        for event in indb.get_events():
            event = convert_nanosec2sec(event)
            indb.default_cursor.execute(
                """
            UPDATE event SET far=? WHERE __event_id=?;
                                        """,
                (
                    float(fapfar.far_from_rank(event["event"]["likelihood"])),
                    event["event"]["__event_id"],
                ),
            )

        #
        # done, file is restored to original location
        #
        # FIXME : figure out how to check in these information for the new CBC db schema using stillsuit package
        # process.set_end_time_now()
        # connection.cursor().execute(
        #     "UPDATE process SET end_time = ? WHERE process_id == ?",
        #     (process.end_time, process.process_id),
        # )

    if options.verbose:
        print("FAR assignment complete for %s" % input_database, file=sys.stderr)
        print("Writing to %s" % output_database, file=sys.stderr)
    indb.to_file(output_database)

    if options.verbose:
        print("Done", file=sys.stderr)


if __name__ == "__main__":
    main()
