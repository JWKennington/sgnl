"""A program to compute the likelhood ratios of inspiral triggers"""

# Copyright (C) 2010--2014  Kipp Cannon, Chad Hanna
# Copyright (C) 2023  Leo Tsukada


#
# =============================================================================
#
#                                   Preamble
#
# =============================================================================
#


import logging
import os
import sys
from argparse import ArgumentParser

import igwn_segments as segments
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.param import use_in as param_use_in
from lal.utils import CacheEntry
from strike.stats import far

from sgnl import sgnlio

process_name = "sgnl-assign-likelihood"


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
        "-i",
        "--input-database-file",
        metavar="filename",
        default=[],
        action="append",
        nargs="+",
        help="Provide the name of an input trigger database. This can be given "
        "multiple times, being separated by a space, e.g., --input-database-file "
        "filename1 filename2 filename3.",
    )
    parser.add_argument(
        "-o",
        "--output-database-file",
        metavar="filename",
        default=[],
        action="append",
        nargs="+",
        help="Provide the name of an output trigger database. This can be given "
        "multiple times, being separated by a space, e.g., --output-database-file "
        "filename1 filename2 filename3.",
    )
    parser.add_argument(
        "--input-database-cache",
        metavar="filename",
        help="Also process the files named in this LAL cache.  See "
        "lalapps_path2cache for information on how to produce a LAL cache file.",
    )
    parser.add_argument(
        "--output-database-cache",
        metavar="filename",
        help="Also write into the files named in this LAL cache.  See "
        "lalapps_path2cache for information on how to produce a LAL cache file.",
    )
    parser.add_argument(
        "-l",
        "--input-likelihood-file",
        metavar="URL",
        action="append",
        nargs="+",
        help="Set the name of the likelihood ratio data file to use. "
        "This can be given multiple times, being separated by a space, e.g., "
        "--input-likelihood-file filename1 filename2 filename3.",
    )
    parser.add_argument(
        "--input-likelihood-cache",
        metavar="filename",
        help="Also load the likelihood ratio data files listsed in this LAL "
        "cache.  See lalapps_path2cache for information on how to produce a "
        "LAL cache file.",
    )
    parser.add_argument(
        "--gstlal",
        action="store_true",
        help="Set if the likelihood ratio data file is in gstlal's format.",
    )
    parser.add_argument(
        "-t",
        "--tmp-space",
        metavar="path",
        help="Path to a directory suitable for use as a work area while "
        "manipulating the database file.  The database file will be worked on in "
        "this directory, and then moved to the final location when complete. "
        "This option is intended to improve performance when running in a "
        "networked environment, where there might be a local disk with higher "
        "bandwidth than is available to the filesystem on which the final output "
        "will reside.",
    )
    parser.add_argument(
        "--vetoes-name",
        metavar="name",
        help="Set the name of the segment lists to use as vetoes (default = do "
        "not apply vetoes).",
    )
    parser.add_argument(
        "--add-zerolag-to-background",
        action="store_true",
        help="Add zerolag events to background before populating coincident "
        "parameter PDF histograms",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force recomputation of likelihood values.",
    )
    parser.add_argument(
        "--verbose-level",
        metavar="name",
        default="INFO",
        help="Give 'DEBUG' if printing out LR components to a .log file",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    paramdict = options.__dict__.copy()

    options.input_likelihood_files = []
    if options.input_likelihood_file is not None:
        options.input_likelihood_files += [
            url for sublist in options.input_likelihood_file for url in sublist
        ]
    if options.input_likelihood_cache is not None:
        options.input_likelihood_files += [
            CacheEntry(line).url for line in open(options.input_likelihood_cache)
        ]
    if not options.input_likelihood_files:
        raise ValueError("no likelihood URLs specified")

    if options.input_database_cache:
        options.input_database_file += [
            [CacheEntry(line).path] for line in open(options.input_database_cache)
        ]
    options.input_database_file = [
        url for sublist in options.input_database_file for url in sublist
    ]

    if options.output_database_cache:
        options.output_database_file += [
            [CacheEntry(line).path] for line in open(options.output_database_cache)
        ]
    options.output_database_file = [
        url for sublist in options.output_database_file for url in sublist
    ]

    for filename in options.output_database_file:
        if os.path.exists(filename):
            raise ValueError("output database %s already exists" % filename)

    if not len(options.input_database_file) == len(options.output_database_file):
        raise ValueError(
            "The number of each given databases are different. There must be "
            "one-to-one mapping between input and output dabases."
        )

    return options, paramdict


#
# =============================================================================
#
#                   Support Funcs for Likelihood Ratio Code
#
# =============================================================================
#


def trigger_veto_func(trigger, vetoseglists):
    # return True if event should be *vetoed*
    if trigger["ifo"] in vetoseglists:
        return trigger["time"] in vetoseglists[trigger["ifo"]]
    else:
        return False


#
# Default content handler for loading RankingStat objects from XML
# documents
#


@array_use_in
@param_use_in
@lsctables.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


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

    options, process_params = parse_command_line()

    #
    # set up logger
    #

    logger = logging.getLogger("LR_components")
    if options.verbose_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(
            filename="assign_likelihood_terms.log",
            filemode="w",
            format="%(name)s - %(levelname)s - %(message)s",
        )
    else:
        logger.setLevel(logging.INFO)

    #
    # load parameter distribution data
    #

    rankingstat = far.marginalize_pdf_urls(
        options.input_likelihood_files, which="LnLikelihoodRatio"
    )

    #
    # apply density estimation kernels, etc.
    # include zero-lag candidates in ranking statistic denominator if requested
    #

    rankingstat.finish(add_zerolag=options.add_zerolag_to_background)

    #
    # iterate over candidate files
    #

    failed = []
    for n, (input_database, output_database) in enumerate(
        zip(options.input_database_file, options.output_database_file), start=1
    ):

        if options.verbose:
            print("%d/%d:" % (n, len(options.input_database_file)), file=sys.stderr)
        try:
            indb = sgnlio.SgnlDB(config=options.config_schema, dbname=input_database)
        except Exception as e:
            if options.verbose:
                print(
                    "failed to load '%s': %s.  trying to continue with remaining files"
                    % (input_database, str(e)),
                    file=sys.stderr,
                )
            failed.append(input_database)
            continue

        #
        # Check if the likelihood ratio have already been populated in the input
        # database
        #

        if (
            not options.force
            and indb.default_cursor.execute(
                """SELECT EXISTS(SELECT * FROM process WHERE program = ?);""",
                (process_name,),
            ).fetchone()[0]
        ):
            if options.verbose:
                print(
                    "already processed, skipping %s" % input_database, file=sys.stderr
                )
            continue

        #
        # record our passage.
        # FIXME can we safely assume the given database always has coinc
        # information? If not, what check should there be here?
        #

        # try:
        #     coinc_def_id = lsctables.CoincDefTable.get_table(xmldoc).get_coinc_def_id(
        #         thinca.InspiralCoincDef.search,
        #         thinca.InspiralCoincDef.search_coinc_type,
        #         create_new=False,
        #     )
        # except KeyError:
        #     if options.verbose:
        #         print(
        #             "document does not contain inspiral coincidences.  skipping.",
        #             file=sys.stderr,
        #         )
        #     xmldoc.unlink()
        #     continue

        indb.default_cursor.execute(
            """UPDATE process SET program = ?;""", (process_name,)
        )
        for name, val in process_params.items():
            indb.default_cursor.execute(
                """
            INSERT INTO process_params (param, program, value)
            VALUES (?, ?, ?);
            """,
                (name, process_name, str(val)),
            )

        # FIXME : at that moment, there is no time slide. We might revisit
        # this later. It is likely that the mapping in get_events() needs to be
        # modified and nothing here should change.
        offset_vectors = {instrument: 0 for instrument in rankingstat.instruments}

        if options.vetoes_name is not None:
            # FIXME : can we get xmldoc like structure from the new db schema so
            # we can reuse ligolw_segments module?
            # vetoseglists = ligolw_segments.segmenttable_get_by_name(
            #     xmldoc, options.vetoes_name
            # ).coalesce()
            raise ValueError("vetoing feature has not implemented yet.")
        else:
            vetoseglists = segments.segmentlistdict()
        if options.verbose:
            print("done", file=sys.stderr)

        #
        # extract coinc-trigger mapping information
        #

        events = []
        for event in indb.get_events(nanosec_to_sec=True, template_duration=True):
            events += [(event["event"]["__event_id"], event["trigger"])]

        if options.verbose_level == "DEBUG":
            print(
                "Warning: dumping %d events of LR term information into a log file..."
                % len(events),
                file=sys.stderr,
            )

        #
        # assign likelihood ratio
        #

        # FIXME : calc_likelihood.assign_likelihood_ratios_xml is no longer
        # used. this might harm the computation efficiency (?), in which case we
        # might want to revisit this.
        for event_id, triggers in events:
            if any(trigger_veto_func(trigger, vetoseglists) for trigger in triggers):
                continue
            indb.default_cursor.execute(
                """
            UPDATE event SET likelihood=? WHERE __event_id=?;
                                        """,
                (
                    float(rankingstat.ln_lr_from_triggers(triggers, offset_vectors)),
                    event_id,
                ),
            )

        #
        # close out process metadata.
        #

        # FIXME : figure out how to check in these information for the new CBC db
        #         schema using stillsuit package
        # process.set_end_time_now()
        # connection.cursor().execute(
        #     "UPDATE process SET end_time = ? WHERE process_id == ?",
        #     (process.end_time, process.process_id),
        # )

        if options.verbose:
            print(
                "likelihood assignment complete for %s" % input_database,
                file=sys.stderr,
            )
            print("Writing to %s" % output_database, file=sys.stderr)
        indb.to_file(output_database)

    #
    # crash if any input files were broken, otherwise exit the program
    #

    if failed:
        raise ValueError(
            "%s could not be processed" % ", ".join("'%s'" % url for url in failed)
        )
    elif options.verbose:
        print("Done", file=sys.stderr)


if __name__ == "__main__":
    main()
