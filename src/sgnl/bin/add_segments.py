"""An executable to add segments table to trigger databases."""

# Copyright (C) 2024-2025 Yun-Jing Huang

from argparse import ArgumentParser

from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.utils import segments as ligolw_segments

from sgnl import sgnlio

process_name = "sgnl-add-segments"


#
# =============================================================================
#
#                                 Command Line
#
# =============================================================================
#


def parse_command_line():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument(
        "-i",
        "--input-database-file",
        metavar="filename",
        action="store",
        required=True,
        help="Provide the name of an input trigger database.",
    )
    parser.add_argument(
        "-o",
        "--output-database-file",
        metavar="filename",
        required=True,
        help="Provide the name of an output trigger database.",
    )
    parser.add_argument(
        "--segments-file",
        metavar="filename",
        required=True,
        help="Provide the segments file name.",
    )
    parser.add_argument(
        "--segments-name",
        metavar="name",
        required=True,
        help="Provide the name of the segments to add to the database.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    process_params = options.__dict__.copy()

    return options, process_params


def init_config_row(self, table, extra=None):
    out = {c["name"]: None for c in table["columns"] if not c["name"].startswith("__")}
    if extra is not None:
        out.update(extra)
    return out


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
    # Iterate over database
    #

    indb = sgnlio.SgnlDB(
        config=options.config_schema, dbname=options.input_database_file
    )

    #
    # record our passage
    #

    indb.default_cursor.execute(
        """UPDATE process SET program = ?;""",
        (process_name,),
    )
    for name, val in process_params.items():
        indb.default_cursor.execute(
            """
        INSERT INTO process_params (param, program, value)
        VALUES (?, ?, ?);
        """,
            (name, process_name, str(val)),
        )
    #
    # Add segments
    #
    frame_segments = ligolw_segments.segmenttable_get_by_name(
        ligolw_utils.load_filename(
            options.segments_file,
            contenthandler=ligolw_segments.LIGOLWContentHandler,
        ),
        options.segments_name,
    ).coalesce()
    indb.default_cursor.execute(
        """UPDATE process SET program = ?;""",
        (process_name,),
    )
    for ifo, segs in frame_segments.items():
        for seg in segs:
            indb.default_cursor.execute(
                """
            INSERT INTO segment (start_time, end_time, ifo, name)
            VALUES (?, ?, ?, ?);
            """,
                (seg[0].ns(), seg[1].ns(), ifo, options.segments_name),
            )

    indb.to_file(options.output_database_file)
