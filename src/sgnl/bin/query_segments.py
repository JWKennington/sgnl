# Copyright (C) 2020-2025  Kipp Cannon, Patrick Godwin, Chad Hanna,
# Ryan Magee, Cody Messick, Zach Yarbrough
"""Query and save segment lists for SGNL analysis."""

import argparse

from sgnl.dags.segments import (
    DEFAULT_DQSEGDB_SERVER,
    query_dqsegdb_segments,
    query_gwosc_segments,
    write_segments
)

VALID_IFOS = {"H1", "L1", "V1", "K1"}


def parse_command_line():
    parser = argparse.ArgumentParser(
        description="Query and download segment lists for SGNL analysis."
    )
    parser.add_argument(
        "--source",
        metavar="SOURCE",
        choices=["dqsegdb", "gwosc"],
        default="dqsegdb",
        help="Set the data source to query "
        "(choices: dqsegdb, gwosc). Default: dqsegdb",
    )
    parser.add_argument(
        "-s",
        "--start",
        metavar="START",
        type=int,
        required=True,
        help="Set the start GPS time.",
    )
    parser.add_argument(
        "-e",
        "--end",
        metavar="END",
        type=int,
        required=True,
        help="Set the end GPS time.",
    )
    parser.add_argument(
        "-f",
        "--flag",
        metavar="FLAG",
        action="append",
        help=(
            "Set the flags to use to query segments (format: IFO:FLAG:VERSION), "
            "one --flag for each instrument (H1, L1, V1, or K1). "
            "Required for dqsegdb source."
        ),
    )
    parser.add_argument(
        "-i",
        "--instruments",
        metavar="INSTRUMENTS",
        nargs="+",
        choices=VALID_IFOS,
        help="Set the instruments to process (choices: H1, L1, V1, K1). "
        "Required for gwosc source.",
    )
    parser.add_argument(
        "-u",
        "--server",
        metavar="url",
        default=DEFAULT_DQSEGDB_SERVER,
        help="Set the URL of the DQSegDB server to query. "
        f"Default: {DEFAULT_DQSEGDB_SERVER}",
    )
    parser.add_argument(
        "--no-cert",
        action="store_true",
        help="Turn off SSL certificate validation (gwosc only). Default: False.",
    )
    parser.add_argument(
        "-n",
        "--segment-name",
        metavar="name",
        default="datasegments",
        help="Set the name for the segment list in the output file. "
        "Default: datasegments",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="file",
        default="segments.xml.gz",
        help="Set the name of the segments file to generate. Default: segments.xml.gz",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print verbose output."
    )

    args = parser.parse_args()

    # Validate source-specific requirements
    if args.source == "dqsegdb":
        if not args.flag:
            parser.error("--flag is required when using dqsegdb source")

        instruments = []
        flags = {}

        for entry in args.flag:
            parts = entry.split(":")

            if len(parts) != 3:
                raise ValueError(f"Flag '{entry}' must be in format IFO:FLAG:VERSION")

            ifo, flag, version = parts

            if ifo not in VALID_IFOS:
                raise ValueError(
                        f"Invalid IFO '{ifo}' in flag '{entry}' (valid: {VALID_IFOS})"
                    )

            instruments.append(ifo)
            flags[ifo] = entry  # entry is already IFO:FLAG:VERSION

        args.instruments = sorted(instruments)
        args.flags = flags

        if args.verbose:
            print(f"Using flags: {flags}")

    elif args.source == "gwosc":
        if not args.instruments:
            parser.error("--instruments is required when using gwosc source")

        args.instruments = sorted(set(args.instruments))

        if args.flag:
            parser.error("--flag cannot be specified when querying GWOSC")

        args.flags = None

    return args


def main():
    args = parse_command_line()

    # query segments based on source
    if args.source == "dqsegdb":
        if args.verbose:
            print(f"Querying DQSegDB segments for {args.instruments}")
            print(f"  GPS time range: {args.start} - {args.end}")
            print(f"  Server: {args.server}")

        segments = query_dqsegdb_segments(
            args.instruments,
            args.start,
            args.end,
            args.flags,
            server=args.server,
        )

    elif args.source == "gwosc":
        if args.verbose:
            print(f"Querying GWOSC segments for {args.instruments}")
            print(f"  GPS time range: {args.start} - {args.end}")

        segments = query_gwosc_segments(
            args.instruments,
            args.start,
            args.end,
            verify_certs=(not args.no_cert),
        )

    if args.verbose:
        print("Found the following segments:")
        print(segments)

    write_segments(
        segments,
        args.output,
        segment_name=args.segment_name,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
