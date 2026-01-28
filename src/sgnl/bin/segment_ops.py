# Copyright (C) 2011 Chad Hanna
# Copyright (C) 2025 Zach Yarbrough
"""
Apply set operations and time filtering to segment lists for SGNL analysis.

Supports:
- Bounding segments in time
- Removing short segments
- Contracting segments
- Union / intersection / diff across multiple files
"""

from __future__ import annotations

import argparse

from sgnl.dags.segments import (
    bound_segments,
    combine_segmentlistdicts,
    contract_segments,
    diff_segmentlistdicts,
    filter_short_segments,
    load_segment_file,
    write_segments,
)


def parse_command_line():
    parser = argparse.ArgumentParser(
        description="Apply SGNL segment operations to one or more segment XML files."
    )

    parser.add_argument(
        "inputs",
        metavar="FILE",
        nargs="+",
        help="Input XML segment files to read.",
    )

    parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        default="segments_out.xml.gz",
        help="Output XML file name (default: segments_out.xml.gz).",
    )

    parser.add_argument(
        "-n",
        "--segment-name",
        metavar="NAME",
        default="datasegments",
        help="Name of segment table in the output file (default: datasegments).",
    )

    # set operations
    parser.add_argument(
        "--operation",
        metavar="OPERATION",
        choices=["union", "intersection", "diff"],
        help="Perform a set operation across input segment files. "
        "Choices: union, intersection, diff. "
        "Requires 2+ files for union/intersection or exactly 2 files for diff.",
    )

    # filtering and trimming
    parser.add_argument(
        "--gps-start",
        metavar="GPS",
        type=float,
        help="Trim segments to begin no earlier than this GPS time.",
    )
    parser.add_argument(
        "--gps-end",
        metavar="GPS",
        type=float,
        help="Trim segments to end no later than this GPS time.",
    )
    parser.add_argument(
        "--min-length",
        metavar="SECONDS",
        type=float,
        default=0.0,
        help="Remove segments shorter than this duration.",
    )
    parser.add_argument(
        "--trim",
        metavar="SECONDS",
        type=float,
        default=0.0,
        help="Contract segments by this many seconds on each end.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output."
    )

    args = parser.parse_args()

    # Validate args
    if args.operation:
        if args.operation == "diff" and len(args.inputs) != 2:
            parser.error("--op diff requires exactly two input segment files.")
        if args.operation in {"union", "intersection"} and len(args.inputs) < 2:
            parser.error(f"--op {args.operation} requires 2 or more input files.")

    return args


def main():
    args = parse_command_line()

    # Load all files
    segdicts = []
    for fname in args.inputs:
        if args.verbose:
            print(f"Loading segments from {fname} ...")
        segdicts.append(load_segment_file(fname))

    # Set operations
    if args.operation:
        if args.verbose:
            print(f"Applying set operation: {args.operation}")

        if args.operation == "union":
            result = combine_segmentlistdicts(segdicts, "union")
        elif args.operation == "intersection":
            result = combine_segmentlistdicts(segdicts, "intersection")
        elif args.operation == "diff":
            result = diff_segmentlistdicts(segdicts[0], segdicts[1])

    else:
        # If no op is specified, take the first file as the base
        result = segdicts[0]

    # Individual operations
    if args.gps_start or args.gps_end:
        if args.verbose:
            print("Applying GPS bounding.")
        result = bound_segments(result, args.gps_start, args.gps_end)

    if args.min_length > 0:
        if args.verbose:
            print(f"Filtering segments shorter than {args.min_length} seconds.")
        result = filter_short_segments(result, args.min_length)

    if args.trim > 0:
        if args.verbose:
            print(f"Contracting segments by {args.trim} seconds.")
        result = contract_segments(result, args.trim)

    # Save output
    if args.verbose:
        print(f"Saving output segments to {args.output}")
        print(result)

    write_segments(
        result,
        output=args.output,
        segment_name=args.segment_name,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
