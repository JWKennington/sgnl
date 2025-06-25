"""An executable to post a gracedb far upload threshold to inspiral jobs"""

# Copyright (C) 2012  Kipp Cannon
# Copyright (C) 2025  Zach Yarbrough

import argparse

import requests


def parse_command_line():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--set-far-threshold",
        metavar="Hertz",
        type=float,
        help="Set the GraceDB false-alarm rate upload threshold to Hertz",
    )
    parser.add_argument(
        "--analysis-tag",
        type=str,
        required=True,
        help="Unique analysis tag, used in request routes",
    )
    parser.add_argument(
        "--query-far-threshold",
        action="store_true",
        help="Query current far threshold of running jobs",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument(
        "filenames",
        nargs="+",
        help="One or more registry files",
    )

    args = parser.parse_args()

    return args, args.filenames


def get_url(filename):
    return [url.strip() for url in open(filename)][0]


def post_far_threshold(filename, far_threshold, analysis_tag):
    url = get_url(filename)
    post_data = requests.post(
        f"{url}/{analysis_tag}/post/gracedb",
        json={"far-threshold": far_threshold},
        timeout=10,
    )

    return post_data


def query_far_threshold(filename, analysis_tag):
    url = get_url(filename)
    get_data = requests.get(
        f"{url}/{analysis_tag}/get/gracedb/far-threshold",
        timeout=10,
    )

    print(filename, "far threshold:", get_data.json())


def main():
    args, filenames = parse_command_line()

    # argument logic
    if args.query_far_threshold and not args.set_far_threshold:
        for file in filenames:
            query_far_threshold(file, args.analysis_tag)

    elif args.set_far_threshold and not args.query_far_threshold:

        for file in filenames:
            try:
                post_far_threshold(
                    file,
                    args.set_far_threshold,
                    args.analysis_tag,
                )

            except Exception as e:
                print(f"Error when posting far threshold to {file}, error:")
                print(f"{e}")

    else:
        raise ValueError(
            "Improper combination of arguments: specify either"
            "--query-far-threshold or --set-far-threshold, but not both."
        )


if __name__ == "__main__":
    main()
