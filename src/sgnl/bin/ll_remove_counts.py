"""An executable to post a gps time for count removal"""

# Copyright (C) 2024-2025 Prathamesh Joshi, Leo Tsukada, Zach Yarbrough

import argparse

import requests


def parse_command_line():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gps-time",
        metavar="seconds",
        type=int,
        help="Set the GPS time at which to remove count tracker counts from",
    )
    parser.add_argument(
        "--action",
        choices=["remove", "check", "undo-remove"],
        help="Choose an action from ['remove', 'check', 'undo-remove']. (remove: "
        "Remove counts at the given time, check: Check whether the given time has "
        "already been sent successfully, undo-remove: Undo the removed counts at "
        "the given time)",
    )
    parser.add_argument(
        "--analysis-tag", type=str, help="Unique analysis tag, used in request routes"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument("filenames", nargs="+", help="One or more registry files")

    args = parser.parse_args()

    return args, args.filenames


def get_url(filename):
    return [url.strip() for url in open(filename)][0]


def submit(filename, gpstime, analysis_tag):
    url = get_url(filename)
    post_data = requests.post(
        f"{url}/{analysis_tag}/post/StrikeSnk",
        json={"count_tracker": gpstime},
        timeout=10,
    )
    return post_data


# post a negative gps time to the count_tracker state dict key
def undo(filename, gpstime, analysis_tag):
    url = get_url(filename)
    post_data = requests.post(
        f"{url}/{analysis_tag}/post/StrikeSnk",
        json={"count_tracker": -gpstime},
        timeout=10,
    )
    return post_data


def check(filename, gpstime, analysis_tag):
    url = get_url(filename)
    get_data = requests.get(
        f"{url}/{analysis_tag}/get/StrikeSnk/count_removal_times", timeout=10
    )
    return get_data


def main():

    args, filenames = parse_command_line()

    if args.action == "remove":
        for file in filenames:
            try:
                response = submit(file, args.gps_time, args.analysis_tag)
            except Exception as e:
                print(f"Error when removing from file {file}, error:")
                print(f"{e}")

    elif args.action == "undo-remove":
        for file in filenames:
            try:
                response = undo(file, args.gps_time, args.analysis_tag)
            except Exception as e:
                print(f"Error when undo-remove from file {file}, error:")
                print(f"{e}")

    elif args.action == "check":
        for file in filenames:
            try:
                response = check(file, args.gps_time, args.analysis_tag)
                if args.gps_time in response.json():
                    print(
                        f"{args.gps_time} in count_removal_times for file {file}, "
                        "check successful"
                    )
                else:
                    print(
                        f"{args.gps_time} not in count_removal_times for file {file}, "
                        "check unsuccessful"
                    )
            except Exception as e:
                print(f"Error when undo-remove from file {file}, error:")
                print(f"{e}")


if __name__ == "__main__":
    main()
