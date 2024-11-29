import argparse
import os
from sgnl import sgnlio
from sgnl import viz


def parse_command_line():
    parser = argparse.ArgumentParser(
        prog="plot-sim",
        description="Makes a result page",
        epilog="I really hope you enjoy this program.",
    )
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument("--input-db", help="the input database.")
    parser.add_argument(
        "--output-html", help="The output html page", default="plot-sim.html"
    )
    parser.add_argument("-v", "--verbose", help="be verbose", action="store_true")
    args = parser.parse_args()

    assert args.config_schema and os.path.exists(args.config_schema)

    return args


def process_events(events, n=200, cols=[], formats={}):
    events = sorted(events, key=lambda x: x["event"]["far"])[:n]
    return [
        {
            k: (v if k not in formats else formats[k](v))
            for k, v in e["event"].items()
            if k in cols
        }
        for e in events
    ]


def main():
    args = parse_command_line()

    indb = sgnlio.SgnlDB(config=args.config_schema, dbname=args.input_db)
    # Summary Tables
    tables_section = viz.Section("Results Table", "results tables")
    table_headers = {
        "time": "time",
        "network_snr": "snr",
        "network_chisq_weighted_snr": "eff snr",
        "likelihood": "logL",
        "far": "far",
    }
    tables_section.append(
        {
            "table": process_events(
                indb.get_events(),
                cols=table_headers,
                formats={
                    "time": (lambda x: "%.4f" % (x * 1e-9)),
                    "network_snr": (lambda x: "%.3f" % x),
                    "network_chisq_weighted_snr": (lambda x: "%.3f" % x),
                    "likelihood": (lambda x: "%.2f" % x),
                    "far": (lambda x: "%.2e" % x),
                },
            ),
            "table-headers": table_headers,
            "title": "Results",
            "caption": "Results",
        }
    )

    html_content = viz.page([tables_section])
    # Save the HTML content to a file
    with open(args.output_html, "w") as f:
        f.write(html_content)


if __name__ == "__main__":
    main()
