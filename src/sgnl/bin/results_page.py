import argparse
import os

from strike.stats import far

from sgnl import sgnlio, viz


def parse_command_line():
    parser = argparse.ArgumentParser(
        prog="plot-sim",
        description="Makes a result page",
        epilog="I really hope you enjoy this program.",
    )
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument("--input-db", help="the input database.")
    parser.add_argument("--input-rank-stat-pdf", help="the input rank stat pdf file.")
    parser.add_argument(
        "--output-html", help="The output html page", default="plot-sim.html"
    )
    parser.add_argument("-v", "--verbose", help="be verbose", action="store_true")
    args = parser.parse_args()

    assert args.config_schema and os.path.exists(args.config_schema)

    return args


def process_events(events, n=200, cols=None, formats=None):
    if cols is None:
        cols = []
    if formats is None:
        formats = {}
    events = sorted(events, key=lambda x: x["event"]["combined_far"])[:n]
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
        "combined_far": "far",
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
                    "combined_far": (lambda x: "%.2e" % x),
                },
            ),
            "table-headers": table_headers,
            "title": "Results",
            "caption": "Results",
        }
    )

    ifar_section = viz.Section("Rate vs. Threshold", "rate vs. threshold")
    zl_stats = {"lnlr": [], "ifar": []}
    for event in indb.get_events(nanosec_to_sec=True):
        zl_stats["lnlr"].append(event["event"]["likelihood"])
        zl_stats["ifar"].append(1 / event["event"]["combined_far"])

    pdf = far.RankingStatPDF.load(args.input_rank_stat_pdf)
    zl_plots = pdf.create_plots(zl_stats)
    for name, plot in zl_plots.items():
        if "IFAR" in name:
            xlabel = "IFAR"
        elif "LNLR" in name:
            xlabel = "LNLR"
        else:
            raise ValueError("unknown plot")

        ifar_section.append(
            {
                "img": viz.b64(plot),
                "title": "%s vs %s" % ("RATE", xlabel),
                "caption": name.split("-")[1],
            }
        )

    html_content = viz.page([tables_section, ifar_section])
    # Save the HTML content to a file
    with open(args.output_html, "w") as f:
        f.write(html_content)


if __name__ == "__main__":
    main()
