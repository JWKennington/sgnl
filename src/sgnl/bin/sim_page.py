import argparse
import os
from sgnl import sgnlio
from sgnl import viz


def parse_command_line():
    parser = argparse.ArgumentParser(
        prog="plot-sim",
        description="This makes a missed found plot",
        epilog="I really hope you enjoy this program.",
    )
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument("--input-db", help="the input database.")
    parser.add_argument(
        "--output-html", help="The output html page", default="plot-sim.html"
    )
    parser.add_argument(
        "--far-threshold",
        default=1 / 86400 / 30.,
        type=float,
        help="FAR threshold in Hz. Default 1/86400/30.",
    )
    parser.add_argument("-v", "--verbose", help="be verbose", action="store_true")
    args = parser.parse_args()

    return args


def main():
    args = parse_command_line()

    indb = sgnlio.SgnlDB(config=args.config_schema, dbname=args.input_db)
    misseddict, founddict = indb.missed_found_by_on_ifos(far_threshold=args.far_threshold, segments_name="afterhtgate")


    # Missed / found
    missed_found_section = viz.Section("SGN missed / found injections", "missed/found")
    for (xcol, ycol, xlabel, ylabel, caption) in [
        ("time", "decisive_snr", "Time", "Decisive SNR", "Decisive SNR is defined as the second highest injected SNR for ifos on at the time of the event regardless of what ifos recovered the event."),
        ("time", "network_snr", "Time", "Network SNR", "Network SNR is defined as the injected RMS SNR for ifos on at the time of the event regardless of what ifos recovered the event."),
        ]:
        fig = viz.plt.figure()
        for combo in misseddict:
            missed = misseddict[combo]
            found = founddict[combo]
    
            viz.plt.semilogy(
                getattr(missed.simulation, xcol),
                getattr(missed.simulation, ycol),
                color=missed.color,
                marker=missed.marker,
                linestyle="None",
            )
            viz.plt.semilogy(
                getattr(found.simulation, xcol),
                getattr(found.simulation, ycol),
                marker=found.marker,
                color=found.color,
                label=",".join(sorted(combo)),
                linestyle="None",
            )
        viz.plt.xlabel(xlabel)
        viz.plt.ylabel(ylabel)
        viz.plt.grid()
        viz.plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
        fig.tight_layout()
        missed_found_section.append({"img":viz.b64(), "title":"%s vs %s" % (ylabel, xlabel), "caption":caption})

    # Injected vs recovered network SNR
    recovered_snr_section = viz.Section("SGN injection SNR recovery", "snr recovery")
    fig = viz.plt.figure(figsize=(6,4))
    xlabel = "Injected Network SNR"
    ylabel = "Recovered Network SNR"
    for combo, found in founddict.items():
        viz.plt.loglog(
            found.simulation.network_snr,
            found.event.network_snr,
            color=found.color,
            marker=found.marker,
            label=",".join(sorted(combo)),
            linestyle="None",
        )
    viz.plt.axis("square")
    viz.plt.xlabel(xlabel)
    viz.plt.ylabel(ylabel)
    viz.plt.grid()
    viz.plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.tight_layout()
    recovered_snr_section.append({"img":viz.b64(), "title":"%s vs %s" % (ylabel, xlabel), "caption":"The RMS injected SNR vs the RMS recovered SNR.  Injected SNR will be for whatever ifos were on regardless of what ifos detetected the event.  Recovered SNR will be only ifos that detected the event."})

    # Combine the template and the images HTML
#    html_content = viz.page(_images_html = viz.image_html(images), _modals = viz.modal_html(images))
    #html_content = viz.page(_images_html = viz.image_html(images))
    html_content = viz.page([missed_found_section, recovered_snr_section])
    # Save the HTML content to a file
    with open(args.output_html, "w") as f:
        f.write(html_content)

if __name__ == "__main__":
    main()
