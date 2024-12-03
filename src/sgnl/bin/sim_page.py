import argparse
import os
from sgnl import sgnlio
from sgnl import viz
import numpy
from numpy.polynomial import Polynomial


def parse_command_line():
    parser = argparse.ArgumentParser(
        prog="plot-sim",
        description="This makes a missed found plot",
        epilog="I really hope you enjoy this program.",
    )
    parser.add_argument("-s", "--config-schema", help="config schema yaml file")
    parser.add_argument("--input-db", help="the input database.")
    parser.add_argument(
        "--segments-name",
        help="the segment name. Default = afterhtgate.",
        default="afterhtgate",
    )
    parser.add_argument(
        "--output-html", help="The output html page", default="plot-sim.html"
    )
    parser.add_argument(
        "--far-threshold",
        default=1 / 86400 / 30.0,
        type=float,
        help="FAR threshold in Hz. Default 1/86400/30.",
    )
    parser.add_argument("-v", "--verbose", help="be verbose", action="store_true")
    args = parser.parse_args()

    assert args.config_schema and os.path.exists(args.config_schema)

    return args


class EffVsDist:
    def __init__(self, m, f, mcstart=0.0, mcend=numpy.inf, order=6):
        self.order = order

        def mc(m1, m2):
            return (m1 * m2) ** 0.6 / (m1 + m2) ** 0.2

        self.dm = sorted(
            [
                d
                for m1, m2, d in zip(
                    m.simulation.mass1, m.simulation.mass2, m.simulation.distance
                )
                if mcstart <= mc(m1, m2) < mcend
            ]
        )
        self.df = sorted(
            [
                d
                for m1, m2, d in zip(
                    f.simulation.mass1, f.simulation.mass2, f.simulation.distance
                )
                if mcstart <= mc(m1, m2) < mcend
            ]
        )
        # self.dm = sorted(m.simulation.distance)
        # self.df = sorted(f.simulation.distance)
        self.Nm = None if not self else 1 + numpy.arange(len(self.dm))
        self.Nf = None if not self else 1 + numpy.arange(len(self.df))
        self.pm = (
            None
            if not self
            else Polynomial.fit(numpy.log(self.dm), numpy.log(self.Nm), self.order)
        )
        self.pf = (
            None
            if not self
            else Polynomial.fit(numpy.log(self.df), numpy.log(self.Nf), self.order)
        )
        self.dint = (
            None
            if not self
            else (max(self.dm[0], self.df[0]), min(self.dm[-1], self.df[-1]))
        )

    def __bool__(self):
        return len(self.dm) > self.order + 1 and len(self.df) > self.order + 1

    def darr(self, n=100):
        if not self:
            return None, None
        edges = numpy.linspace(*self.dint, n + 1)
        deltas = edges[1:] - edges[:-1]
        centers = edges[:-1] + deltas / 2
        return centers, deltas

    def __call__(self, d):
        lnd = numpy.log(d)
        if self:
            num = k = numpy.exp(self.pf.deriv()(lnd))
            den = N = numpy.exp(self.pf.deriv()(lnd)) + numpy.exp(self.pm.deriv()(lnd))
            low = (N * (2 * k + 1) - numpy.sqrt(4 * N * k * (N - k) + N**2)) / (
                2 * N * (N + 1)
            )
            high = (N * (2 * k + 1) + numpy.sqrt(4 * N * k * (N - k) + N**2)) / (
                2 * N * (N + 1)
            )
            return num / den, low, high
        else:
            return (
                0,
                0,
                0 if not isinstance(lnd, numpy.array) else numpy.zeros(len(lnd)),
                numpy.zeros(len(lnd)),
                numpy.zeros(len(lnd)),
            )

    def vt(self, t, n=100):
        if not self:
            return None
        d, dx = self.darr()
        eff_middle, eff_low, eff_high = self(d)

        def f(_d, eff_of_d, _dx, t=t):
            return (4 * numpy.pi * _d**2 * eff_of_d * _dx).sum() * t

        return f(d, eff_middle, dx), f(d, eff_low, dx), f(d, eff_high, dx)


# This function is slow because I am lazy
def VT(
    indb,
    segments_name,
    mcstart=(0, 0.5, 450.0),
    mcend=(0.5, 450.0, numpy.inf),
    ifars=10.0 ** numpy.arange(0, 13),
):
    vts = {}
    vts_low = {}
    vts_high = {}
    ifars_years = ifars / 86400 / 365.25
    for ifar in ifars:
        misseddict, founddict = indb.missed_found_by_on_ifos(
            far_threshold=1.0 / ifar, segments_name=segments_name
        )
        for combo in misseddict:
            vts.setdefault(combo, {})
            vts_low.setdefault(combo, {})
            vts_high.setdefault(combo, {})
            for mcs, mce in zip(mcstart, mcend):
                key = "%.1f-%.1f" % (mcs, mce)
                eff = EffVsDist(
                    misseddict[combo], founddict[combo], mcstart=mcs, mcend=mce
                )
                if eff:
                    vm, vl, vh = eff.vt(
                        abs(misseddict[combo].segments) / 1e9 / 365.25 / 86400
                    )
                    vts[combo].setdefault(key, []).append(vm)
                    vts_low[combo].setdefault(key, []).append(vl)
                    vts_high[combo].setdefault(key, []).append(vh)
                else:
                    vts[combo].setdefault(key, []).append(0.0)
                    vts_low[combo].setdefault(key, []).append(0.0)
                    vts_high[combo].setdefault(key, []).append(0.0)
    return vts, vts_low, vts_high


def main():
    args = parse_command_line()

    indb = sgnlio.SgnlDB(config=args.config_schema, dbname=args.input_db)

    # VT
    vt_section = viz.Section("Injection derived VT", "vt")
    ifars = 10.0 ** numpy.arange(0, 13)
    ifars_years = ifars / (86400 * 365.25)
    vts, vts_low, vts_high = VT(indb, args.segments_name, ifars=ifars)
    for combo in vts:
        fig = viz.plt.figure()
        for mcs in vts[combo]:
            viz.plt.loglog(ifars_years, vts[combo][mcs], label=mcs)
            viz.plt.fill_between(
                ifars_years, vts_low[combo][mcs], vts_high[combo][mcs], alpha=0.3
            )
        viz.plt.grid()
        viz.plt.legend()
        viz.plt.xlabel("IFAR (years)")
        viz.plt.ylabel("VT (Mpc^3 yr)")
        fig.tight_layout()
        vt_section.append(
            {
                "img": viz.b64(),
                "title": "VT vs FAR for %s" % ",".join(sorted(combo)),
                "caption": "VT vs FAR computed from injections and ignoring cosmology",
            }
        )

    # Reuse a single missed / found dictionary for the rest of the plots
    misseddict, founddict = indb.missed_found_by_on_ifos(
        far_threshold=args.far_threshold, segments_name=args.segments_name
    )

    # Summary Tables
    tables_section = viz.Section("Injection Summary Tables", "summary tables")
    tables_section.append(
        {
            "table": [
                {
                    "on ifos": ",".join(sorted(combo)),
                    "missed": len(misseddict[combo]),
                    "found": len(founddict[combo]),
                }
                for combo in founddict
            ],
            "title": "Missed / Found Summary Statistics",
            "caption": "Missed and found for different on ifo combinations",
        }
    )

    # Injection distributions
    distribution_section = viz.Section(
        "SGN injection distributions", "injection distributions"
    )
    for xcol, ycol, xlabel, ylabel, caption, plttype, axis in [
        (
            "mass1",
            "mass2",
            "Mass 1",
            "Mass 2",
            "Injected component masses",
            "loglog",
            "square",
        ),
    ]:

        fig = viz.plt.figure(figsize=(6, 6))
        for combo in misseddict:
            missed = misseddict[combo]
            found = founddict[combo]
            getattr(viz.plt, plttype)(
                getattr(missed.simulation, xcol),
                getattr(missed.simulation, ycol),
                color="k",
                marker=".",
                linestyle="None",
            )
            getattr(viz.plt, plttype)(
                getattr(found.simulation, xcol),
                getattr(found.simulation, ycol),
                color="k",
                marker=".",
                linestyle="None",
            )
        viz.plt.axis(axis)
        if axis == "square":
            viz.plt.gca().set_aspect("equal", adjustable="box")
        viz.plt.xlabel(xlabel)
        viz.plt.ylabel(ylabel)
        viz.plt.grid()
        fig.tight_layout()
        distribution_section.append(
            {
                "img": viz.b64(),
                "title": "%s vs %s" % (ylabel, xlabel),
                "caption": caption,
            }
        )

    # Missed / found
    missed_found_section = viz.Section("SGN missed / found injections", "missed/found")
    for xcol, ycol, xlabel, ylabel, caption in [
        (
            "time",
            "decisive_snr",
            "Time",
            "Decisive SNR",
            "Decisive SNR is defined as the second highest injected SNR for ifos on at the time of the event regardless of what ifos recovered the event.",
        ),
        (
            "time",
            "network_snr",
            "Time",
            "Network SNR",
            "Network SNR is defined as the injected RMS SNR for ifos on at the time of the event regardless of what ifos recovered the event.",
        ),
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
        missed_found_section.append(
            {
                "img": viz.b64(),
                "title": "%s vs %s" % (ylabel, xlabel),
                "caption": caption,
            }
        )

    # Injected vs recovered network SNR
    recovered_snr_section = viz.Section("SGN injection SNR recovery", "snr recovery")
    fig = viz.plt.figure(figsize=(6, 4))
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
    viz.plt.gca().set_aspect("equal", adjustable="box")
    viz.plt.xlabel(xlabel)
    viz.plt.ylabel(ylabel)
    viz.plt.grid()
    viz.plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.tight_layout()
    recovered_snr_section.append(
        {
            "img": viz.b64(),
            "title": "%s vs %s" % (ylabel, xlabel),
            "caption": "The RMS injected SNR vs the RMS recovered SNR.  Injected SNR will be for whatever ifos were on regardless of what ifos detetected the event.  Recovered SNR will be only ifos that detected the event.",
        }
    )

    # Combine the template and the images HTML
    #    html_content = viz.page(_images_html = viz.image_html(images), _modals = viz.modal_html(images))
    # html_content = viz.page(_images_html = viz.image_html(images))
    html_content = viz.page(
        [
            vt_section,
            missed_found_section,
            recovered_snr_section,
            distribution_section,
            tables_section,
        ]
    )
    # Save the HTML content to a file
    with open(args.output_html, "w") as f:
        f.write(html_content)


if __name__ == "__main__":
    main()
