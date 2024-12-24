"""an executable to upload auxiliary files and plots for GraceDB """

# Copyright (C) 2019 Alexander Pace, Kipp Cannon, Chad Hanna, Drew Keppel
# Copyright (C) 2020 Patrick Godwin, Cody Messick
# Copyright (C) 2024 Yun-Jing Huang

__usage__ = "sgnl-ll-inspiral-event-plotter [--options]"

# -------------------------------------------------
#                   Preamble
# -------------------------------------------------

import copy
import http.client
import io
import json
import logging
import math
import os
import time
from argparse import ArgumentParser
from collections import OrderedDict
from enum import Enum
from xml.sax import SAXParseException

import numpy
from lal import LIGOTimeGPS, series
from ligo.gracedb.rest import DEFAULT_SERVICE_URL as DEFAULT_GRACEDB_URL
from ligo.gracedb.rest import GraceDb, HTTPError
from ligo.lw import array as ligolw_array
from ligo.lw import ligolw, lsctables
from ligo.lw import param as ligolw_param
from ligo.lw import utils as ligolw_utils
from ligo.scald import utils
from strike.stats import far
from strike.stats.likelihood_ratio import LnLikelihoodRatio

from sgnl.plots.util import set_matplotlib_cache_directory

set_matplotlib_cache_directory()  # FIXME: I don't know if this needs to be done here

from strike.plots.stats import plot_dtdphi  # noqa: E402

from sgnl import events, svd_bank  # noqa: E402 I003
from sgnl.gracedb import FakeGracedbClient, upload_fig  # noqa: E402
from sgnl.plots import psd as plotpsd  # noqa: E402

# from strike.plots.stats import far as plotfar  # noqa: E402

import matplotlib  # noqa: E402 I001 isort:skip

matplotlib.rcParams.update(
    {
        "font.size": 10.0,
        "axes.titlesize": 10.0,
        "axes.labelsize": 10.0,
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "legend.fontsize": 8.0,
        "figure.dpi": 100,
        "savefig.dpi": 100,
        "text.usetex": True,
    }
)
from matplotlib import figure  # noqa: E402
from matplotlib.backends.backend_agg import (  # noqa: E402
    FigureCanvasAgg as FigureCanvas,
)

matplotlib.use("agg")
from matplotlib import pyplot as plt  # noqa: E402


# -------------------------------------------------
#                Content Handler
# -------------------------------------------------
@lsctables.use_in
@ligolw_array.use_in
@ligolw_param.use_in
class ligolwcontenthandler(ligolw.LIGOLWContentHandler):
    pass


# -------------------------------------------------
#                   Functions
# -------------------------------------------------


def parse_command_line():

    parser = ArgumentParser(usage=__usage__, description=__doc__)
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true", help="Be verbose."
    )
    parser.add_argument(
        "--tag",
        metavar="string",
        default="test",
        help="Sets the name of the tag used. Default = 'test'",
    )
    parser.add_argument(
        "--max-event-time",
        type=int,
        default=7200,
        help="Maximum time to keep around an event. Default = 2 hours.",
    )
    parser.add_argument(
        "--processing-cadence",
        type=float,
        default=0.1,
        help="Rate at which the event plotter acquires and processes data. Default = "
        "0.1 seconds.",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=0.2,
        help="Timeout for requesting messages from a topic. Default = 0.2 seconds.",
    )
    parser.add_argument(
        "--kafka-server",
        metavar="string",
        help="Sets the server url that the kafka topic is hosted on. Required.",
    )
    parser.add_argument(
        "--upload-topic",
        metavar="string",
        help="Sets the input kafka topic to get uploaded event info from. Required.",
    )
    parser.add_argument(
        "--ranking-stat-topic",
        metavar="string",
        help="Sets the input kafka topic to get ranking stat info from. Required.",
    )
    parser.add_argument(
        "--gracedb-service-url",
        metavar="url",
        default=DEFAULT_GRACEDB_URL,
        help="Override default GracedB service url (optional, default is {}).".format(
            DEFAULT_GRACEDB_URL
        ),
    )
    parser.add_argument(
        "--max-snr",
        metavar="SNR",
        type=float,
        default=200.0,
        help="Set the upper bound of the SNR ranges in plots (default = 200).",
    )
    parser.add_argument(
        "--format",
        default="png",
        help="Set file format by selecting the extention (default = 'png').",
    )
    parser.add_argument(
        "--output-path",
        metavar="PATH",
        help="Write local copies of the plots to this directory (default = don't).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Disable upload of plots to gracedb, e.g., for testing new plots.",
    )
    parser.add_argument(
        "--plot",
        action="append",
        help=(
            "Plots to make. Options are: "
            "1. RANKING_DATA: upload: ranking_data.xml.gz. "
            "2. RANKING_PLOTS: upload background plots from the ranking statistic data."
            "3. SNR_PLOTS: upload SNR time series plots. "
            "4. PSD_PLOTS: upload PSD plots. "
            "5. DTDPHI_PLOTS: upload DTDPHI plots. "
            " Can be given multiple times. If not specified, all plots are made."
        ),
    )

    options = parser.parse_args()

    if options.no_upload and options.output_path is None:
        raise ValueError("--no-upload without setting --ouput-path disables all output")

    return options


def load_rankingstat_xml_with_retries(filename, logger):
    # The rankingstat is not guaranteed to be available on disk immediately
    # after getting written. Try 3 times, with 1s sleep in between.
    for tries in range(3):
        try:
            xmldoc = ligolw_utils.load_filename(
                filename, contenthandler=LnLikelihoodRatio.LIGOLWContentHandler
            )
            break
        except SAXParseException as e:
            if tries < 2:
                logger.warning(
                    "Failed to load rankingstat located at %s. Trying again.", filename
                )
                time.sleep(1)
            else:
                raise IOError(
                    f"Could not load rankingstat located at {filename} after 3 tries. "
                    f"Failed with error:\n{e}"
                )
    return xmldoc


# -------------------------------------------------
#                    Classes
# -------------------------------------------------


class Plots(Enum):
    RANKING_DATA = 1
    RANKING_PLOTS = 2
    SNR_PLOTS = 3
    PSD_PLOTS = 4
    DTDPHI_PLOTS = 5


class EventPlotter(events.EventProcessor):
    """
    manages plotting and file uploading for incoming events.
    """

    _name = "event_plotter"

    def __init__(
        self,
        kafka_server: str,
        logger,
        output_path: str,
        plot: list[str],
        upload_topic: str,
        ranking_stat_topic: str,
        format: str = "png",
        gracedb_service_url: str = DEFAULT_GRACEDB_URL,
        max_event_time: int = 7200,
        max_snr: float = 200.0,
        no_upload: bool = False,
        processing_cadence: float = 0.1,
        request_timeout: float = 0.2,
        tag: str = "test",
    ):
        self.logger = logger
        self.logger.info("setting up event plotter...")

        self.upload_topic = f"sgnl.{tag}.{upload_topic}"
        self.ranking_stat_topic = f"sgnl.{tag}.{ranking_stat_topic}"

        plot_string = "-".join([str(Plots[plot].value) for plot in plot])

        is_injection_job = upload_topic == "inj_uploads"
        heartbeat_topic = (
            f"sgnl.{tag}.event_plotter_heartbeat"
            if not is_injection_job
            else f"sgnl.{tag}.inj_event_plotter_heartbeat"
        )

        events.EventProcessor.__init__(
            self,
            process_cadence=processing_cadence,
            request_timeout=request_timeout,
            kafka_server=kafka_server,
            input_topic=[self.upload_topic, self.ranking_stat_topic],
            tag=f"{tag}-{plot_string}",
            send_heartbeats=True,
            heartbeat_cadence=60.0,
            heartbeat_topic=heartbeat_topic,
        )

        # initialize timing options
        self.max_event_time = max_event_time
        self.retries = 5
        self.retry_delay = 1

        # initialize gracedb client
        if gracedb_service_url.startswith("file"):
            self.client = FakeGracedbClient(gracedb_service_url)
        else:
            self.client = GraceDb(gracedb_service_url)

        # initialize event storage
        self.events = OrderedDict()

        # initialize plotting options
        self.to_upload = plot
        self.max_snr = max_snr
        self.format = format
        self.output_path = output_path
        self.no_upload = no_upload

    def ingest(self, message):
        """
        parse a message from a kafka topic
        """
        payload = json.loads(message.value())

        time = LIGOTimeGPS(payload["time"], payload["time_ns"])
        coinc_fileobj = io.BytesIO(payload["coinc"].encode("utf-8"))
        psd_fileobj = copy.copy(coinc_fileobj)
        xmldoc = ligolw_utils.load_fileobj(
            coinc_fileobj, contenthandler=ligolwcontenthandler
        )
        coinc_fileobj.close()
        sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(xmldoc)
        bank_bin = "{:04}".format(int(sngl_inspiral_table[0].Gamma1))
        # No guarantee that the coinc event id will be unique between
        # bins, so use int(time) and the bank bin as an identifier,
        # which should be unique as only one event / second / bin may
        # be uploaded
        event_key = "{}_{}".format(payload["time"], bank_bin)

        if event_key not in self.events:
            self.logger.info("found new event at %s from bin %s", time, bank_bin)
            self.events[event_key] = self.new_event(time, bank_bin)

        # ranking stat
        if message.topic() == self.ranking_stat_topic:
            self.events[event_key]["ranking_data_path"] = payload["ranking_data_path"]
            # we'll just take the xmldoc from the preferred event, which will be
            # identical
            xmldoc.unlink()

        # preferred event
        elif message.topic() == self.upload_topic:
            self.logger.info("GID %s", payload["gid"])
            self.events[event_key]["gid"] = payload["gid"]
            self.events[event_key]["coinc"] = xmldoc
            self.events[event_key]["psd"] = ligolw_utils.load_fileobj(
                psd_fileobj, contenthandler=series.PSDContentHandler
            )
            self.events[event_key]["snr_optimized"] = (
                False if "snr_optimized" not in payload else payload["snr_optimized"]
            )
        psd_fileobj.close()

    def new_event(self, time, bank_bin):
        """
        returns the structure that defines an event
        """
        return {
            "time": time,
            "bin": bank_bin,
            "coinc": None,
            "gid": None,
            "psd": None,
            "ranking_data_path": None,
            "uploaded": {upload: False for upload in self.to_upload},
        }

    def handle(self):
        """
        handle aux data and plot uploading, clearing out
        old events as necessary.
        """
        for event in self.events.values():
            uploaded = event["uploaded"]
            if event["gid"]:
                if (
                    "RANKING_DATA" in uploaded
                    and not uploaded["RANKING_DATA"]
                    and event["ranking_data_path"]
                ):
                    self.upload_ranking_data(event)
                    uploaded["RANKING_DATA"] = True
                if (
                    "RANKING_PLOTS" in uploaded
                    and not uploaded["RANKING_PLOTS"]
                    and event["ranking_data_path"]
                ):
                    self.upload_ranking_plots(event)
                    uploaded["RANKING_PLOTS"] = True
                if (
                    "PSD_PLOTS" in uploaded
                    and not uploaded["PSD_PLOTS"]
                    and event["psd"]
                ):
                    self.upload_psd_plots(event)
                    uploaded["PSD_PLOTS"] = True
                if "SNR_PLOTS" in uploaded and not uploaded["SNR_PLOTS"]:
                    self.upload_snr_plots(event)
                    uploaded["SNR_PLOTS"] = True
                if (
                    "DTDPHI_PLOTS" in uploaded
                    and not uploaded["DTDPHI_PLOTS"]
                    and event["ranking_data_path"]
                ):
                    self.upload_dtdphi_plots(event)
                    uploaded["DTDPHI_PLOTS"] = True

        # clean out events once all plots are uploaded
        # and clean out old events
        current_time = utils.gps_now()
        for event_key in list(self.events.keys()):
            event = self.events[event_key]
            if (
                all(event["uploaded"].values())
                or current_time - event["time"] >= self.max_event_time
            ):
                self.logger.info(
                    "removing event from %s and bin %s", event["time"], event["bin"]
                )
                if event["coinc"] is not None:
                    self.logger.info(
                        "Did not receive path of ranking data file associated with "
                        "event from %s and bin %s",
                        event["time"],
                        event["bin"],
                    )
                    event["coinc"].unlink()
                    event["psd"].unlink()
                self.events.pop(event_key)

    def upload_file(self, message, filename, tag, contents, graceid):
        """
        upload a file to gracedb
        """
        self.logger.info("posting %s to gracedb ID %s", filename, graceid)
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.client.writeLog(
                    graceid,
                    message,
                    filename=filename,
                    filecontents=contents,
                    tagname=tag,
                )
            except HTTPError:
                self.logger.exception("HTTPError")
            else:
                if resp.status == http.client.CREATED:
                    break
            self.logger.info(
                "gracedb upload of %s for ID %s " "failed on attempt %d/%d",
                filename,
                graceid,
                attempt,
                self.retries,
            )
            time.sleep(numpy.random.lognormal(math.log(self.retry_delay), 0.5))
        else:
            self.logger.warning(
                "gracedb upload of %s for ID %s failed", filename, graceid
            )
            return False

    def upload_ranking_data(self, event):
        ranking_fobj = io.BytesIO()
        gz = "gz" if event["ranking_data_path"].endswith("gz") else False
        ligolw_utils.write_fileobj(
            load_rankingstat_xml_with_retries(event["ranking_data_path"], self.logger),
            ranking_fobj,
            compress=gz,
        )
        self.upload_file(
            "ranking statistic PDFs",
            "ranking_data.xml.gz",
            "ranking_statistic",
            ranking_fobj.getvalue(),
            event["gid"],
        )
        ranking_fobj.close()

    def upload_ranking_plots(self, event):
        # load all of the information needed to generate plots
        sngl_inspirals = dict(
            (row.ifo, row)
            for row in lsctables.SnglInspiralTable.get_table(event["coinc"])
        )
        coinc_event_table = lsctables.CoincTable.get_table(event["coinc"])
        try:
            (coinc_event,) = coinc_event_table
        except ValueError:
            raise ValueError("document does not contain exactly one candidate")

        rankingstat, rankingstatpdf = far.parse_likelihood_control_doc(
            load_rankingstat_xml_with_retries(event["ranking_data_path"], self.logger)
        )
        rankingstat.finish()
        fapfar = far.FAPFAR(rankingstatpdf.new_with_extinction())

        # generate and upload plots
        for plot_type in ["background_pdf", "injection_pdf", "zero_lag_pdf", "LR"]:
            for instrument in rankingstat.instruments:
                for chi_type in ("chi", "bankchi"):
                    if instrument in sngl_inspirals:
                        # place marker on plot
                        if sngl_inspirals[instrument].snr >= 4.0:
                            snr = sngl_inspirals[instrument].snr
                            chisq = getattr(
                                sngl_inspirals[instrument],
                                "%ssq" % chi_type.replace("bank", "bank_"),
                            )
                        else:
                            snr = None
                            chisq = None
                        fig = plotfar.plot_snr_chi_pdf(
                            rankingstat,
                            instrument,
                            plot_type,
                            chi_type,
                            self.max_snr,
                            event_snr=snr,
                            event_chisq=chisq,
                        )
                    else:
                        # no sngl for this instrument
                        fig = plotfar.plot_snr_chi_pdf(
                            rankingstat, instrument, plot_type, chi_type, self.max_snr
                        )
                    if fig is not None:
                        filename = "{}_{}_{}_snr{}.{}".format(
                            event["gid"], instrument, plot_type, chi_type, self.format
                        )
                        if not self.no_upload:
                            upload_fig(
                                fig,
                                self.client,
                                event["gid"],
                                filename=filename,
                                log_message="%s SNR, %ssq PDF" % (instrument, chi_type),
                                tagname="background",
                            )
                        if self.output_path is not None:
                            filename = os.path.join(self.output_path, filename)
                            self.logger.info("writing %s ...", filename)
                            fig.savefig(filename)
                        plt.close(fig)

        fig = plotfar.plot_likelihood_ratio_ccdf(
            fapfar,
            (
                0.0,
                max(40.0, coinc_event.likelihood - coinc_event.likelihood % 5.0 + 5.0),
            ),
            ln_likelihood_ratio_markers=(coinc_event.likelihood,),
        )
        filename = "{}_likehoodratio_ccdf.{}".format(event["gid"], self.format)
        if not self.no_upload:
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="Likelihood Ratio CCDF",
                tagname="background",
            )
        if self.output_path is not None:
            filename = os.path.join(self.output_path, filename)
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        plt.close(fig)

        fig = plotfar.plot_horizon_distance_vs_time(
            rankingstat, (event["time"] - 14400.0, event["time"]), tref=event["time"]
        )
        filename = "{}_horizon_distances.{}".format(event["gid"], self.format)
        if not self.no_upload:
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="Horizon Distances",
                tagname="psd",
            )
        if self.output_path is not None:
            filename = os.path.join(self.output_path, filename)
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        plt.close(fig)

        fig = plotfar.plot_rates(rankingstat)
        filename = "{}_rates.{}".format(event["gid"], self.format)
        if not self.no_upload:
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="Instrument combo rates",
                tagname="background",
            )
        if self.output_path is not None:
            filename = os.path.join(self.output_path, filename)
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        plt.close(fig)
        self.logger.info("finished processing ranking data plots for %s", event["gid"])

    def upload_psd_plots(self, event):
        psds = series.read_psd_xmldoc(event["psd"])
        if psds is None:
            self.logger.info("Could not get_psds, exiting loop")
            return

        #
        # PSD plot
        #

        fig = plotpsd.plot_psds(psds, event["coinc"], plot_width=800)
        fig.tight_layout()

        filename = "{}_psd.{}".format(event["gid"], self.format)
        if self.no_upload:
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        else:
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="strain spectral density plot",
                tagname="psd",
            )
        plt.close(fig)

        #
        # Cumulative SNRs plot
        #

        fig = plotpsd.plot_cumulative_snrs(psds, event["coinc"], plot_width=800)
        fig.tight_layout()

        filename = "{}_cumulative_snrs.{}".format(event["gid"], self.format)
        if self.no_upload:
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        else:
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="cumulative SNRs plot",
                tagname="psd",
            )
        plt.close(fig)

        self.logger.info("finished processing psd plot for %s", event["gid"])

    def upload_snr_plots(self, event):
        # create two dicts keyed by event id: the first dict contains
        # COMPLEX8TimeSeries which contain the snr time series, the second dict
        # contains the template row
        timeseries_ligolw_dict = dict(
            (
                ligolw_param.get_pyvalue(elem, "event_id"),
                series.parse_COMPLEX8TimeSeries(elem),
            )
            for elem in event["coinc"].getElementsByTagName(ligolw.LIGO_LW.tagName)
            if elem.hasAttribute("Name") and elem.Name == "COMPLEX8TimeSeries"
        )
        eventid_trigger_dict = dict(
            (row.event_id, row)
            for row in lsctables.SnglInspiralTable.get_table(event["coinc"])
        )

        # we don't have an autocorrelation series for an snr optimized event, so don't
        # plot it
        plot_autocorrelation = (
            False if "snr_optimized" in event and event["snr_optimized"] else True
        )

        # Parse the bank files
        # NOTE This assumes --svd-bank will also be provided once in the
        # ProcessParamsTable
        if plot_autocorrelation:
            self.logger.info("Reading svd bank...")
            bank_files = [
                row.value
                for row in lsctables.ProcessParamsTable.get_table(event["coinc"])
                if row.param == "--svd-bank"
            ]
            self.logger.info("Got process params table...")
            svd_bank_string = ",".join(
                [
                    f'{os.path.basename(file).split("-")[0]}:{file}'
                    for file in bank_files
                ]
            )

            banks = svd_bank.parse_bank_files(
                svd_bank.parse_svdbank_string(svd_bank_string), verbose=False
            )
            self.logger.info("Parsed bank files...")

            #
            # Find the template (to retrieve the autocorrelation later)
            #
            banknum = None
            for i, bank in enumerate(list(banks.values())[0]):
                for j, row in enumerate(bank.sngl_inspiral_table):
                    # The templates should all have the same template_id, so just grab
                    # one
                    if row.Gamma0 == list(eventid_trigger_dict.values())[0].Gamma0:
                        banknum = i
                        tmpltnum = j
                        break
                if banknum is not None:
                    break

            if banknum is None:
                raise ValueError(
                    "The svd banks in the process params table do not contain the "
                    "template the event was found with"
                )
            self.logger.info("Finish reading svd bank...")

        #
        # Plot the time series and the expected snr
        #
        fig = figure.Figure()
        FigureCanvas(fig)

        self.logger.info("Begin plotting snr time series...")
        zero_pad = 4
        for i, (eventid, complex8timeseries) in enumerate(
            timeseries_ligolw_dict.items()
        ):
            ifo = eventid_trigger_dict[eventid].ifo
            autocorr_length = complex8timeseries.data.length

            # add zero pad as safety in case the peak offset is not the center of snr
            # timeseries
            time = numpy.linspace(
                float(complex8timeseries.epoch) - zero_pad * complex8timeseries.deltaT,
                float(complex8timeseries.epoch)
                + (autocorr_length + zero_pad - 1) * complex8timeseries.deltaT,
                autocorr_length + zero_pad * 2,
            )
            complex_snr_timeseries = numpy.concatenate(
                [
                    numpy.zeros(zero_pad),
                    complex8timeseries.data.data,
                    numpy.zeros(zero_pad),
                ]
            )
            if plot_autocorrelation:
                auto = numpy.concatenate(
                    [
                        numpy.zeros(zero_pad),
                        banks[ifo][banknum].autocorrelation_bank[tmpltnum],
                        numpy.zeros(zero_pad),
                    ]
                )

            peakoffset = numpy.argmin(abs(time - eventid_trigger_dict[eventid].end))
            phase = numpy.angle(complex_snr_timeseries[peakoffset])
            snr = (complex_snr_timeseries * numpy.exp(-1.0j * phase)).real
            snrsigma = numpy.sqrt(2)
            peaktime = time[peakoffset]
            time -= peaktime
            maxsnr = snr.max()

            lo_idx = int(peakoffset - (autocorr_length - 1) / 2)
            hi_idx = int(peakoffset + (autocorr_length + 1) / 2 + 1)

            ax = fig.add_subplot(len(timeseries_ligolw_dict.items()), 1, i + 1)
            ax.fill_between(
                time[lo_idx:hi_idx],
                snr[lo_idx:hi_idx] - snrsigma,
                snr[lo_idx:hi_idx] + snrsigma,
                color="0.75",
            )
            ax.plot(
                time[lo_idx:hi_idx],
                snr[lo_idx:hi_idx],
                "k",
                label=r"$\mathrm{Measured}\,\rho(t)$",
            )
            if plot_autocorrelation:
                ax.plot(
                    time[lo_idx:hi_idx],
                    auto.real[lo_idx:hi_idx] * maxsnr,
                    "b--",
                    label=r"$\mathrm{Scaled\,Autocorrelation}$",
                )
            ax.set_xlim(time[lo_idx], time[hi_idx])
            ax.set_ylabel(r"$\mathrm{{{}}}\,\rho(t)$".format(ifo))
            ax.set_xlabel(r"$\mathrm{{Time\,from\,{}}}$".format(peaktime))
            ax.legend(loc="upper right")
            ax.grid()

        fig.tight_layout()
        filename = "{}_snrtimeseries.{}".format(event["gid"], self.format)

        if not self.no_upload:
            self.logger.info("writing %s ...", filename)
            upload_fig(
                fig,
                self.client,
                event["gid"],
                filename=filename,
                log_message="SNR time series",
                tagname="background",
            )

        if self.output_path is not None:
            filename = os.path.join(self.output_path, filename)
            self.logger.info("writing %s ...", filename)
            fig.savefig(filename)
        plt.close(fig)

        self.logger.info(
            "finished processing SNR time series plot for %s", event["gid"]
        )

    def upload_dtdphi_plots(self, event):
        self.logger.info("Begin plotting dtdphi plots for %s...", event["gid"])
        sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(event["coinc"])
        offset_vectors = lsctables.TimeSlideTable.get_table(event["coinc"]).as_dict()
        assert (
            len(offset_vectors) == 1
        ), "the time slide table has to have exactly one time-slide entry."

        offset_vector = offset_vectors[list(offset_vectors)[0]]

        rankingstat = LnLikelihoodRatio.from_xml(
            load_rankingstat_xml_with_retries(event["ranking_data_path"], self.logger)
        )
        rankingstat.finish()  # FIXME: is this needed?

        # Map sgnl_inspiral rows to trigger columns
        # FIXME: should this be in strike?
        triggers = []
        for row in sngl_inspiral_table:
            if row.chisq is not None:  # FIXME: sometimes chisq is None breaks the code
                triggers.append(
                    {
                        "__trigger_id": row.event_id,
                        "_filter_id": row.template_id,
                        "ifo": row.ifo,
                        "time": row.end,
                        "snr": row.snr,
                        "chisq": row.chisq,
                        "phase": row.coa_phase,
                        "Gamma2": row.Gamma2,
                        "epoch_start": row.end - row.template_duration,
                        "epoch_end": row.end,
                    }
                )
        # FIXME: the "time" and "epoch" needs to be fixed in strike to be in nanoseconds
        event_kwargs = rankingstat.kwargs_from_triggers(triggers, offset_vector)
        ifos = sorted(event_kwargs["snrs"])
        ifo1 = ifos.pop(0)
        snrs = event_kwargs["snrs"]
        # remove segments for ifos not having horizon distance after
        # compression, which would cause an error inside
        # local_mean_horizon_distance()
        segs_nonzero = {
            ifo: seg
            for ifo, seg in event_kwargs["segments"].items()
            if len(rankingstat.terms["P_of_tref_Dh"].horizon_history[ifo]) > 0
        }
        horizons = rankingstat.terms["P_of_tref_Dh"].local_mean_horizon_distance(
            segs_nonzero, template_id=event_kwargs["template_id"]
        )

        # generate and upload plots
        for ifo2 in ifos:
            ifo_pair = ifo1[0] + ifo2[0]
            dt_ref = event_kwargs["dt"][ifo2] - event_kwargs["dt"][ifo1]
            dphi_ref = (
                event_kwargs["dphi"][ifo2] - event_kwargs["dphi"][ifo1]
            )  # FIXME: check with Leo if phase->dphi is correct

            fig = plot_dtdphi(
                rankingstat.terms["P_of_dt_dphi"],
                ifo1,
                ifo2,
                snrs,
                horizons,
                event_dt=dt_ref,
                event_dphi=dphi_ref,
            )
            filename = "{}_dtdphi_{}.{}".format(event["gid"], ifo_pair, self.format)
            if not self.no_upload:
                upload_fig(
                    fig,
                    self.client,
                    event["gid"],
                    filename=filename,
                    log_message="{} dtdphi 2D pdf plot".format(ifo_pair),
                    tagname=("dtdphi", "background"),
                )
            if self.output_path is not None:
                filename = os.path.join(self.output_path, filename)
                self.logger.info("writing %s ...", filename)
                fig.savefig(filename)
            plt.close(fig)
            self.logger.info(
                "finished processing %s dtdphi pdf plot for %s", ifo_pair, event["gid"]
            )

    def finish(self):
        """
        upload remaining files/plots before shutting down
        """
        self.handle()


# -------------------------------------------------
#                     Main
# -------------------------------------------------


def main():
    # parse arguments
    options = parse_command_line()

    # check input options
    allowed_plots = [p.name for p in Plots]
    if not options.plot:
        # make all allowed plots by default
        options.plot = allowed_plots
    else:
        for p in options.plot:
            if p not in allowed_plots:
                raise ValueError(
                    f"Unsupported option {p} for --plot. Allowed options are: "
                    f"{allowed_plots}"
                )

    # set up logging
    log_level = logging.INFO if options.verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s | ll_inspiral_event_plotter : %(levelname)s : %(message)s"
    )
    logger = logging.getLogger("ll_inspiral_event_plotter")
    logger.setLevel(log_level)

    # create event plotter instance
    event_plotter = EventPlotter(
        format=options.format,
        gracedb_service_url=options.gracedb_service_url,
        kafka_server=options.kafka_server,
        logger=logger,
        max_event_time=options.max_event_time,
        max_snr=options.max_snr,
        no_upload=options.no_upload,
        output_path=options.output_path,
        plot=options.plot,
        processing_cadence=options.processing_cadence,
        ranking_stat_topic=options.ranking_stat_topic,
        request_timeout=options.request_timeout,
        tag=options.tag,
        upload_topic=options.upload_topic,
    )

    # start up
    event_plotter.start()
