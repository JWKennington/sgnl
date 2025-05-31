"""This program runs sgnl-assign-likelihood in a while True loop

This program is not meant to be executed standalone by a user. It should be part of a
DAG managing a running sgnl-inspiral online analysis.

This program takes two or more arguments;

1. The path of the output file name
2. One or more paths to text files each containing a single line giving the root URL of
a web server from which to retrieve event parameter distribution data (e.g.,
"http://node001.ligo.caltech.edu")

This program queries each running sgnl-inspiral job via the URL, computes PDFs of the
likelihood ratio ranking statistics from the parameter distribution data, then
marginalizes the ranking statistic PDFs across jobs and writes the result to the given
output file.

It continues to do that in an infinite loop with a 10 minute pause on each iteration.
Files are not overwritten directly, but rather via a temporary file and mv operation to
ensure that no files are corrupted in a POSIX file environment.
"""

# Copyright (C) 2012,2014  Kipp Cannon
# Copyright (C) 2024       Yun-Jing Huang

import logging
import os
import sys
import time
from argparse import ArgumentParser
from collections import deque
from urllib.error import HTTPError, URLError

from lal.utils import CacheEntry
from sgnligo.base import now as time_now
from strike.stats import far

from sgnl import events
from sgnl.dags.util import DEFAULT_BACKUP_DIR, DataCache, DataType, T050017_filename

program_name = "sgnl-ll-marginalize-likelihoods-online"


def parse_command_line():
    parser = ArgumentParser()

    parser.add_argument(
        "--output",
        metavar="path",
        help="Set the path where the output marginalized PDF is stored",
    )
    parser.add_argument(
        "--registry",
        metavar="filename",
        action="append",
        help="The registry filenames.",
    )
    parser.add_argument(
        "-j",
        "--num-cores",
        metavar="cores",
        default=1,
        type=int,
        help="Number of cores to use when constructing ranking statistic histograms "
        "(default = 1 cores).",
    )
    parser.add_argument(
        "--output-kafka-server",
        metavar="addr",
        help="Set the server address and port number for output data. Optional, e.g., "
        "10.14.0.112:9092",
    )
    parser.add_argument(
        "--tag",
        metavar="string",
        default="test",
        help="Sets the name of the tag used. Default = 'test'",
    )
    parser.add_argument(
        "--ifo",
        metavar="ifo",
        action="append",
        help="ifos with which to create output filenames if they don't already exist",
    )
    parser.add_argument(
        "--fast-burnin",
        action="store_true",
        help="Set whether to enable fast burn-in or not. If enabled, only 67 percent "
        "of bins are required to contribute to the marginalized PDF",
    )
    parser.add_argument(
        "--extinct-percent",
        action="store",
        type=float,
        default=0.99,
        help="Set percentage of bins required to contribute to the marginalized PDF",
    )
    parser.add_argument("--verbose", action="store_true", help="Be verbose.")
    options = parser.parse_args()

    if options.output is None:
        raise ValueError("must set --output.")

    if options.registry is None:
        raise ValueError("must provide at least one registry file.")

    return options


def calc_rank_pdfs(url, samples, num_cores, verbose=False):
    """
    compute a Ranking Stat PDF from a url
    """
    logger = logging.getLogger("ll-marginalize-likelihoods-online")
    try:
        rankingstat = far.marginalize_pdf_urls(
            [url], "LnLikelihoodRatio", verbose=verbose
        )
    except (URLError, HTTPError):
        logger.exception("Caught error while running calc rank pdfs")
        return 0, None

    lr_rankingstat = rankingstat.copy()
    lr_rankingstat.finish()
    if lr_rankingstat.is_healthy():
        rankingstatpdf = far.RankingStatPDF(
            lr_rankingstat,
            signal_noise_pdfs=None,
            nsamples=samples,
            nthreads=num_cores,
            verbose=verbose,
        )
    else:
        # create an empty rankingstat pdf, so that the template ids
        # from this bin get added to the marg rankingstat pdf
        rankingstatpdf = far.RankingStatPDF(
            lr_rankingstat,
            signal_noise_pdfs=None,
            nsamples=0,
            nthreads=num_cores,
            verbose=verbose,
        )
        # wait a bit so that we're not constatntly firing HTTP
        # requests to jobs at the start of an analysis
        time.sleep(2)

    return 1, rankingstatpdf


def process_svd_bin(
    reg,
    svd_bin,
    likelihood_path,
    zerolag_counts_path,
    pdfs,
    ranking_stat_samples,
    num_cores,
    verbose=False,
    process_params=None,
):
    """
    This method does the following operations:
    1. loads the old PDF for this bin
    2. calls calc_rank_pdfs to compute the new PDF for this bin
    3. adds the two together
    4. saves the sum to disk if the new PDF was successfully calculated
    5. gets the zerolag for that bin and performs bin-specific (first round) extinction
    6. returns the success status of the new PDF, extinction status of the old+new PDF,
       and the extincted PDF
    """

    if process_params is None:
        process_params = {}

    logger = logging.getLogger("ll-marginalize-likelihoods-online")
    logger.info("Querying registry %s...", reg)
    pdf_path = pdfs[svd_bin].files[0]
    url = url_from_registry(reg, likelihood_path + "/" + svd_bin)

    if os.path.isfile(pdf_path):
        # load the old ranking stat pdf for this bin:
        old_pdf = far.RankingStatPDF.load(pdf_path, verbose=verbose)
    else:
        # FIXME: should we provide an empty file in the setup dag?
        logger.warning("Couldn't find %s, starting from scratch", pdf_path)
        old_pdf = None

    logger.info("calc_rank_pdfs...")
    # create the new ranking stat pdf and marginalize as we go
    new_pdf_status, pdf = calc_rank_pdfs(
        url, ranking_stat_samples, num_cores, verbose=verbose
    )
    logger.info("Finished calc_rank_pdfs")

    # add the old and new pdfs if they are available
    if new_pdf_status and old_pdf:
        # both are available
        pdf += old_pdf
    if not new_pdf_status and old_pdf:
        # only the old pdf is available, overwrite pdf with it
        pdf = old_pdf

    # make sure the zerolag in the pdf is empty
    if pdf:
        pdf.zero_lag_lr_lnpdf.count.array[:] = 0.0

    logger.info("Saving %s...", pdf_path)
    # save the new PDF + old PDF (if it exists) to disk
    if new_pdf_status:
        pdf.save(
            pdf_path,
            process_name=program_name,
            process_params=process_params,
            verbose=verbose,
        )

    extinction_status = 0
    if pdf:
        # get the zerolag pdf for this bin and use it to perform bin-specific extinction
        zerolag_counts_url = url_from_registry(reg, zerolag_counts_path + "/" + svd_bin)
        try:
            pdf += far.RankingStatPDF.load(
                zerolag_counts_url,
                verbose=verbose,
            )
        except (URLError, HTTPError):
            logger.exception(
                "Caught error while trying to get zerolag for bin %s", svd_bin
            )
        if pdf.ready_for_extinction():
            # LR calculation has started and we are ready to perform first-round
            # extinction
            extinction_status = 1
            logger.info("new_with_extinction...")
            pdf = pdf.new_with_extinction()
        else:
            # add a zeroed-out PDF instead, so that the template ids get added to data
            logger.warning(
                "Skipping first-round extinction for %s, using an empty PDF instead",
                pdfs[svd_bin].files[0],
            )
            pdf.noise_lr_lnpdf.array[:] = 0.0
            pdf.signal_lr_lnpdf.array[:] = 0.0
            pdf.zero_lag_lr_lnpdf.array[:] = 0.0

    return new_pdf_status, extinction_status, pdf


def url_from_registry(registry, path):
    """
    parse a registry file and return
    the url to given path
    """
    with open(registry, "r") as f:
        server = f.read()
    server = server.replace("\n", "")
    logger = logging.getLogger("ll-marginalize-likelihoods-online")
    logger.info("server: %s", server)

    return os.path.join(server, path)


def main():
    options = parse_command_line()

    process_params = options.__dict__.copy()

    # set up logging
    log_level = logging.INFO if options.verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s | ll-marginalize-likelihoods-online: %(levelname)s : "
        "%(message)s"
    )
    logger = logging.getLogger("ll-marginalize-likelihoods-online")
    logger.setLevel(log_level)

    registries = options.registry
    svd_bins = []
    reg_dict = {}
    for r in registries:
        name = r.split("_")
        if len(name) == 4:
            new_svd_bins = ["%04d" % i for i in range(int(name[0]), int(name[1]) + 1)]
            svd_bins.extend(new_svd_bins)
        elif len(name) == 3:
            new_svd_bins = ["%04d" % int(name[0])]
            svd_bins.extend(new_svd_bins)
        else:
            raise ValueError("Wrong name for registry file.")

        for svd_bin in new_svd_bins:
            reg_dict[svd_bin] = r

    # options for generating ranking stat pdfs
    # get 10 million samples
    ranking_stat_samples = int(10000000 / len(svd_bins))

    #
    # set up the output paths
    #

    pdfs = DataCache.generate(
        DataType.RANK_STAT_PDFS,
        CacheEntry.from_T050017(options.output).observatory,
        svd_bins=svd_bins,
    )
    pdfs = pdfs.groupby("svd_bin")

    #
    # paths to data objects on each job's web management interface
    #

    likelihood_path = options.tag + "/get/application/xml/StrikeSnk/xml"
    zerolag_counts_path = options.tag + "/get/application/xml/StrikeSnk/zerolagxml"

    #
    # send heartbeat messages for the purpose of monitoring
    #
    if options.output_kafka_server:
        kafka_processor = events.EventProcessor(
            kafka_server=options.output_kafka_server,
            tag=options.tag,
            send_heartbeats=True,
            heartbeat_cadence=60.0,
            heartbeat_topic=f"sgnl.{options.tag}."
            "marginalize_likelihoods_online_heartbeat",
        )
    else:
        kafka_processor = None

    #
    # pause on start up to make sure all inspiral jobs are running
    #

    sleep = 600
    logger.info("... sleeping for %d seconds ...", sleep)
    time.sleep(sleep)

    if kafka_processor:
        kafka_processor.heartbeat()

    #
    # loop forever
    #

    while True:
        # collect the current coinc parameter PDF data from each job, and
        # run some iterations of the ranking statistic sampler to generate
        # noise and signal model ranking statistic histograms for each job.
        # NOTE:  the zero-lag ranking statistic histograms in the files
        # generated here are all 0.
        data = None
        failed = deque(maxlen=len(svd_bins))
        num_extincted = (
            0  # number of bins for whom we were able to perform first-round extinction
        )

        for svd_bin, reg in reg_dict.items():
            # process every svd bin, retry twice if it failed
            for _ in range(3):
                status, extinction_status, pdf = process_svd_bin(
                    reg,
                    svd_bin,
                    likelihood_path,
                    zerolag_counts_path,
                    pdfs,
                    ranking_stat_samples,
                    options.num_cores,
                    verbose=options.verbose,
                    process_params=process_params,
                )
                if status:
                    # add pdf to data
                    if data:
                        data += pdf
                    else:
                        data = pdf
                    num_extincted += extinction_status
                    break

            if not status:
                logger.info(
                    "failed to complete bin %s registry %s during regular running",
                    svd_bin,
                    reg,
                )
                failed.append(svd_bin)

            # while looping through registries
            # send heartbeat messages
            if kafka_processor:
                kafka_processor.heartbeat()

        # retry registries that we failed to process the first time
        # and remove from the deque upon success
        for svd_bin in list(failed):
            status, extinction_status, pdf = process_svd_bin(
                reg_dict[svd_bin],
                svd_bin,
                likelihood_path,
                zerolag_counts_path,
                pdfs,
                ranking_stat_samples,
                options.num_cores,
                verbose=options.verbose,
                process_params=process_params,
            )
            if status:
                logger.info(
                    "completed bin %s registry %s on final retry",
                    svd_bin,
                    reg_dict[svd_bin],
                )
                failed.remove(svd_bin)
                # add pdf to data
                if data:
                    data += pdf
                else:
                    data = pdf
            else:
                logger.info(
                    "failed to complete bin %s registry %s on final retry",
                    svd_bin,
                    reg_dict[svd_bin],
                )
                # add pdf to data anyway, as this will contain the extincted
                # old pdf for this bin. Adding the old pdf for this bin is
                # better than adding no pdf for this bin. Don't remove the
                # registry from the failed deque
                if pdf:
                    if data:
                        data += pdf
                    else:
                        data = pdf

            num_extincted += extinction_status

            if kafka_processor:
                kafka_processor.heartbeat()

        # zero out the zerolag after the first round of extinction is finished
        if data:
            data.zero_lag_lr_lnpdf.count.array[:] = 0

        # if we fail to complete more than 1% of the bins,
        # this is a serious problem and we should just quit
        # and restart from scratch
        if len(failed) >= 0.01 * len(svd_bins):
            logger.critical(
                "Failed to complete %d svd_bins out of %d, exiting.",
                len(failed),
                len(svd_bins),
            )
            sys.exit(1)

        # otherwise, lets cut our losses and continue
        logger.info("Done with calc rank pdfs. Failed svd_bins: %s", str(failed))

        # sum the noise and signal model ranking statistic histograms
        # across jobs, and collect and sum the current observed zero-lag
        # ranking statistic histograms from all jobs. combine with the
        # noise and signal model ranking statistic histograms.  NOTE:  the
        # noise and signal model ranking statistic histograms in the
        # zero-lag counts files downloaded from the jobs must be all 0, and
        # the zero-lag counts in the output generated by
        # sgnl_calc_rank_pdfs must be 0.  NOTE:  this is where
        # the zero-lag counts have the density estimation transform
        # applied.

        zerolag_counts_path_all = (
            options.tag + "/get/application/xml/trigger_counter/zerolagxml/all"
        )
        zerolag_counts_url = url_from_registry(
            "sgnl_trigger_counter_registry.txt", zerolag_counts_path_all
        )

        # add zerolag counts url to marginalized data
        if data:
            data += far.RankingStatPDF.load(
                zerolag_counts_url,
                verbose=options.verbose,
            )
        else:
            data = far.RankingStatPDF.load(
                zerolag_counts_url,
                verbose=options.verbose,
            )

        if kafka_processor:
            kafka_processor.heartbeat()

        # apply density estimation and normalize the PDF
        data.density_estimate_zero_lag_rates()

        # write output document only if 99% of bins have been extincted and
        # hence have contributed to the noise diststat PDF. Otherwise, the
        # PDF will not be representative of the LRs across all bins
        # If fast burn-in is enabled, this value is changed to 67%. This is
        # taken from how many bins are ready_for_extinction after a 2 week
        # period in an HL EW analysis with 1000 templates / bin
        # NOTE: This option is not meant to be used for analyses other than
        # the one mentioned above.
        # if num_extincted >= 0.99 * len(registries) or (
        if num_extincted >= options.extinct_percent * len(svd_bins) or (
            options.fast_burnin and num_extincted >= 0.666667 * len(svd_bins)
        ):
            data.save(
                options.output,
                process_name=program_name,
                process_params=process_params,
                verbose=options.verbose,
            )

            # save the same file to the backup dir as a precaution
            now = int(time_now())
            f = CacheEntry.from_T050017(options.output)
            backup_dir = os.path.join(
                DEFAULT_BACKUP_DIR, os.path.dirname(options.output)
            )
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            backup_fname = T050017_filename(
                f.observatory, f.description, (now, now), "xml.gz"
            )
            backup_fname = os.path.join(backup_dir, backup_fname)
            data.save(
                backup_fname,
                process_name=program_name,
                process_params=process_params,
                verbose=options.verbose,
            )

        logger.info("Done marginalizing likelihoods.")

        if kafka_processor:
            kafka_processor.heartbeat()

    sys.exit(1)
