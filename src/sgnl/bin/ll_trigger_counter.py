"""Keep track of zero lag counts clustered over 10s fixed windows"""

# Copyright (C) 2016  Kipp Cannon, Chad Hanna
# Copyright (C) 2019  Patrick Godwin
# Copyright (C) 2025  Yun-Jing Huang


import argparse
import io
import json
import logging
import os
import threading
import time
import timeit
from collections import deque
from queue import Queue

from lal.utils import CacheEntry
from ligo.lw import utils as ligolw_utils
from ligo.scald import aggregator
from ligo.scald.io import kafka
from sgn.control import HTTPControl
from sgnligo.base import now as time_now
from strike.stats import far

from sgnl import events
from sgnl.dags.util import DEFAULT_BACKUP_DIR, T050017_filename

#
# =============================================================================
#
#                                 Command Line
#
# =============================================================================
#

program_name = "sgnl-ll-trigger-counter"


def xml_string(rstat):
    f = io.BytesIO()
    rstat.save_fileobj(f)
    f.seek(0)
    return f.read().decode("utf-8")


class ZeroLagCounts(events.EventProcessor):
    """
    A Class to keep track of zero lag counts clustered over 10s fixed windows
    """

    _name = "trigger_counter"

    def __init__(
        self,
        kafka_server,
        logger,
        sgn_control,
        output="zerolag_rankingstatpdf.xml.gz",
        output_period=3600,
        process_params=None,
        tag="test",
        topic="coinc",
        verbose=False,
    ):
        """
        fname is the name of the zerolag file read from disk to start the counting
        """
        if process_params is None:
            self.process_params = {}
        else:
            self.process_params = process_params
        self.logger = logger
        self.lock = threading.Lock()
        self.fname = output
        self.verbose = verbose
        self.time_since_last = time.time()
        self.output_period = output_period

        if os.access(ligolw_utils.local_path_from_url(self.fname), os.R_OK):
            self.zerolag_rankingstatpdf = far.RankingStatPDF.load(
                self.fname, verbose=verbose
            )
        else:
            raise ValueError("zerolag ranking stat PDF does not exist")
        self.counts_dict = {}

        # set up kafka client to continuously listen for messages
        events.EventProcessor.__init__(
            self,
            input_topic=f"sgnl.{tag}.{topic}",
            kafka_server=kafka_server,
            num_messages=10000,
            tag=f"{tag}-counter",
            send_heartbeats=True,
            heartbeat_cadence=60.0,
            heartbeat_topic=f"sgnl.{tag}.trigger_counter_heartbeat",
            sgn_control=sgn_control,
        )

        # store incoming messages in a deque before handling them
        self.msgs = deque()
        self.state_dict = {
            "zerolagxml": {"all": xml_string(self.zerolag_rankingstatpdf)}
        }
        HTTPControl.exchange_state(self._name, self.state_dict)

    def ingest(self, message):
        # keep list of incoming triggers
        if message.key():
            key = message.key().decode("utf-8").split(".")
        else:
            key = []
        self.msgs.append(
            kafka.Message(
                message.topic(),
                message.partition(),
                message.offset(),
                tuple(key),
                json.loads(message.value().decode("utf-8")),
                message.timestamp()[0],
            )
        )

    def handle(self):
        triggers = aggregator.parse_triggers(self.msgs)

        # clear out msgs deque to avoid double counting triggers
        self.msgs.clear()

        # store and reduce data for each job
        if triggers:
            start = timeit.default_timer()
            self.logger.debug("adding triggers")

            num = self.add_coincs(triggers)

            elapsed = timeit.default_timer() - start
            self.logger.debug("time to add %d triggers: %.1f s", num, elapsed)
        else:
            self.logger.debug("no triggers to process")

        if (time.time() - self.time_since_last) > self.output_period:
            self.logger.info("making snapshot")
            self.snapshot_output_url()
            self.time_since_last = time.time()

    def add_coincs(self, coincs):
        """
        Iterate over a list of coincs to extract their gps time and
        likelihood ratio value.  GPS times are rounded to the nearest 10s in order to
        cluster events. The max likelihood is tracked in the dictionary over the 10s
        window. Nothing is added to the zerolag histograms. The counts are purely
        internal. Only when a new zerolag histogram is requested will the zerolag
        histogram be updated. That makes it easier to manage late buffers.  No
        additional logic required.  The number of coincs added is returned for
        logging purposes
        """
        cnt = 0
        for c in coincs:
            # avoid trigs before LR assigned
            if "likelihood" in c:
                # equiv to 10s clustering
                key = round(c["end"], -1)
                lr = c["likelihood"]
                self.counts_dict[key] = max(self.counts_dict.setdefault(key, lr), lr)
                cnt += 1
        return cnt

    def __add_counts_to_zerolag_pdf(self, zlpdf):
        """
        Add the internal counts to a zerolag histogram. NOTE!!!! This
        should never be called on the internal zerolag histogram, only on a copy. That
        is why this method is not invoked unless someone is requesting an updated file
        """
        for lr in self.counts_dict.values():
            zlpdf.zero_lag_lr_lnpdf.count[lr,] += 1

    def snapshot_output_url(self):
        """
        Write a new XML doc with the internal counts added to disk with fname
        """
        with self.lock:
            HTTPControl.exchange_state(self._name, self.state_dict)
            zlpdf = self.zerolag_rankingstatpdf.copy()
            self.__add_counts_to_zerolag_pdf(zlpdf)
            zlpdf.save(
                self.fname,
                process_name=program_name,
                process_params=self.process_params,
                verbose=self.verbose,
            )
            self.state_dict["zerolagxml"]["all"] = xml_string(zlpdf)

        # save the same file to the backup dir as a precaution
        with self.lock:
            now = int(time_now())
            f = CacheEntry.from_T050017(self.fname)
            backup_dir = os.path.join(DEFAULT_BACKUP_DIR, os.path.dirname(self.fname))
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            backup_fname = T050017_filename(
                f.observatory, f.description, (now, now), "xml.gz"
            )
            backup_fname = os.path.join(backup_dir, backup_fname)
            zlpdf.save(
                backup_fname,
                process_name=program_name,
                process_params=self.process_params,
                verbose=self.verbose,
            )


# Read command line options
def parse_command_line():

    parser = argparse.ArgumentParser(description="Online trigger counter")

    # directory to put everything in
    parser.add_argument(
        "--topic",
        default="coinc",
        help="Specify the topic where triggers are located.",
    )
    parser.add_argument(
        "--output-period",
        type=float,
        default=3600.0,
        help="Wait this many seconds between writing the output file (default = 3600)",
    )
    parser.add_argument(
        "--kafka-server",
        metavar="string",
        help="Sets the server url that the kafka topic is hosted on. Required.",
    )
    parser.add_argument(
        "--output",
        default="zerolag_rankingstatpdf.xml.gz",
        help="Choose the output file. Default zerolag_rankingstatpdf.xml.gz",
    )
    parser.add_argument(
        "--tag",
        metavar="string",
        default="test",
        help="Sets the name of the tag used. Default = 'test'",
    )
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true", help="Be verbose."
    )

    args = parser.parse_args()

    return args


#
# =============================================================================
#
#                                     Main
#
# =============================================================================
#


def main():
    options = parse_command_line()
    process_params = options.__dict__.copy()

    # set up logging
    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s | sgnl-ll-trigger-counter : %(levelname)s : %(message)s"
    )
    logger = logging.getLogger("trigger_counter")
    logger.setLevel(log_level)

    port = 40000  # FIXME: do we use other ports??
    HTTPControl.port = port
    HTTPControl.tag = options.tag
    HTTPControl.registry_file = "sgnl_trigger_counter_registry.txt"
    HTTPControl.get_queues["trigger_counter"] = Queue(1)
    HTTPControl.post_queues["trigger_counter"] = Queue(1)
    with HTTPControl() as sgn_control:
        ZLC = ZeroLagCounts(
            kafka_server=options.kafka_server,
            logger=logger,
            sgn_control=sgn_control,
            output=options.output,
            output_period=options.output_period,
            process_params=process_params,
            tag=options.tag,
            topic=options.topic,
            verbose=options.verbose,
        )

        # start an infinite loop to keep updating and aggregating data
        logger.info("starting up...")
        ZLC.start()
