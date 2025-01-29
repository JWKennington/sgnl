"""an executable to calculate p(astro) values and upload to GraceDB events"""

# Copyright (C)      Becca Ewing
# Copyright (C) 2024 Yun-Jing Huang

__usage__ = "sgnl-ll-inspiral-pastro-uploader [--options]"
__author__ = (
    "Rebecca Ewing (rebecca.ewing@ligo.org), "
    "Yun-Jing Huang (yun-jing.huang@ligo.org)"
)

# -------------------------------------------------
#                   Preamble
# -------------------------------------------------

import copy
import http.client as httplib
import json
import logging
import os
import shutil
import tempfile
import time
from argparse import ArgumentParser
from collections import deque
from io import BytesIO

from ligo.gracedb.rest import GraceDb, HTTPError
from ligo.lw import ligolw, lsctables
from ligo.lw import utils as ligolw_utils
from ligo.scald import utils
from pastro import pastro  # FIXME move pastro to sgn?

from sgnl import events
from sgnl.gracedb import FakeGracedbClient


class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


lsctables.use_in(LIGOLWContentHandler)

# -------------------------------------------------
#                   Functions
# -------------------------------------------------


def parse_command_line():

    parser = ArgumentParser(usage=__usage__, description=__doc__)
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true", help="Be verbose."
    )
    parser.add_argument(
        "--num-messages",
        type=int,
        default=10000,
        help="number of messages to process per cadence",
    )
    parser.add_argument(
        "--tag",
        metavar="string",
        default="test",
        help="Sets the name of the tag used. Default = 'test'",
    )
    parser.add_argument(
        "--processing-cadence",
        type=float,
        default=0.1,
        help="Rate at which the event uploader acquires and processes data. Default = "
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
        "--input-topic",
        metavar="string",
        action="append",
        help="Sets the input kafka topic(s). Required.",
    )
    parser.add_argument(
        "--gracedb-service-url",
        metavar="url",
        help="Override default GracedB service url.",
    )
    parser.add_argument(
        "--pastro-filename",
        metavar="string",
        default="p_astro.json",
        help="Name to upload the p(astro) file with. Default is p_astro.json.",
    )
    parser.add_argument(
        "--model-name", metavar="string", help="Name of pastro model used, eg FGMC."
    )
    parser.add_argument(
        "--pastro-model-file", metavar="file", help="Filename of pastro model."
    )
    parser.add_argument(
        "--rank-stat",
        metavar="file",
        help="Filename of ranking stat pdf to update the pastro model with.",
    )

    options = parser.parse_args()

    return options


# -------------------------------------------------
#                    Classes
# -------------------------------------------------


class PAstroUploader(events.EventProcessor):
    def __init__(
        self,
        gracedb_service_url: str,
        input_topic: list[str],
        kafka_server: str,
        logger: logging.Logger,
        model_name: str,
        pastro_model_file: str,
        rank_stat: str,
        num_messages: int = 10000,
        pastro_filename: str = "p_astro.json",
        processing_cadence: float = 0.1,
        request_timeout: float = 0.2,
        tag: str = "test",
    ):
        self.logger = logger
        self.logger.info("setting up pastro uploader...")

        self.tag = tag
        self.model_name = model_name
        self.max_retries = 3
        self.filename = pastro_filename
        self.pastro_model_file = pastro_model_file
        self.p_astro_topic = f"sgnl.{self.tag}.p_astro"

        self.rank_stat = rank_stat
        self.last_rankstat_update = None
        self.update_rankstat_cadence = 4.0 * 3600.0

        self.is_injection_job = input_topic[0] == "inj_uploads"
        heartbeat_topic = (
            f"sgnl.{tag}.pastro_uploader_heartbeat"
            if not self.is_injection_job
            else f"sgnl.{tag}.inj_pastro_uploader_heartbeat"
        )

        # init the EventProcessor
        events.EventProcessor.__init__(
            self,
            process_cadence=processing_cadence,
            request_timeout=request_timeout,
            num_messages=num_messages,
            kafka_server=kafka_server,
            input_topic=[f"sgnl.{tag}.{topic}" for topic in input_topic],
            tag="-".join([self.tag, self.model_name]),
            send_heartbeats=True,
            heartbeat_cadence=60.0,
            heartbeat_topic=heartbeat_topic,
        )

        # set up gracedb client
        if gracedb_service_url.startswith("file"):
            self.client = FakeGracedbClient(gracedb_service_url)
        else:
            self.client = GraceDb(gracedb_service_url)

        # load model file
        self.model = self.load_model(self.pastro_model_file)
        # attempt to initialize model with ranking stat info
        self.add_rankstat_to_model()

        # start a list of events
        self.events = deque(maxlen=100)

    def ingest(self, message):
        # load the message value
        msg = json.loads(message.value())

        # append to list of messages to be handled
        # these messages come post aggregation, so
        # we will calculate a pastro for each one
        # without checking for duplicates
        self.events.append(
            {
                "gid": msg["gid"],
                "time": msg["time"] + msg["time_ns"] * 10.0**-9.0,
                "coinc": self.load_xmlobj(msg["coinc"]),
                "pastro": None,
                "snr_optimized": msg["snr_optimized"],
                "upload_attempts": 0,
                "upload_success": False,
            }
        )

        # update pastro model with ranking stat info
        t = utils.gps_now()
        if (
            not self.last_rankstat_update
            or t - self.last_rankstat_update >= self.update_rankstat_cadence
        ):
            self.add_rankstat_to_model()

    def handle(self):
        # if we have not updated the model with
        # ranking stat info yet, we cant
        # accurately compute pastros. Check that
        # this is done before continuing.
        if not self.last_rankstat_update:
            return

        # for all events in the list, calculate
        # the pastro and upload to GraceDB
        for event in copy.copy(self.events):
            if (
                not event["upload_success"]
                and event["upload_attempts"] < self.max_retries
            ):
                self.logger.debug("Processing %s...", event["gid"])
                pastro = self.calculate_pastro(event)
                # FIXME: hard code this value until pastro dependency is resolved
                event["pastro"] = pastro

                # attempt to upload the file
                response = self.upload_file(
                    event["gid"],
                    "SGNL internally computed p-astro",
                    self.filename,
                    pastro,
                    f"{self.model_name}_p_astro",
                )

                # after successful upload, add label
                if response:
                    try:
                        response = self.client.writeLabel(event["gid"], "PASTRO_READY")
                    except HTTPError:
                        self.logger.exception("except HTTPError")
                    else:
                        # remove this event from the list
                        self.logger.debug(
                            "Successfully uploaded %s to %s.",
                            self.filename,
                            event["gid"],
                        )
                        event["upload_success"] = True
                        self.send_p_astro(event, pastro)
                else:
                    self.logger.debug(
                        "Failed to upload %s to %s.", self.filename, event["gid"]
                    )
                    event["upload_attempts"] += 1

    def load_xmlobj(self, xmlobj):
        if isinstance(xmlobj, str):
            xmlobj = BytesIO(xmlobj.encode("utf-8"))
        return ligolw_utils.load_fileobj(xmlobj, contenthandler=LIGOLWContentHandler)

    def load_model(self, filename):
        model = pastro.load(filename)
        model.finalize(model.prior())
        return model

    def add_rankstat_to_model(self):
        # update the pastro model with ranking stat information
        # from the current analysis in the path provided by
        # self.rank_stat. Note: this will fail if the analysis
        # hasnt burned in yet, since there wont be a ranking stat
        # pdf file yet. Hence the try/except with OSError
        try:
            self.model.update_rankstatpdf(self.rank_stat)
            self.model.finalize(self.model.prior())

            tmp, tmp_path = tempfile.mkstemp(
                ".h5", dir=os.getenv("_CONDOR_SCRATCH_DIR", tempfile.gettempdir())
            )
            os.close(tmp)

            self.logger.debug("Writing model to %s...", tmp_path)
            self.model.to_h5(tmp_path)

            self.logger.debug("Moving %s to %s...", tmp_path, self.pastro_model_file)
            shutil.move(tmp_path, self.pastro_model_file)

            self.last_rankstat_update = utils.gps_now()

        except (OSError, ValueError):
            self.logger.exception("Failed to update model with ranking stat info")

        return

    def calculate_pastro(self, event):
        pa = None
        if not event["snr_optimized"]:
            data = self.parse_data_from_coinc(event["coinc"])
            if data:
                pa = self.model(data, inj=self.is_injection_job)
        # for snr optimized coincs from manifold, just copy the pastro
        # from the corresponding non-optimized gstlal coinc
        else:
            this_time = event["time"]
            for e in self.events:
                if e["time"] == this_time and not e["snr_optimized"]:
                    pa = e["pastro"]
        return pa

    def parse_data_from_coinc(self, coinc):
        try:
            mchirp = lsctables.CoincInspiralTable.get_table(coinc).getColumnByName(
                "mchirp"
            )[0]
            snr = lsctables.CoincInspiralTable.get_table(coinc).getColumnByName("snr")[
                0
            ]
            likelihood = lsctables.CoincTable.get_table(coinc).getColumnByName(
                "likelihood"
            )[0]
            template_id = lsctables.SnglInspiralTable.get_table(coinc).getColumnByName(
                "Gamma0"
            )[0]
        except Exception:
            self.logger.exception("Failed to parse coinc file.")

        else:
            return {
                "mchirp": mchirp,
                "likelihood": likelihood,
                "template_id": template_id,
                "snr": snr,
            }

    def upload_file(self, graceid, message, filename, contents, tag, retries=3):
        for attempt in range(retries):
            try:
                # try to upload the file
                response = self.client.writeLog(
                    graceid,
                    message,
                    filename=filename,
                    filecontents=contents,
                    tagname=tag,
                )
            except HTTPError:
                # if the upload fails with an HTTPError, print it out
                self.logger.exception("except HTTPError")
            else:
                # if the upload does not fail check that the
                # response is what we want and leave this function
                if response.status == httplib.CREATED:
                    return response

            # if this upload attempt failed print a message, sleep for
            # 1 second before trying again
            self.logger.info(
                "upload of %s for %s failed on attempt %d / %d",
                filename,
                graceid,
                attempt,
                retries,
            )
            time.sleep(1.0)

        else:
            # if all uploads failed, return False
            self.logger.warning("upload of %s for %s failed.", filename, graceid)
            return False

    def send_p_astro(self, event, pastro):
        """
        send p(astro) via Kafka
        """
        p_astro = {"time": event["time"], "p_astro": pastro}
        self.producer.produce(topic=self.p_astro_topic, value=json.dumps(p_astro))
        # FIXME: the above is copied from gstlal, but should it actually be p_astro?
        self.producer.poll(0)


# -------------------------------------------------
#                     Main
# -------------------------------------------------


def main():
    # parse arguments
    options = parse_command_line()

    # set up logging
    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s | ll_inspiral_pastro_uploader : %(levelname)s : %(message)s"
    )
    logger = logging.getLogger("ll_inspiral_pastro_uploader")
    logger.setLevel(log_level)

    # create event uploader instance
    pastro_uploader = PAstroUploader(
        gracedb_service_url=options.gracedb_service_url,
        input_topic=options.input_topic,
        kafka_server=options.kafka_server,
        logger=logger,
        model_name=options.model_name,
        pastro_model_file=options.pastro_model_file,
        rank_stat=options.rank_stat,
        num_messages=options.num_messages,
        pastro_filename=options.pastro_filename,
        processing_cadence=options.processing_cadence,
        request_timeout=options.request_timeout,
        tag=options.tag,
    )

    # start up
    pastro_uploader.start()
