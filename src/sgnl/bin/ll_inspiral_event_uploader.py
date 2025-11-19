"""an executable to aggregate and upload GraceDB events from sgnl-inspiral jobs"""

# Copyright (C) 2019  Patrick Godwin
# Copyright (C) 2024  Yun-Jing Huang

__usage__ = "sgnl-ll-inspiral-event-uploader [--options]"
__author__ = (
    "Patrick Godwin (patrick.godwin@ligo.org), Yun-Jing Huang (yun-jing.huang@ligo.org)"
)

# -------------------------------------------------
#                   Preamble
# -------------------------------------------------

import http.client as httplib
import json
import logging
import math
import time
from argparse import ArgumentParser
from collections import OrderedDict, deque
from io import BytesIO

import numpy
import yaml
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.param import use_in as param_use_in
from igwn_segments import segment
from lal import LIGOTimeGPS
from ligo.gracedb.rest import DEFAULT_SERVICE_URL as DEFAULT_GRACEDB_URL
from ligo.gracedb.rest import GraceDb, HTTPError
from ligo.scald import utils
from ligo.scald.io import influx

from sgnl import events
from sgnl.gracedb import FakeGracedbClient


@array_use_in
@param_use_in
@lsctables.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
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
        "--num-jobs", type=int, default=10000, help="number of jobs to listen to"
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
        "--upload-cadence-type",
        metavar="string",
        default="geometric",
        help="Choose the method [geometric|linear] in which the cadence of subsequent "
        "uploads are done. Default = geometric.",
    )
    parser.add_argument(
        "--upload-cadence-factor",
        type=float,
        default=4,
        help="Cadence factor T for sending out subsequent events for the same event "
        "window. For geometric cadence, first event gets sent T seconds later, second "
        "event gets sent T^2 seconds later, etc. For linear cadence, subsequent events "
        "get sent T seconds later. Default = 4.0.",
    )
    parser.add_argument(
        "--selection-criteria",
        type=str,
        default="MAXSNR",
        help="Choose method for determining favored event. Select one of the following:"
        " 1. MAXSNR - upload highest SNR candidate *below* public alert threshold "
        "(default). 2. MINFAR - upload lowest FAR candidate. 3. COMPOSITE - upload "
        "composite event by assigning the minimum FAR to the highest SNR candidate.",
    )
    parser.add_argument(
        "--far-threshold",
        type=float,
        default=3.84e-07,
        help="FAR threshold considered for an event to be public, not including a "
        "trials factor. Default = 1 / month",
    )
    parser.add_argument(
        "--far-trials-factor",
        type=int,
        default=1,
        help="Trials factor for number of CBC pipelines uploading events to GraceDB. "
        "Default = 1.",
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
        "--input-topic", metavar="string", help="Sets the input kafka topic. Required."
    )
    parser.add_argument(
        "--gracedb-group",
        metavar="name",
        default="Test",
        help="Gracedb group to which to upload events (default is Test).",
    )
    parser.add_argument(
        "--gracedb-pipeline",
        metavar="name",
        default="SGNL",
        help="Name of pipeline to provide in GracedB uploads (default is SGNL).",
    )
    parser.add_argument(
        "--gracedb-search",
        metavar="name",
        default="LowMass",
        help="Name of search to provide in GracedB uploads (default is LowMass).",
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
        "--scald-config",
        metavar="path",
        help="sets ligo-scald options based on yaml configuration.",
    )
    parser.add_argument(
        "--max-partitions",
        type=int,
        default=10,
        help="Sets the max number of partitions for the output topic. Used for the "
        "different SNR optimizer jobs.",
    )

    options = parser.parse_args()

    return options


# -------------------------------------------------
#                    Classes
# -------------------------------------------------


class EventUploader(events.EventProcessor):
    """
    manages handling of incoming events, selecting the best and uploading to GraceDB.
    """

    _name = "event_uploader"

    def __init__(
        self,
        input_topic,
        kafka_server,
        logger,
        scald_config,
        far_threshold: float = 3.84e-07,
        far_trials_factor: int = 1,
        gracedb_group: str = "Test",
        gracedb_pipeline: str = "SGNL",
        gracedb_search: str = "LowMass",
        gracedb_service_url: str = DEFAULT_GRACEDB_URL,
        max_event_time: int = 7200,
        max_partitions: int = 10,
        num_jobs: int = 10000,
        processing_cadence: float = 0.1,
        request_timeout: float = 0.2,
        selection_criteria: str = "MAXSNR",
        tag: str = "test",
        upload_cadence_factor: float = 4,
        upload_cadence_type: str = "geometric",
    ):
        self.logger = logger
        self.logger.info("setting up event uploader...")

        self.is_injection_job = input_topic == "inj_events"
        topic_prefix = "" if not self.is_injection_job else "inj_"
        heartbeat_topic = f"sgnl.{tag}.{topic_prefix}event_uploader_heartbeat"
        self.favored_event_topic = f"sgnl.{tag}.{topic_prefix}favored_events"
        self.upload_topic = f"sgnl.{tag}.{topic_prefix}uploads"

        # set up output topics. Note that the uploads topic is special,
        # we use it for the SNR optimizer. We want to divide the work
        # among multiple consumers so we set the number of partitions to
        # 10. This is calculated based on the number of expected g-events
        # within a 5 minute window. FIXME: check this number
        output_topics = [self.favored_event_topic, self.upload_topic]
        self.max_partitions = max_partitions
        num_partitions = [1, self.max_partitions]

        events.EventProcessor.__init__(
            self,
            process_cadence=processing_cadence,
            request_timeout=request_timeout,
            num_messages=num_jobs,
            kafka_server=kafka_server,
            input_topic=f"sgnl.{tag}.{input_topic}",
            output_topic=output_topics,
            topic_partitions=num_partitions,
            tag=tag,
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

        # gracedb settings
        self.gracedb_group = gracedb_group
        self.gracedb_pipeline = gracedb_pipeline
        self.gracedb_search = gracedb_search

        # upload cadence settings
        self.upload_cadence_type = upload_cadence_type
        self.upload_cadence_factor = upload_cadence_factor

        # initialize event store
        self.events: OrderedDict = OrderedDict()

        # favored event settings
        if selection_criteria == "MAXSNR":
            self.favored_function = self.select_maxsnr_candidate
        elif selection_criteria == "MINFAR":
            self.favored_function = self.select_minfar_candidate
        else:
            self.favored_function = self.construct_composite_candidate

        self.public_far_threshold = far_threshold / far_trials_factor

        # heartbeat settings
        self.last_inspiral_heartbeat = 0.0
        self.heartbeat_write = utils.gps_now()

        # upload topic settings
        # keep track of the partition that we send messages to
        # so that we can iterate across all partitions evenly
        # start with 0, iterate up to self.max_partitions - 1, and repeat
        self.partition_key = 0

        # set up aggregator sink
        with open(scald_config, "r") as f:
            agg_config = yaml.safe_load(f)
        self.agg_sink = influx.Aggregator(**agg_config["backends"]["default"])

        # register measurement schemas for aggregators
        self.agg_sink.load(path=scald_config)

    def ingest(self, message):
        """
        parse a message containing a candidate event
        """
        # process heartbeat messages from inspiral jobs
        if message.key() and "heartbeat" == message.key().decode("UTF-8"):
            heartbeat = json.loads(message.value())
            if heartbeat["time"] > self.last_inspiral_heartbeat:
                self.last_inspiral_heartbeat = heartbeat["time"]

        # process candidate event
        else:
            candidate = json.loads(message.value())
            candidate["time"] = LIGOTimeGPS(candidate["time"], candidate.pop("time_ns"))
            candidate.update(self.trigger_info(candidate))
            self.process_candidate(candidate)

    def process_candidate(self, candidate):
        """
        handles the processing of a candidate, creating
        a new event if necessary
        """
        key = self.event_window(candidate["time"])
        if key in self.events:
            self.logger.info("adding new candidate for event: [%.1f, %.1f]", *key)
            self.events[key]["candidates"].append(candidate)
            self.update_trigger_history(key, candidate)
        else:
            new_event = True
            for seg, event in self.events.items():
                if segment(candidate["time"], candidate["time"]) in seg:
                    self.logger.info(
                        "adding new candidate for time window: [%.1f, %.1f]", *seg
                    )
                    event["candidates"].append(candidate)
                    self.update_trigger_history(seg, candidate)
                    new_event = False

            # event not found, create a new event
            if new_event:
                self.logger.info("found new event: [%.1f, %.1f]", *key)
                self.events[key] = self.new_event()
                self.events[key]["candidates"].append(candidate)
                self.update_trigger_history(key, candidate)

    def update_trigger_history(self, key, candidate):
        """
        update trigger history dict for each candidate
        """
        self.events[key]["trigger_history"].update(candidate["trigger_info"])

    def trigger_info(self, candidate):
        """
        gather trigger information for each candidate,
        used for rtpe
        """
        # parse candidate for SVD bin, masses, LR, and SNR
        svdbin, type = self.parse_job_tag(candidate["job_tag"])

        coinc = self.load_xmlobj(candidate["coinc"])
        coinc_row = lsctables.CoincTable.get_table(coinc)[0]
        snglinspiral_row = lsctables.SnglInspiralTable.get_table(coinc)[0]

        return {
            "trigger_info": {
                svdbin: {
                    "snr": candidate["snr"],
                    "likelihood": coinc_row.likelihood,
                    "mass1": snglinspiral_row.mass1,
                    "mass2": snglinspiral_row.mass2,
                    "spin1z": snglinspiral_row.spin1z,
                    "spin2z": snglinspiral_row.spin2z,
                    "Gamma0": snglinspiral_row.Gamma0,
                }
            }
        }

    def parse_job_tag(self, tag):
        """
        get svd bin and job type from job tag
        """
        svdbin = tag.split("_")[0]
        name = "_".join(tag.split("_")[1:])
        return svdbin, name

    def load_xmlobj(self, xmlobj):
        """
        returns the coinc xml object from the kafka message
        """
        if isinstance(xmlobj, str):
            xmlobj = BytesIO(xmlobj.encode("utf-8"))
        return ligolw_utils.load_fileobj(xmlobj, contenthandler=LIGOLWContentHandler)

    def event_window(self, t):
        """
        returns the event window representing the event
        """
        dt = 0.2
        return segment(utils.floor_div(t - dt, 0.5), utils.floor_div(t + dt, 0.5) + 0.5)

    def new_event(self):
        """
        returns the structure that defines an event
        """
        return {
            "num_sent": 0,
            "time_sent": None,
            "favored": None,
            "gid": None,
            "candidates": deque(maxlen=self.num_messages),
            "trigger_history": {},
        }

    def handle(self):
        """
        handle events stored, selecting the best candidate.
        upload if a new favored event is found
        """
        for key, event in sorted(self.events.items(), reverse=True):
            if (
                (event["num_sent"] == 0 and len(event["candidates"]) > 0)
                or event["candidates"]
                and (
                    (utils.gps_now() >= self.next_event_upload(event))
                    or (
                        event["favored"]["far"] > self.public_far_threshold
                        and any(
                            [
                                candidate["far"] <= self.public_far_threshold
                                for candidate in event["candidates"]
                            ]
                        )
                    )
                )
            ):
                self.logger.info(
                    "handle: process_event num %d [%.1f, %.1f]",
                    len(event["candidates"]),
                    *key,
                )
                self.process_event(event, key)

        # clean out old events
        current_time = utils.gps_now()
        for key in list(self.events.keys()):
            if current_time - key[0] >= self.max_event_time:
                if self.events[key]["gid"]:
                    self.logger.info(
                        "sending final trigger history for event [%.1f, %.1f]", *key
                    )
                    self.upload_file(
                        "Trigger history file for RTPE",
                        "trigger_history.json",
                        "trigger_history",
                        json.dumps(self.events[key]["trigger_history"]),
                        self.events[key]["gid"],
                    )
                self.logger.info("removing stale event [%.1f, %.1f]", *key)
                self.events.pop(key)

        # has it been more than 15 minutes since last inspiral heartbeat
        # jobs should send heartbeats every 10 minutes
        now = utils.gps_now()
        if now - self.heartbeat_write > 15 * 60:
            state = 0 if now - self.last_inspiral_heartbeat > 15 * 60 else 1
            data = {
                "heartbeat": {
                    "heartbeat_tag": {"time": [int(now)], "fields": {"data": [state]}}
                }
            }

            self.logger.debug("Storing heartbeat state %d to influx...", state)
            self.agg_sink.store_columns("heartbeat", data["heartbeat"], aggregate=None)
            self.heartbeat_write = now

    def process_event(self, event, window):
        """
        handle a single event, selecting the best candidate.
        upload if a new favored event is found
        """
        updated, event = self.process_candidates(event)
        if event["num_sent"] == 0:
            assert updated
        if updated:
            self.logger.info(
                "uploading %s candidate with FAR = %.3E, "
                "SNR = %2.1f for event: [%.1f, %.1f]",
                self.to_ordinal(event["num_sent"] + 1),
                event["favored"]["far"],
                event["favored"]["snr"],
                window[0],
                window[1],
            )
            gid = self.upload_event(event)
            self.send_favored_event(event, window)
            if gid:
                event["num_sent"] += 1
                event["gid"] = gid
                self.send_uploaded(event, gid)

    def process_candidates(self, event):
        """
        process candidates and update the favored (maxsnr) event
        if needed

        returns event and whether the favored (maxsnr) event was updated
        """
        updated, this_favored = self.favored_function(
            event["candidates"], event["favored"]
        )

        if updated:
            event["favored"] = this_favored

        event["candidates"].clear()

        return updated, event

    def construct_composite_candidate(self, candidates, favored=None):
        """
        Construct composite event. Replace far and likelihood
        in maxsnr candidate with those in the minfar candidate.
        """

        # add previous favored event to list so we can get
        # the overall min FAR and max SNR instead of just
        # over the new candidates
        if favored:
            candidates.append(favored)

        maxsnr_candidate = max(candidates, key=self.rank_snr)
        maxsnr = maxsnr_candidate["snr"]

        minfar_candidate = min(candidates, key=self.rank_far)
        minfar = minfar_candidate["far"]

        # if neither the FAR nor SNR have improved compared to
        # the previous favored, no update. Otherwise, make
        # composite event and send an update
        if favored and maxsnr <= favored["snr"] and minfar >= favored["far"]:
            return False, favored
        else:
            # construct composite event
            if maxsnr_candidate["far"] != minfar:
                self.logger.info(
                    "construct new composite event with FAR: %.3E, " "SNR: %2.3f",
                    minfar,
                    maxsnr_candidate["snr"],
                )

                # replace far
                maxsnr_candidate["far"] = minfar

                # load coinc file
                maxsnr_coinc_row, maxsnr_coinc_file = self.get_coinc_row(
                    maxsnr_candidate
                )
                minfar_coinc_row, minfar_coinc_file = self.get_coinc_row(
                    minfar_candidate
                )

                # update likelihood and far
                maxsnr_coinc_row.likelihood = minfar_coinc_row.likelihood
                maxsnr_coinc_row.combined_far = minfar

                # save coinc file
                coinc_obj = BytesIO()
                ligolw_utils.write_fileobj(maxsnr_coinc_file, coinc_obj)
                maxsnr_candidate["coinc"] = coinc_obj.getvalue().decode("utf-8")

            if favored:
                assert (
                    maxsnr_candidate["far"] <= favored["far"]
                ), "composite event FAR should be smaller than previous favored FAR"
                assert (
                    maxsnr_candidate["snr"] >= favored["snr"]
                ), "composite event SNR should be larger than previous favored SNR"

            return True, maxsnr_candidate

    def select_maxsnr_candidate(self, candidates, favored=None):
        """
        select max snr candidate from candidates below
        public alert threshold:
        """
        # select the best candidate
        new_favored = max(candidates, key=self.rank_candidate)
        if not favored:
            return True, new_favored
        elif self.rank_candidate(new_favored) > self.rank_candidate(favored):
            return True, new_favored
        else:
            return False, favored

    def select_minfar_candidate(self, candidates, favored=None):
        """
        select the min far candidate out of the candidates
        """
        minfar_candidate = min(candidates, key=self.rank_far)
        if favored and minfar_candidate["far"] >= favored["far"]:
            return False, minfar_candidate
        else:
            return True, minfar_candidate

    def rank_candidate(self, candidate):
        """
        rank a candidate based on the following criterion:
        * FAR >  public threshold, choose lowest FAR
        * FAR <= public threshold, choose highest SNR
        """
        if candidate["far"] <= self.public_far_threshold:
            return True, candidate["snr"], 1.0 / candidate["far"]
        else:
            return False, 1.0 / candidate["far"], candidate["snr"]

    @staticmethod
    def rank_snr(candidate):
        return candidate["snr"]

    @staticmethod
    def rank_far(candidate):
        return candidate["far"]

    def send_favored_event(self, event, event_window):
        """
        send a favored event via Kafka
        """
        favored_event = {
            "event_window": list(event_window),
            "time": event["favored"]["time"].gpsSeconds,
            "time_ns": event["favored"]["time"].gpsNanoSeconds,
            "snr": event["favored"]["snr"],
            "far": event["favored"]["far"],
            "coinc": event["favored"]["coinc"],
        }
        self.producer.produce(
            topic=self.favored_event_topic, value=json.dumps(favored_event)
        )
        self.producer.poll(0)

    def send_uploaded(self, event, gid):
        """
        send an uploaded event via Kafka
        """
        uploaded = {
            "gid": gid,
            "time": event["favored"]["time"].gpsSeconds,
            "time_ns": event["favored"]["time"].gpsNanoSeconds,
            "snr": event["favored"]["snr"],
            "far": event["favored"]["far"],
            "coinc": event["favored"]["coinc"],
            "is_injection": self.is_injection_job,
            "snr_optimized": (
                True
                if "snr_optimized" in event["favored"]
                and event["favored"]["snr_optimized"]
                else False
            ),  # prevents re-triggering of the optimizer
        }

        # iterate the key so the next message goes to a different partition
        if self.partition_key >= self.max_partitions:
            self.partition_key = 0
        self.producer.produce(
            topic=self.upload_topic,
            value=json.dumps(uploaded),
            partition=self.partition_key,
        )
        self.partition_key += 1
        self.producer.poll(0)

    def upload_event(self, event):
        """
        upload a new event + auxiliary files
        """
        # upload event
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.client.createEvent(
                    group=self.gracedb_group,
                    pipeline=self.gracedb_pipeline,
                    filename="coinc.xml",
                    filecontents=event["favored"]["coinc"],
                    search=self.gracedb_search,
                    offline=False,
                    labels=(
                        "SNR_OPTIMIZED"
                        if "apply_snr_optimized_label" in event["favored"]
                        and event["favored"]["apply_snr_optimized_label"]
                        else None
                    ),
                    # don't apply the SNR_OPTIMIZED label for skymap optimizer events
                )
            except HTTPError:
                self.logger.exception("upload_event:HTTPError")
            except Exception:
                self.logger.exception("upload_event:Exception")
            else:
                resp_json = resp.json()
                if resp.status == httplib.CREATED:
                    graceid = resp_json["graceid"]
                    self.logger.info("event assigned grace ID %s", graceid)
                    if not event["time_sent"]:
                        event["time_sent"] = utils.gps_now()
                    break
            self.logger.warning(
                "gracedb upload of %s " "failed on attempt %d/%d",
                "coinc.xml",
                attempt,
                self.retries,
            )
            time.sleep(numpy.random.lognormal(math.log(self.retry_delay), 0.5))
        else:
            self.logger.warning("gracedb upload of %s failed", "coinc.xml")
            return None

        self.upload_file(
            "Trigger history file for RTPE",
            "trigger_history.json",
            "trigger_history",
            json.dumps(event["trigger_history"]),
            graceid,
        )

        return graceid

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
                self.logger.exception("upload_file:HTTPError")
            else:
                if resp.status == httplib.CREATED:
                    break
            self.logger.warning(
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

    def next_event_upload(self, event):
        """
        check whether enough time has elapsed to send an updated event
        """
        if self.upload_cadence_type == "geometric":
            return event["time_sent"] + numpy.power(
                self.upload_cadence_factor, event["num_sent"]
            )
        elif self.upload_cadence_type == "linear":
            return event["time_sent"] + self.upload_cadence_factor * event["num_sent"]

    def finish(self):
        """
        send remaining events before shutting down
        """
        for key, event in sorted(self.events.items(), reverse=True):
            if event["candidates"]:
                self.process_event(event, key)

    @staticmethod
    def to_ordinal(n):
        """
        given an integer, returns the ordinal number
        representation.

        this black magic is taken from
        https://stackoverflow.com/a/20007730
        """
        return "%d%s" % (n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10 :: 4])

    def get_coinc_row(self, event):
        coinc_file = self.load_xmlobj(event["coinc"])
        return lsctables.CoincTable.get_table(coinc_file)[0], coinc_file


# -------------------------------------------------
#                     Main
# -------------------------------------------------


def main():
    # parse arguments
    options = parse_command_line()

    # check input topic
    if options.input_topic not in ("events", "inj_events"):
        raise Exception("Input topic should be either events or inj_events.")

    # check favored method
    if options.selection_criteria not in ("MAXSNR", "MINFAR", "COMPOSITE"):
        raise ValueError(
            "Favored event method must be either MAXSNR, MINFAR, or COMPOSITE."
        )

    # set up logging
    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s | ll-inspiral-event-uploader : %(levelname)s : %(message)s"
    )
    logger = logging.getLogger("ll-inspiral-event-uploader")
    logger.setLevel(log_level)

    # create event uploader instance
    event_uploader = EventUploader(
        input_topic=options.input_topic,
        kafka_server=options.kafka_server,
        logger=logger,
        scald_config=options.scald_config,
        far_threshold=options.far_threshold,
        far_trials_factor=options.far_trials_factor,
        gracedb_group=options.gracedb_group,
        gracedb_pipeline=options.gracedb_pipeline,
        gracedb_search=options.gracedb_search,
        gracedb_service_url=options.gracedb_service_url,
        max_event_time=options.max_event_time,
        num_jobs=options.num_jobs,
        processing_cadence=options.processing_cadence,
        request_timeout=options.request_timeout,
        selection_criteria=options.selection_criteria,
        tag=options.tag,
        upload_cadence_factor=options.upload_cadence_factor,
        upload_cadence_type=options.upload_cadence_type,
    )

    # start up
    event_uploader.start()
