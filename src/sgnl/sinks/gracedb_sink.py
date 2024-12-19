from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
import time
import os
import io
import json

from sgn.control import HTTPControlSinkElement
from sgn.sinks import SinkElement

from ligo.gracedb.rest import GraceDb
from confluent_kafka import Producer

from lal import LIGOTimeGPS
from ligo.lw import utils as ligolw_utils
from ligo.lw.utils import process as ligolw_process
from ligo.lw import ligolw
from ligo.lw import table
from ligo.lw import lsctables
from ligo.lw import array
from ligo.lw import param

@dataclass
class GraceDBSink(HTTPControlSinkElement):
    """A sink that will turn the lowest FAR event into a gracedb upload, if it passes the threshold"""

    far_thresh: float = 1e-6
    output_kafka_server: str = None
    gracedb_server: str = "http://127.0.0.1:5000/api/"
    template_sngls: list = None
    analysis_tag: str = "mockery"
    job_tag: str = "mockingly"
    on_ifos: list = None
    process_params: dict = None

    def __post_init__(self):
        assert len(self.sink_pad_names) == 1
        super().__post_init__()
        self.events = None
        self.state = {"far-threshold": self.far_thresh}
        self.sngls_dict = {}
        for sub in self.template_sngls:
            for tid, sngl in sub.items():
                self.sngls_dict[tid] = sngl

        assert not (
            self.output_kafka_server is not None and self.gracedb_server is not None
        )

        self.start_time = time.time()

        # set up kafka producer or a gracedb client
        if self.output_kafka_server is not None:
            self.client = Producer(
                {
                    "bootstrap.servers": self.output_kafka_server,
                    "message.max.bytes": 20971520,
                }
            )
        elif self.gracedb_server is not None:
            self.client = GraceDb(self.gracedb_server)
        else:
            self.client = None

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)
        self.events = frame

    def internal(self):
        HTTPControlSinkElement.exchange_state(self.name, self.state)

        # send event data to kafka or gracedb
        if self.client:

            event, trigs = min_far_event(
                self.events["event"].data,
                self.events["trigger"].data,
                self.state["far-threshold"],
            )
            if event is not None:
                xmldoc = event_trigs_to_coinc_xmldoc(
                    event, trigs, self.sngls_dict, self.on_ifos, self.process_params
                )

                if self.output_kafka_server:
                    publish_kafka(self.client, xmldoc, self.job_tag, self.analysis_tag)
                else:
                    publish_gracedb(self.client, xmldoc)

                self.start_time = time.time()


def publish_kafka(client, xmldoc, job_tag, analysis_tag):
    """Send the xmldoc to kafka, where it will eventually be uploaded to gracedb"""
    coinc_table_row = lsctables.CoincTable.get_table(xmldoc)[0]
    coinc_inspiral_row = lsctables.CoincInspiralTable.get_table(xmldoc)[0]
    sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(xmldoc)

    message = io.BytesIO()
    ligolw_utils.write_fileobj(xmldoc, message)

    topic_prefix = "" if "noninj" in job_tag else "inj_"
    client.produce(
        topic=f"gstlal.{analysis_tag}.{topic_prefix}events",
        value=json.dumps(
            {
                "far": coinc_inspiral_row.combined_far,
                "snr": coinc_inspiral_row.snr,
                "time": coinc_inspiral_row.end_time,
                "time_ns": coinc_inspiral_row.end_time_ns,
                "coinc": message.getvalue().decode("utf-8"),
                "job_tag": job_tag,
            }
        ),
        key=job_tag,
    )


def publish_gracedb(client, xmldoc):
    """Send the xml doc to gracedb directly"""
    message = io.BytesIO()
    ligolw_utils.write_fileobj(xmldoc, message)
    resp = client.createEvent(
        group="CBC",
        pipeline="SGN",
        filename="coinc.xml",
        search="MOCK",
        labels="MOCK",
        offline=False,
        filecontents=message.getvalue(),
    )
    print("Created Event:", resp.json())


def min_far_event(events, triggers, thresh):
    if not events:
        return None, None
    min_far, min_ix = min((e["far"], n) for n, e in enumerate(events))
    if min_far <= thresh:
        return events[min_ix], [t for t in triggers[min_ix] if t is not None]
    return None, None


#
# Below is code required to make a correctly formatted LIGO_LW XML Document
# FIXME It is likely to be slower than we want and might still have bugs.
#

@lsctables.use_in
@array.use_in
@param.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


def add_table_with_n_rows(xmldoc, tblcls, n):
    """Given an xmldoc tree and a table class, add the table with n rows
    initialized to 0 for numeric types and "" for string types"""
    table = lsctables.New(tblcls)
    for i in range(n):
        row = table.RowType()
        for col, _type in table.validcolumns.items():
            col = col.split(":")[-1]
            setattr(row, col, "" if _type == "lstring" else 0)
        table.append(row)
    xmldoc.childNodes[-1].appendChild(table)
    return table


def col_map(row, datadict, mapdict, funcdict):
    """A function to map between SGN columns and ligolw columns. datadict has
    what the data you want to insert into the ligolw "row".  mapdict allows you to
    map between different names (if needed) or exclude data by setting the value to
    None.  funcdict lets you call simple functions to e.g., convert the type"""
    for col, value in datadict.items():
        oldcol = col
        if col in mapdict:
            col = mapdict[col]
            if col is None:
                continue
        if oldcol in funcdict:
            value = funcdict[oldcol](value)

        setattr(row, col, value)


def ns_to_gps(ns):
    # Cast ns GPS to LIGOTimeGPS - we are using the lal verison and that doesn't automatically do this? :(
    return LIGOTimeGPS(int(ns // 1_000_000_000), int(ns % 1_000_000_000))


def sngl_map(row, sngldict, _filter_id, exclude):
    for col in lsctables.SnglInspiralTable.validcolumns:
        col = col.split(":")[-1]
        if col not in exclude:
            setattr(row, col, getattr(sngldict[_filter_id], col))


def event_trigs_to_coinc_xmldoc(event, trigs, sngls_dict, on_ifos, process_params):
    """Given an event dict and a trigs list of dicts corresponding to single
    event as well as the parameters in the sngls_dict corresponding, construct an
    xml doc suitable for upload to gracedb"""

    # Initialize the main document
    xmldoc = ligolw.Document()
    xmldoc.appendChild(ligolw.LIGO_LW())
    ligolw_process.register_to_xmldoc(xmldoc, "sngl-inspiral", process_params)

    process = ligolw_process.register_to_xmldoc(xmldoc, "sgnl-inspiral", process_params)

    # Add tables that depend on the number of triggers
    # NOTE: tables are initilize to 0 or null values depending on the type.
    time_slide_table = add_table_with_n_rows(
        xmldoc, lsctables.TimeSlideTable, len(trigs)
    )
    sngl_inspiral_table = add_table_with_n_rows(
        xmldoc, lsctables.SnglInspiralTable, len(trigs)
    )
    coinc_map_table = add_table_with_n_rows(xmldoc, lsctables.CoincMapTable, len(trigs))
    found_ifos = []
    for n, row in enumerate(sngl_inspiral_table):
        row.event_id = n
        sngl_map(row, sngls_dict, trigs[n]["_filter_id"], exclude=())
        col_map(
            row,
            trigs[n],
            {
                "_filter_id": "template_id",
                "time": "end",
                "phase": "coa_phase",
                "epoch_start": None,
                "epoch_end": None,
            },
            {"time": ns_to_gps},
        )
        time_slide_table[n].instrument = row.ifo
        coinc_map_table[n].event_id = row.event_id
        coinc_map_table[n].table_name = "sngl_inspiral"
        found_ifos.append(row.ifo)

    # The remaining tables will all have ids set to integer zeros which
    # actually makes this single event document just magically have the right
    # relationships
    coinc_inspiral_table = add_table_with_n_rows(
        xmldoc, lsctables.CoincInspiralTable, 1
    )
    col_map(
        coinc_inspiral_table[0],
        event,
        {
            "time": "end",
            "network_snr": "snr",
            "far": "combined_far",
            "likelihood": None,
        },
        {"time": ns_to_gps},
    )
    coinc_inspiral_table[0].ifos = ",".join(sorted(found_ifos))

    coinc_event_table = add_table_with_n_rows(xmldoc, lsctables.CoincTable, 1)
    col_map(
        coinc_event_table[0],
        event,
        {
            "time": None,
            "network_snr": None,
            "far": None,
        },
        {},
    )
    coinc_event_table[0].instruments = ",".join(sorted(on_ifos))

    coinc_def_table = add_table_with_n_rows(xmldoc, lsctables.CoincDefTable, 1)

    return xmldoc


