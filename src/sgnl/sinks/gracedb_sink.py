import base64
import io
import json
import math
import os
import sys
import time
from dataclasses import dataclass

import lal
import numpy
from confluent_kafka import Producer
from lal import LIGOTimeGPS
from lal import series as lalseries
from ligo.gracedb.rest import GraceDb
from ligo.lw import array, ligolw, lsctables, param
from ligo.lw import utils as ligolw_utils
from ligo.lw.utils import process as ligolw_process
from ligo.lw.utils import segments as ligolw_segments
from sgn.control import HTTPControlSinkElement

from sgnl.strike_object import StrikeObject
from sgnl.transforms.itacacac import light_travel_time
from sgnl.viz import logo_data


@dataclass
class GraceDBSink(HTTPControlSinkElement):
    """A sink that will turn the lowest FAR event into a gracedb upload, if it passes
    the threshold
    """

    far_thresh: float = -1
    output_kafka_server: str = None
    gracedb_service_url: str = None
    gracedb_group: str = "Test"
    gracedb_pipeline: str = "SGNL"
    gracedb_search: str = "MOCK"
    gracedb_label: list[str] = None
    gracedb_cred_reload: bool = True
    gracedb_reload_buffer: int = 300
    template_sngls: list = None
    analysis_tag: str = "mockery"
    job_type: str = None
    analysis_ifos: list = None
    process_params: dict = None
    event_pad: str = None
    spectrum_pads: list[str] = None
    delta_t: float = 0.005
    strike_object: StrikeObject = None
    channel_dict: dict[str, str] = None
    autocorrelation_lengths: dict[str, int] = None

    def __post_init__(self):
        self.sink_pad_names = (self.event_pad,) + self.spectrum_pads
        super().__post_init__()
        self.events = None
        self.psds = {}
        self.state = {"far-threshold": self.far_thresh}
        self.sngls_dict = {}
        for sub in self.template_sngls:
            for tid, sngl in sub.items():
                self.sngls_dict[tid] = sngl

        assert not (
            self.output_kafka_server is not None
            and self.gracedb_service_url is not None
        )

        self.start_time = time.time()

        # set up kafka producer or a gracedb client
        if self.output_kafka_server is not None:
            print("GraceDBSink: sending events to kafka", flush=True, file=sys.stderr)
            self.client = Producer(
                {
                    "bootstrap.servers": self.output_kafka_server,
                    "message.max.bytes": 20971520,
                }
            )
        elif self.gracedb_service_url is not None:
            self.client = GraceDb(
                self.gracedb_service_url,
                reload_cred=self.gracedb_cred_reload,
                reload_buffer=self.gracedb_reload_buffer,
            )
        else:
            self.client = None

    def pull(self, pad, frame):
        if frame.EOS:
            self.mark_eos(pad)

        if self.rsnks[pad] == self.event_pad:
            self.events = frame
        else:
            if frame.metadata["psd"] is not None:
                self.psds[self.rsnks[pad]] = frame.metadata["psd"]

    def internal(self):
        HTTPControlSinkElement.exchange_state(self.name, self.state)

        # send event data to kafka or gracedb
        if self.client and self.state["far-threshold"] >= 0:

            event, trigs, snr_ts = min_far_event(
                self.events["event"].data,
                self.events["trigger"].data,
                self.events["snr_ts"].data,
                self.state["far-threshold"],
            )
            if event is not None:
                xmldoc = event_trigs_to_coinc_xmldoc(
                    event,
                    trigs,
                    snr_ts,
                    self.sngls_dict,
                    self.analysis_ifos,
                    self.process_params,
                    self.delta_t,
                    self.channel_dict,
                    self.autocorrelation_lengths,
                )

                # add psd frequeny series
                lal.series.make_psd_xmldoc(self.psds, xmldoc.childNodes[-1])

                if self.output_kafka_server:
                    publish_kafka(
                        self.client,
                        xmldoc,
                        self.job_type,
                        self.analysis_tag,
                        "".join(sorted(self.analysis_ifos)),
                        self.gracedb_group,
                        self.gracedb_search,
                        self.strike_object,
                    )
                else:
                    publish_gracedb(
                        self.client,
                        xmldoc,
                        self.gracedb_group,
                        self.gracedb_pipeline,
                        self.gracedb_search,
                        self.gracedb_label,
                    )

                # count tracker
                gracedb_times = [event["time"] / 1e9]
                self.strike_object.store_counts(gracedb_times)

                self.start_time = time.time()

        if self.at_eos and self.output_kafka_server is not None:
            print("shutdown: flush kafka client", flush=True, file=sys.stderr)
            self.client.flush()


def publish_kafka(
    client,
    xmldoc,
    job_type,
    analysis_tag,
    instruments,
    group,
    search,
    strike_object,
):
    """Send the xmldoc to kafka, where it will eventually be uploaded to gracedb"""

    # coinc_table_row = lsctables.CoincTable.get_table(xmldoc)[0]
    coinc_inspiral_row = lsctables.CoincInspiralTable.get_table(xmldoc)[0]
    sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(xmldoc)

    message = io.BytesIO()
    ligolw_utils.write_fileobj(xmldoc, message)

    background_bin = "%04d" % int(sngl_inspiral_table[0].Gamma1)
    job_tag = background_bin + "_" + job_type

    topic_prefix = "" if "noninj" in job_tag else "inj_"
    client.produce(
        topic=f"sgnl.{analysis_tag}.{topic_prefix}events",
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
    client.poll(0)

    description = "%s_%s_%s_%s_%s" % (
        "SGNL",
        job_tag,
        ("%.4g" % coinc_inspiral_row.mass).replace(".", "_").replace("-", "_"),
        group,
        search,
    )
    end_time = int(coinc_inspiral_row.end)

    #
    # make sure the directory where we will write the files to disk exists
    #

    gracedb_uploads_gps_dir = os.path.join("gracedb_uploads", str(end_time)[:5])
    if not os.path.exists(gracedb_uploads_gps_dir):
        # set exist_ok = True in case multiple jobs try
        # to create the directory at the same time;
        # this is more likely to happen with injection
        # jobs due to the high injection rate
        os.makedirs(gracedb_uploads_gps_dir, exist_ok=True)

    rankingstat_filename = os.path.join(
        gracedb_uploads_gps_dir,
        "%s-%s_RankingData-%d-%d.xml.gz" % (instruments, description, end_time, 1),
    )
    write_rankingstat_xmldoc_gracedb(
        strike_object.likelihood_ratio_uploads[background_bin],
        rankingstat_filename,
    )

    ligolw_utils.write_filename(
        xmldoc, rankingstat_filename.replace("_RankingData", "")
    )

    client.produce(
        topic=f"sgnl.{analysis_tag}.{topic_prefix}ranking_stat",
        value=json.dumps(
            {
                "ranking_data_path": os.path.realpath(rankingstat_filename),
                "time": coinc_inspiral_row.end_time,
                "time_ns": coinc_inspiral_row.end_time_ns,
                "coinc": message.getvalue().decode("utf-8"),
            }
        ),
    )
    client.poll(0)


def publish_gracedb(client, xmldoc, group, pipeline, search, labels):
    """Send the xml doc to gracedb directly"""
    message = io.BytesIO()
    ligolw_utils.write_fileobj(xmldoc, message)
    resp = client.createEvent(
        group=group,
        pipeline=pipeline,
        filename="coinc.xml",
        search=search,
        labels=labels,
        offline=False,
        filecontents=message.getvalue(),
    )
    print("Created Event:", resp.json())

    # Write a log for the created event
    graceid = resp.json()["graceid"]
    log = client.writeLog(
        object_id=graceid,
        message="This is a test log.",
        filename="sgn.png",
        filecontents=base64.b64decode(logo_data()),
        tag_name=["plot"],
        displayName=["plot"],
    )
    print("Written Log:", log)


def min_far_event(events, triggers, snr_ts, thresh):
    if not events:
        return None, None, None
    min_far, min_ix = min(
        (
            (e["combined_far"], n)
            for n, e in enumerate(events)
            if e["combined_far"] is not None
        ),
        default=(None, None),
    )
    if min_far is not None and min_far <= thresh:
        return (
            events[min_ix],
            [t for t in triggers[min_ix] if t is not None],
            snr_ts[min_ix],
        )
    return None, None, None


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
    for _ in range(n):
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
    # Cast ns GPS to LIGOTimeGPS - we are using the lal verison and that doesn't
    # automatically do this? :(
    return LIGOTimeGPS(int(ns // 1_000_000_000), int(ns % 1_000_000_000))


def sngl_map(row, sngldict, _filter_id, exclude):
    for col in lsctables.SnglInspiralTable.validcolumns:
        col = col.split(":")[-1]
        if col not in exclude:
            setattr(row, col, getattr(sngldict[_filter_id], col))


def event_trigs_to_coinc_xmldoc(
    event,
    trigs,
    snr_ts,
    sngls_dict,
    analysis_ifos,
    process_params,
    delta_t,
    channel_dict,
    autocorrelation_lengths,
):
    """Given an event dict and a trigs list of dicts corresponding to single
    event as well as the parameters in the sngls_dict corresponding, construct an
    xml doc suitable for upload to gracedb"""

    # Initialize the main document
    xmldoc = ligolw.Document()
    xmldoc.appendChild(ligolw.LIGO_LW())
    # FIXME bayestar needs the program name
    ligolw_process.register_to_xmldoc(
        xmldoc,
        "sgnl-inspiral",
        process_params,
        instruments=analysis_ifos,
        is_online=True,
    )

    # Add tables that depend on the number of triggers
    # NOTE: tables are initilize to 0 or null values depending on the type.
    time_slide_table = add_table_with_n_rows(
        xmldoc, lsctables.TimeSlideTable, len(analysis_ifos)
    )
    process_id = lsctables.ProcessTable.get_table(xmldoc)[0].process_id

    found_ifos = []

    # Add subthreshold trigger
    triggerless_ifos = set(snr_ts.keys()) - set([t["ifo"] for t in trigs])
    if triggerless_ifos:
        try:
            subthresh_trigs = []
            # FIXME: handle the case of multiple trigs, pick the highest network snr one
            for ifo in sorted(triggerless_ifos):
                coinc_segment = ligolw_segments.segments.segment(
                    ligolw_segments.segments.NegInfinity,
                    ligolw_segments.segments.PosInfinity,
                )
                for trig in trigs:
                    trigger_time = trig["time"] / 1_000_000_000
                    trigger_ifo = trig["ifo"]
                    filter_id = trig["_filter_id"]
                    coincidence_window = light_travel_time(ifo, trigger_ifo) + delta_t
                    coinc_segment &= ligolw_segments.segments.segment(
                        trigger_time - coincidence_window,
                        trigger_time + coincidence_window,
                    )
                    # half_autocorr_length = (snr_ts[trigger_ifo].data.length - 1) // 2
                    half_autocorr_length = (
                        autocorrelation_lengths[
                            "%04d" % int(sngls_dict[filter_id].Gamma1)
                        ]
                        - 1
                    ) // 2

                for trig in subthresh_trigs:
                    trigger_time = trig["time"] / 1_000_000_000
                    trigger_ifo = trig["ifo"]
                    coincidence_window = light_travel_time(ifo, trigger_ifo) + delta_t
                    coinc_segment &= ligolw_segments.segments.segment(
                        trigger_time - coincidence_window,
                        trigger_time + coincidence_window,
                    )

                snr_ts_this_ifo = snr_ts[ifo]
                snr_ts_this_ifo_array = snr_ts_this_ifo.data.data
                t0 = snr_ts_this_ifo.epoch
                dt = snr_ts_this_ifo.deltaT
                idx0 = int((coinc_segment[0] - t0) / dt)
                idxf = int(math.ceil((coinc_segment[1] - t0) / dt))
                length = snr_ts_this_ifo_array.shape[-1]

                if idx0 < 0 or idxf > length:
                    # FIXME: should we actually skip if we don't have enough samples?
                    print("warning: not enougth samples to find coincidence")
                    idx0 = max(0, idx0)
                    idxf = min(idxf, length)

                seq = abs(snr_ts_this_ifo_array[idx0:idxf])

                maxid = numpy.argmax(seq)

                maxsnr = abs(snr_ts_this_ifo_array[idx0 + maxid])
                phase = math.atan2(
                    snr_ts_this_ifo_array[idx0 + maxid].imag,
                    snr_ts_this_ifo_array[idx0 + maxid].real,
                )
                peakt = float(t0) + (idx0 + maxid) * dt

                deltaT = snr_ts_this_ifo.deltaT

                snip0 = idx0 + maxid - half_autocorr_length
                snipf = idx0 + maxid + half_autocorr_length + 1
                sniplength = snr_ts_this_ifo_array.shape[-1]

                if snip0 < 0 or snipf > sniplength:
                    # FIXME: in gstlal it says Bayestar needs at least 26.3ms
                    print(
                        ifo,
                        "not enough samples to produce snr snippet",
                        f"{snip0=} {snipf=} {sniplength=}",
                    )
                    continue

                snr_ts_snippet = snr_ts_this_ifo_array[snip0:snipf]
                if snr_ts_snippet.shape[-1] == 0:
                    print(
                        f"{ifo=} {idx0=} {idxf=} {maxid=} {half_autocorr_length=}",
                        f"{coinc_segment=} {trigger_time=} {coincidence_window=} {t0=}",
                        f"{dt=} {snr_ts_this_ifo_array.shape=}",
                        file=sys.stderr,
                    )
                    raise ValueError("no snr ts snippet")

                snr_ts_snippet_out = lal.CreateCOMPLEX8TimeSeries(
                    name="snr",
                    epoch=peakt - half_autocorr_length * deltaT,
                    f0=0.0,
                    deltaT=deltaT,
                    sampleUnits=lal.DimensionlessUnit,
                    length=snr_ts_snippet.shape[-1],
                )
                snr_ts_snippet_out.data.data = snr_ts_snippet
                snr_ts[ifo] = snr_ts_snippet_out

                subthresh_trigs.append(
                    {
                        "_filter_id": filter_id,
                        "ifo": ifo,
                        "time": int(peakt * 1e9),
                        "phase": phase,
                        "chisq": None,
                        "snr": float(maxsnr),
                    }
                )

            trigs.extend(subthresh_trigs)
            event["network_snr"] = sum(trig["snr"] ** 2 for trig in trigs) ** 0.5
            event["time"] = min(trig["time"] for trig in trigs)
        except Exception as e:
            print(f"Subthreshold search failed with error:\n{e}")

    for n, ifo in enumerate(analysis_ifos):
        time_slide_table[n].instrument = ifo
        time_slide_table[n].process_id = process_id

    sngl_inspiral_table = add_table_with_n_rows(
        xmldoc, lsctables.SnglInspiralTable, len(trigs)
    )
    coinc_map_table = add_table_with_n_rows(xmldoc, lsctables.CoincMapTable, len(trigs))
    coinc_def_table = add_table_with_n_rows(xmldoc, lsctables.CoincDefTable, 1)
    coinc_def_table[0].description = "sngl_inspiral<-->sngl_inspiral coincidences"
    coinc_def_table[0].search = "inspiral"
    for n, row in enumerate(sngl_inspiral_table):
        sngl_map(row, sngls_dict, trigs[n]["_filter_id"], exclude=())
        row.event_id = n
        row.channel = channel_dict[row.ifo]
        row.chisq_dof = 1
        row.eff_distance = float("nan")
        row.process_id = process_id
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
        coinc_map_table[n].event_id = row.event_id
        coinc_map_table[n].table_name = "sngl_inspiral"
        found_ifos.append(row.ifo)

        # Add snr time series
        ts = snr_ts[row.ifo]
        snr_time_series_element = lalseries.build_COMPLEX8TimeSeries(ts)
        snr_time_series_element.appendChild(
            param.Param.from_pyvalue("event_id", row.event_id)
        )
        xmldoc.childNodes[-1].appendChild(snr_time_series_element)

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
            "combined_far": "combined_far",
            "false_alarm_probability": "false_alarm_rate",
            "likelihood": None,
        },
        {"time": ns_to_gps},
    )
    coinc_inspiral_table[0].ifos = ",".join(sorted(found_ifos))

    sngl_row = sngls_dict[trigs[0]["_filter_id"]]
    mass = sngl_row.mass1 + sngl_row.mass2
    mchirp = sngl_row.mchirp

    coinc_inspiral_table[0].mass = mass
    coinc_inspiral_table[0].mchirp = mchirp

    coinc_event_table = add_table_with_n_rows(xmldoc, lsctables.CoincTable, 1)
    col_map(
        coinc_event_table[0],
        event,
        {
            "time": None,
            "network_snr": None,
            "combined_far": None,
        },
        {},
    )
    coinc_event_table[0].instruments = ",".join(sorted(found_ifos))
    coinc_event_table[0].process_id = process_id

    # coinc_def_table = add_table_with_n_rows(xmldoc, lsctables.CoincDefTable, 1)

    return xmldoc


def write_rankingstat_xmldoc_gracedb(rankingstat_upload, output_filename):
    rankingstat = rankingstat_upload.copy()
    try:
        endtime = rankingstat.terms["P_of_tref_Dh"].horizon_history.maxkey()
    except ValueError:
        # empty horizon history
        pass
    else:
        # keep the last hour of history
        endtime -= 3600.0 * 1
        for history in rankingstat.terms["P_of_tref_Dh"].horizon_history.values():
            del history[:endtime]
    rankingstat.save(output_filename)
