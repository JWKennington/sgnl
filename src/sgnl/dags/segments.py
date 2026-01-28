# Copyright (C) 2010-2020  Kipp Cannon, Patrick Godwin, Chad Hanna, Ryan Magee
# Copyright (C) 2024-2025  Yun-Jing Huang, Cort Posnansky


from __future__ import annotations

import itertools
import math
import os
import ssl
import urllib.request
import warnings
from typing import Iterable, Mapping, Union

import dqsegdb2.query
import igwn_segments as segments
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.param import use_in as param_use_in
from igwn_ligolw.utils import segments as ligolw_segments
from igwn_segments import utils as segutils
from lal import LIGOTimeGPS

DEFAULT_DQSEGDB_SERVER = os.environ.get(
    "DEFAULT_SEGMENT_SERVER", "https://segments.ligo.org"
)


@array_use_in
@param_use_in
@lsctables.use_in
class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


def query_dqsegdb_segments(
    instruments: Union[str, Iterable],
    start: Union[int, LIGOTimeGPS],
    end: Union[int, LIGOTimeGPS],
    flags: Union[str, Mapping],
    server: str = DEFAULT_DQSEGDB_SERVER,
) -> segments.segmentlistdict:
    """Query DQSegDB for science segments.

    Args:
            instruments:
                    Union[str, Iterable], the instruments to query segments for
            start:
                    Union[int, LIGOTimeGPS], the GPS start time
            end:
                    Union[int, LIGOTimeGPS], the GPS end time
            flags:
                    Union[str, Mapping], the name of the DQ flags used to query
            server:
                    str, defaults to main DQSegDB server, the server URL

    Returns:
            segmentlistdict, the queried segments

    """
    span = segments.segment(LIGOTimeGPS(start), LIGOTimeGPS(end))
    if isinstance(flags, str):
        if not isinstance(instruments, str):
            raise ValueError(
                "if flags is type str, then instruments must also be type str"
            )
        flags = {instruments: flags}
    if isinstance(instruments, str):
        instruments = [instruments]

    segs = segments.segmentlistdict()
    for ifo, flag in flags.items():
        active = dqsegdb2.query.query_segments(
            flag, start, end, host=server, coalesce=True
        )["active"]
        segs[ifo] = segments.segmentlist([span]) & active

    segs = filter_short_segments(segs)

    return segs


def query_dqsegdb_veto_segments(
    instruments: Union[str, Iterable],
    start: Union[int, LIGOTimeGPS],
    end: Union[int, LIGOTimeGPS],
    veto_definer_file: str,
    category: str,
    cumulative: bool = True,
    server: str = DEFAULT_DQSEGDB_SERVER,
) -> segments.segmentlistdict:
    """Query DQSegDB for veto segments.

    Args:
            instruments:
                    Union[str, Iterable], the instruments to query segments for
            start:
                    Union[int, LIGOTimeGPS], the GPS start time
            end:
                    Union[int, LIGOTimeGPS], the GPS end time
            veto_definer_file:
                    str, the veto definer file in which to query veto segments for
            category:
                    the veto category to use for vetoes, one of CAT1, CAT2, CAT3
            cumulative:
                    whether veto categories are cumulative, e.g. choosing CAT2
                    also includes CAT1 vetoes
            server:
                    str, defaults to main DQSegDB server, the server URL

    Returns:
            segmentlistdict, the queried veto segments

    """
    if isinstance(instruments, str):
        instruments = [instruments]

    if category not in set(("CAT1", "CAT2", "CAT3")):
        raise ValueError("not valid category")

    # read in vetoes
    xmldoc = ligolw_utils.load_filename(
        veto_definer_file, contenthandler=LIGOLWContentHandler
    )
    vetoes = lsctables.VetoDefTable.get_table(xmldoc)

    # filter vetoes by instruments
    vetoes[:] = [v for v in vetoes if v.ifo in set(instruments)]

    # filter vetoes by category
    cat_level = int(category[-1])
    if cumulative:
        vetoes[:] = [v for v in vetoes if v.category <= cat_level]
    else:
        vetoes[:] = [v for v in vetoes if v.category == cat_level]

    # retrieve segments corresponding to flags
    segs = segments.segmentlistdict()
    for instrument in instruments:
        segs[instrument] = segments.segmentlist()
    for veto in vetoes:
        flag = f"{veto.ifo}:{veto.name}:{veto.version}"
        segs[veto.ifo] |= dqsegdb2.query.query_segments(
            flag, start, end, host=server, coalesce=True
        )["active"]
    segs.coalesce()

    return segs


def query_gwosc_segments(
    instruments: Union[str, Iterable],
    start: Union[int, LIGOTimeGPS],
    end: Union[int, LIGOTimeGPS],
    verify_certs: bool = True,
) -> segments.segmentlistdict:
    """Query GWOSC for science segments.

    Args:
            instruments:
                    Union[str, Iterable], the instruments to query segments for
            start:
                    Union[int, LIGOTimeGPS], the GPS start time
            end:
                    Union[int, LIGOTimeGPS], the GPS end time
            verify_certs:
                    bool, default True, whether to verify SSL certificates when querying
                    GWOSC.

    Returns:
            segmentlistdict, the queried segments

    """
    if isinstance(instruments, str):
        instruments = [instruments[i : i + 2] for i in range(0, len(instruments), 2)]

    # Set up SSL context
    context = ssl.create_default_context()
    if not verify_certs:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    # Retrieve segments
    segs = segments.segmentlistdict()
    for instrument in instruments:
        url = _gwosc_segment_url(start, end, f"{instrument}_DATA")
        urldata = urllib.request.urlopen(url, context=context).read().decode("utf-8")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            segs[instrument] = segutils.fromsegwizard(
                urldata.splitlines(),
                coltype=lsctables.LIGOTimeGPS,
            )
    segs.coalesce()

    segs = filter_short_segments(segs)

    segs.coalesce()

    return segs


def query_gwosc_veto_segments(
    instruments: Union[str, Iterable],
    start: Union[int, LIGOTimeGPS],
    end: Union[int, LIGOTimeGPS],
    category: str,
    cumulative: bool = True,
    verify_certs: bool = True,
) -> segments.segmentlistdict:
    """Query GWOSC for veto segments.

    Args:
            instruments:
                    Union[str, Iterable], the instruments to query segments for
            start:
                    Union[int, LIGOTimeGPS], the GPS start time
            end:
                    Union[int, LIGOTimeGPS], the GPS end time
            category:
                    the veto category to use for vetoes, one of CAT1, CAT2, CAT3
            cumulative:
                    whether veto categories are cumulative, e.g. choosing CAT2
                    also includes CAT1 vetoes
            verify_certs:
                    bool, default True, whether to verify SSL certificates when querying
                    GWOSC.

    Returns:
            segmentlistdict, the queried veto segments

    """
    span = segments.segment(LIGOTimeGPS(start), LIGOTimeGPS(end))
    if isinstance(instruments, str):
        instruments = [instruments]

    if category not in set(("CAT1", "CAT2", "CAT3")):
        raise ValueError("not valid category")

    if cumulative:
        flags = [f"CBC_CAT{i}" for i in range(1, int(category[-1]) + 1)]
    else:
        flags = [f"CBC_{category}"]

    # hardware injections not in normal categories in GWOSC
    # so we treat them all as CAT1 (except CW)
    hw_inj_flags = [
        "NO_BURST_HW_INJ",
        "NO_CBC_HW_INJ",
        "NO_DETCHAR_HW_INJ",
        "NO_STOCH_HW_INJ",
    ]
    flags += hw_inj_flags

    # set up SSL context
    context = ssl.create_default_context()
    if not verify_certs:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    # retrieve segments corresponding to flags
    segs = segments.segmentlistdict()
    for instrument in instruments:
        segs[instrument] = segments.segmentlist([span])
        for flag in flags:
            url = _gwosc_segment_url(start, end, f"{instrument}_{flag}")
            urldata = (
                urllib.request.urlopen(url, context=context).read().decode("utf-8")
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                segs[instrument] &= segutils.fromsegwizard(
                    urldata.splitlines(),
                    coltype=lsctables.LIGOTimeGPS,
                )
    segs.coalesce()

    # invert segments to transform into vetoes
    for instrument in instruments:
        segs[instrument] = segments.segmentlist([span]) & ~segs[instrument]

    return segs


def write_segments(
    seglistdict: segments.segmentlistdict,
    output: str = "segments.xml.gz",
    segment_name: str = "datasegments",
    process_name: str = "sgnl-write-segments",
    verbose: bool = False,
) -> None:
    """Save a segmentlistdict to disk in LIGO-LW XML format.

    Automatically infers metadata (start, end, instruments) from the segments.

    Args:
        seglistdict:
            The segmentlistdict to save.
        output:
            Output filename (e.g. "segments.xml.gz").
        segment_name:
            Name of the segment table in the XML file.
        process_name:
            Name to register as the process in the XML header.
        verbose:
            Print progress messages if True.
    """
    # parse save info from seg dict
    instruments = list(seglistdict.keys())
    all_segments = segments.segmentlist()

    for seglist in seglistdict.values():
        all_segments.extend(seglist)

    if not all_segments:
        raise ValueError("No segments found in seglistdict; nothing to save.")

    span = all_segments.extent()
    start, end = float(span[0]), float(span[1])

    # creat xml
    xmldoc = ligolw.Document()
    xmldoc.appendChild(ligolw.LIGO_LW())

    process_params = {
        "start": start,
        "end": end,
        "instruments": ",".join(instruments),
        "segment_name": segment_name,
        "output": output,
    }
    process = xmldoc.register_process(process_name, process_params)

    with ligolw_segments.LigolwSegments(xmldoc, process) as lwseglists:
        lwseglists.insert_from_segmentlistdict(seglistdict, segment_name)

    process.set_end_time_now()
    ligolw_utils.write_filename(xmldoc, output)


def load_segment_file(filename: str):
    """Load a segmentlistdict from a LIGO-LW XML file."""
    print(f"Loading segments from {filename}")
    xmldoc = ligolw_utils.load_filename(
        filename,
        contenthandler=ligolw_segments.LIGOLWContentHandler,
    )
    segs = ligolw_segments.segmenttable_get_by_name(
        xmldoc, "datasegments"
    ).coalesce()

    return segs



def analysis_segments(
    ifos: Iterable[str],
    allsegs: segments.segmentlistdict,
    boundary_seg: segments.segment,
    start_pad: float = 0.0,
    overlap: float = 0.0,
    min_instruments: int = 1,
    one_ifo_length: float = (3600 * 8.0),
) -> segments.segmentlistdict:
    """Generate all disjoint detector combination segments for analysis job
    boundaries.
    """
    ifos = set(ifos)
    segsdict = segments.segmentlistdict()

    # segment length dependent on the number of instruments
    # so that longest job runtimes are similar
    def _sl(n_ifo, one_ifo_length=one_ifo_length):
        return one_ifo_length / 2 ** (n_ifo - 1)

    segment_length = _sl

    # generate analysis segments
    for n in range(min_instruments, 1 + len(ifos)):
        for ifo_combos in itertools.combinations(list(ifos), n):
            ifo_key = frozenset(ifo_combos)
            segsdict[ifo_key] = allsegs.intersection(ifo_combos) - allsegs.union(
                ifos - set(ifo_combos)
            )
            segsdict[ifo_key] &= segments.segmentlist([boundary_seg])
            segsdict[ifo_key] = segsdict[ifo_key].protract(overlap)
            segsdict[ifo_key] &= segments.segmentlist([boundary_seg])
            segsdict[ifo_key] = split_segments(
                segsdict[ifo_key], segment_length(len(ifo_combos)), start_pad
            )
            if not segsdict[ifo_key]:
                del segsdict[ifo_key]

    return segsdict


def split_segments_by_lock(
    ifos: Iterable,
    seglistdicts: segments.segmentlistdict,
    boundary_seg: segments.segment,
    max_time: float = 10 * 24 * 3600.0,
) -> segments.segmentlist:
    """Split segments into segments with maximum time and boundaries outside of lock
    stretches.
    """
    ifos = set(seglistdicts)

    # create set of segments for each ifo when it was
    # in coincidence with at least one other ifo
    doublesegs = segments.segmentlistdict()
    for ifo1 in ifos:
        for ifo2 in ifos - set([ifo1]):
            if ifo1 in doublesegs:
                doublesegs[ifo1] |= seglistdicts.intersection((ifo1, ifo2))
            else:
                doublesegs[ifo1] = seglistdicts.intersection((ifo1, ifo2))

    # This is the set of segments when at least two ifos were on
    doublesegsunion = doublesegs.union(doublesegs.keys())

    # This is the set of segments when at least one ifo was on
    # segs = seglistdicts.union(seglistdicts.keys())

    # define when "enough time" has passed
    def enoughtime(seglist, start, end):
        return (
            abs(seglist & segments.segmentlist([segments.segment(start, end)]))
            > 0.7 * max_time
        )

    # iterate through all the segment where at least one ifo was on and extract
    # chunks where each ifo satisfies our coincidence requirement. A consequence is
    # that we only define boundaries when one ifos is on
    chunks = segments.segmentlist([boundary_seg])

    # This places boundaries when only one ifo or less was on
    for _, end in doublesegsunion:
        if all([enoughtime(s, chunks[-1][0], end) for s in doublesegs.values()]):
            chunks[-1] = segments.segment(chunks[-1][0], end)
            chunks.append(segments.segment(end, boundary_seg[1]))

    # check that last segment has enough livetime
    # if not, merge it with the previous segment
    if len(chunks) > 1 and abs(chunks[-1]) < 0.3 * max_time:
        last_chunk = chunks.pop()
        chunks[-1] = segments.segmentlist([chunks[-1], last_chunk]).coalesce().extent()

    return chunks


def split_segments(
    seglist: segments.segmentlist, maxextent: float, overlap: float
) -> segments.segmentlist:
    """Split a segmentlist into segments of maximum extent."""
    newseglist = segments.segmentlist()
    for bigseg in seglist:
        newseglist.extend(split_segment(bigseg, maxextent, overlap))
    return newseglist


def split_segment(
    seg: segments.segment, maxextent: float, overlap: float
) -> segments.segmentlist:
    """Split a segment into segments of maximum extent."""
    if maxextent <= 0:
        raise ValueError("maxextent must be positive, not %s" % repr(maxextent))

    # Simple case of only one segment
    if abs(seg) < maxextent:
        return segments.segmentlist([seg])

    # adjust maxextent so that segments are divided roughly equally
    maxextent = max(int(abs(seg) / (int(abs(seg)) // int(maxextent) + 1)), overlap)
    maxextent = int(math.ceil(abs(seg) / math.ceil(abs(seg) / maxextent)))
    end = seg[1]

    seglist = segments.segmentlist()

    while abs(seg):
        if (seg[0] + maxextent + overlap) < end:
            seglist.append(segments.segment(seg[0], seg[0] + maxextent + overlap))
            seg = segments.segment(seglist[-1][1] - overlap, seg[1])
        else:
            seglist.append(segments.segment(seg[0], end))
            break

    return seglist


def filter_short_segments(
    segs: segments.segmentlistdict,
    min_duration: float = 512,
) -> segments.segmentlistdict:
    """
    Filter a segmentlistdict by removing segments shorter than `min_duration`.

    Parameters
    ----------
    segs : segmentlistdict
        Dictionary mapping IFO → segmentlist.
    min_duration : float, default 512
        Minimum allowed segment duration in seconds.

    Returns
    -------
    segmentlistdict
        A new segmentlistdict containing only segments of sufficient length.
    """
    trimmed = segments.segmentlistdict()

    for ifo, seglist in segs.items():
        trimmed_segments = segments.segmentlist(
            [seg for seg in seglist if abs(seg) >= min_duration]
        )
        trimmed[ifo] = trimmed_segments

    return trimmed


def bound_segments(
    segs: segments.segmentlistdict,
    start: float | None = None,
    end: float | None = None,
) -> segments.segmentlistdict:
    """
    Restrict all segments in a ``segmentlistdict`` to a GPS time range.

    Parameters
    ----------
    segs : segmentlistdict
        Dictionary mapping IFO → segmentlist.
    start : float or None, optional
        GPS start time. If ``None``, no lower bound is applied.
    end : float or None, optional
        GPS end time. If ``None``, no upper bound is applied.

    Returns
    -------
    segmentlistdict
        A new dictionary where each IFO's segments have been intersected
        with the interval ``[start, end]``.
    """
    if start is None and end is None:
        return segs

    start = LIGOTimeGPS(start) if start is not None else segments.NegInfinity
    stop = LIGOTimeGPS(end) if end is not None else segments.PosInfinity

    boundaries = segments.segmentlist([segments.segment(start, stop)])

    bounded = segments.segmentlistdict()
    for ifo, seglist in segs.items():
        bounded[ifo] = seglist & boundaries

    return bounded


def contract_segments(
    segs: segments.segmentlistdict,
    trim: float,
) -> segments.segmentlistdict:
    """
    Contract each segment by removing ``trim`` seconds from both ends,
    optionally discarding segments too short to survive contraction.

    Parameters
    ----------
    segs : segmentlistdict
        Dictionary mapping IFO → segmentlist.
    trim : float
        Amount in seconds to contract from each segment edge. If ``trim <= 0``,
        the input is returned unchanged.

    Returns
    -------
    segmentlistdict
        A new dictionary of contracted segments. Segments shorter than
        ``2 * trim`` are discarded.
    """
    if trim <= 0:
        return segs

    safe = segments.segmentlistdict()

    for ifo, seglist in segs.items():
        safe[ifo] = segments.segmentlist(
            [seg for seg in seglist if abs(seg) > 2 * trim]
        )

    safe.contract(trim)
    return safe


def process_segments(
    segs: segments.segmentlistdict,
    gps_start: float | None = None,
    gps_end: float | None = None,
    min_length: float = 0,
    trim: float = 0,
) -> segments.segmentlistdict:
    """
    Apply bounding, minimum length filtering, and contraction to
    a ``segmentlistdict``.

    Parameters
    ----------
    segs : segmentlistdict
        Dictionary mapping IFO → segmentlist.
    gps_start : float or None
        Optional GPS start bound.
    gps_end : float or None
        Optional GPS end bound.
    min_length : float, default 0
        Minimum allowed segment duration in seconds.
    trim : float, default 0
        Contraction length in seconds.

    Returns
    -------
    segmentlistdict
        Processed segment dictionary after applying all requested operations.
    """
    segs = bound_segments(segs, gps_start, gps_end)

    if min_length > 0:
        segs = filter_short_segments(segs, min_length)

    if trim > 0:
        segs = contract_segments(segs, trim)

    return segs


def union_segmentlistdicts(
    a: segments.segmentlistdict,
    b: segments.segmentlistdict,
) -> segments.segmentlistdict:
    """
    Compute the union of two ``segmentlistdict`` objects.

    Parameters
    ----------
    a, b : segmentlistdict
        Dictionaries mapping IFO → segmentlist.

    Returns
    -------
    segmentlistdict
        Dictionary whose values are the unions of corresponding segmentlists.
        Missing keys are treated as empty segmentlists.
    """
    out = segments.segmentlistdict()

    all_keys = set(a.keys()) | set(b.keys())
    for key in all_keys:
        segs_a = a.get(key, segments.segmentlist())
        segs_b = b.get(key, segments.segmentlist())
        out[key] = (segs_a | segs_b).coalesce()

    return out


def intersection_segmentlistdicts(
    a: segments.segmentlistdict,
    b: segments.segmentlistdict,
) -> segments.segmentlistdict:
    """
    Compute the intersection of two ``segmentlistdict`` objects.

    Parameters
    ----------
    a, b : segmentlistdict
        Dictionaries mapping IFO → segmentlist.

    Returns
    -------
    segmentlistdict
        Dictionary whose keys are the IFOs present in both inputs, and
        whose values are the intersections of the corresponding segmentlists.
    """
    out = segments.segmentlistdict()

    common_keys = set(a.keys()) & set(b.keys())
    for key in common_keys:
        out[key] = (a[key] & b[key]).coalesce()

    return out


def diff_segmentlistdicts(
    a: segments.segmentlistdict,
    b: segments.segmentlistdict,
) -> segments.segmentlistdict:
    """
    Compute the difference between two ``segmentlistdict`` objects,
    defined as segments in ``a`` that are not in ``b``.

    Parameters
    ----------
    a : segmentlistdict
        The minuend (segments to keep).
    b : segmentlistdict
        The subtrahend (segments to remove). Missing keys are treated
        as empty segmentlists.

    Returns
    -------
    segmentlistdict
        Dictionary whose values are ``a[key] - b[key]`` for each key in ``a``.
    """
    out = segments.segmentlistdict()

    for key in a.keys():
        segs_a = a[key]
        segs_b = b.get(key, segments.segmentlist())
        out[key] = (segs_a - segs_b).coalesce()

    return out


def combine_segmentlistdicts(
    segdicts: list[segments.segmentlistdict],
    operation: str,
) -> segments.segmentlistdict:
    """
    Combine multiple ``segmentlistdict`` objects using a specified set
    operation.

    Parameters
    ----------
    segdicts : list of segmentlistdict
        Sequence of dictionaries to combine. Must contain at least two entries.
    operation : {"union", "intersection", "diff"}
        Set-like operation to apply cumulatively across the list.

    Returns
    -------
    segmentlistdict
        The cumulative result of applying the chosen operation.

    Raises
    ------
    ValueError
        If ``operation`` is invalid or fewer than two dictionaries are provided.
    """
    if operation not in {"union", "intersection", "diff"}:
        raise ValueError("operation must be 'union', 'intersection', or 'diff'")

    if len(segdicts) < 2:
        raise ValueError("Need at least two segmentlistdicts")

    result = segdicts[0]

    for other in segdicts[1:]:
        if operation == "union":
            result = union_segmentlistdicts(result, other)
        elif operation == "intersection":
            result = intersection_segmentlistdicts(result, other)
        elif operation == "diff":
            result = diff_segmentlistdicts(result, other)

    return result


def _gwosc_segment_url(start, end, flag):
    """Returns the GWOSC URL associated with segments."""
    span = segments.segment(LIGOTimeGPS(start), LIGOTimeGPS(end))

    # determine GWOSC URL to query from
    urlbase = "https://gwosc.org/timeline/segments/"
    if start in segments.segment(1126051217, 1137254417):
        query_url = f"{urlbase}/O1"
    elif start in segments.segment(1164556817, 1187733618):
        query_url = f"{urlbase}/O2_16KHZ_R1"
    elif start in segments.segment(1238166018, 1253977218):
        query_url = f"{urlbase}/O3a_16KHZ_R1"
    elif start in segments.segment(1256655618, 1269363618):
        query_url = f"{urlbase}/O3b_16KHZ_R1"
    elif start in segments.segment(1368975618, 1389456018):
        query_url = f"{urlbase}/O4a_16KHZ_R1"
    else:
        raise ValueError("GPS times requested not in GWOSC")

    return f"{query_url}/{flag}/{span[0]}/{abs(span)}"
