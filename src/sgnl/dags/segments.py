# Copyright (C) 2010--2020  Kipp Cannon, Patrick Godwin, Chad Hanna, Ryan Magee
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


from __future__ import annotations

import itertools
import math
import os
import ssl
import urllib.request
import warnings
from typing import Iterable, Mapping, Union

import dqsegdb2.query
from lal import LIGOTimeGPS
from ligo import segments
from ligo.lw import ligolw, lsctables
from ligo.lw import utils as ligolw_utils
from ligo.segments import utils as segutils

DEFAULT_DQSEGDB_SERVER = os.environ.get(
    "DEFAULT_SEGMENT_SERVER", "https://segments.ligo.org"
)


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
        instruments = [instruments]

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
    segment_length = lambda n_ifo: one_ifo_length / 2 ** (n_ifo - 1)

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


def _gwosc_segment_url(start, end, flag):
    """Returns the GWOSC URL associated with segments."""
    span = segments.segment(LIGOTimeGPS(start), LIGOTimeGPS(end))

    # determine GWOSC URL to query from
    urlbase = "https://gw-openscience.org/timeline/segments"
    if start in segments.segment(1126051217, 1137254417):
        query_url = f"{urlbase}/O1"
    elif start in segments.segment(1164556817, 1187733618):
        query_url = f"{urlbase}/O2_16KHZ_R1"
    elif start in segments.segment(1238166018, 1253977218):
        query_url = f"{urlbase}/O3a_16KHZ_R1"
    elif start in segments.segment(1256655618, 1269363618):
        query_url = f"{urlbase}/O3b_16KHZ_R1"
    else:
        raise ValueError("GPS times requested not in GWOSC")

    return f"{query_url}/{flag}/{span[0]}/{abs(span)}"
