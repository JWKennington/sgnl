import math

from lal import LIGOTimeGPS
from ligo import segments
from ligo.lw import ligolw, lsctables
from ligo.lw import utils as ligolw_utils


class ContentHandler(ligolw.LIGOLWContentHandler):
    pass


lsctables.use_in(ContentHandler)


def sim_inspiral_to_segment_list(fname, pad=1, verbose=False):
    """Given an xml file create a segment list that marks the time of an injection
    with padding

    Args:
        fname:
            str, the xml file name
        pad:
            float, duration in seconds to pad the coalescence time when producing a
            segment, e.g., [tc-pad, tc+pad)
    """

    # initialization

    seglist = segments.segmentlist()

    # Parse the XML file

    xmldoc = ligolw_utils.load_filename(
        fname, contenthandler=ContentHandler, verbose=verbose
    )

    # extract the padded geocentric end times into segment lists

    for row in lsctables.SimInspiralTable.get_table(xmldoc):
        t = row.time_geocent
        seglist.append(
            segments.segment(
                LIGOTimeGPS(math.floor(t - pad)), LIGOTimeGPS(math.ceil(t + pad))
            )
        )

    # help the garbage collector

    xmldoc.unlink()

    return seglist.coalesce()
