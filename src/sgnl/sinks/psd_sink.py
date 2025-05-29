"""An element to write out PSDs to xml files."""

# Copyright (C) 2024 Anushka Doke, Ryan Magee, Shio Sakon

from dataclasses import dataclass

import lal
from igwn_ligolw import utils as ligolw_utils
from sgn.base import Frame, SinkElement, SinkPad


def write_psd(fname, psddict, verbose=True, trap_signals=None):
    ligolw_utils.write_filename(
        lal.series.make_psd_xmldoc(psddict),
        fname,
        verbose=verbose,
        trap_signals=trap_signals,
    )


@dataclass
class PSDSink(SinkElement):
    """
    A sink element that dumps a PSD to an LIGOLW XML file
    """

    fname: str = None

    def __post_init__(self):
        super().__post_init__()
        self.psd = {}

    def pull(self, pad: SinkPad, frame: Frame) -> None:
        # FIXME: we actually just need the last non-gap psd
        # FIXME: put psd in EventFrame
        psd = frame.metadata["psd"]
        if psd is not None:
            self.psd[self.rsnks[pad]] = psd

        if frame.EOS:
            self.mark_eos(pad)

    def internal(self) -> None:
        if self.at_eos:
            write_psd(self.fname, self.psd)
