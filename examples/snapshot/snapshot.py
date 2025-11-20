import os
from dataclasses import dataclass

import torch

os.environ["STRIKE_DATA_PATH"] = "/Users/crh184/.strike"
import time

from ligo import segments
from sgn.apps import Pipeline
from sgn.sinks import NullSink
from sgn.sources import SignalEOS, SourceElement
from sgn.subprocess import Parallelize
from sgnligo.base import now
from sgnts.base import EventBuffer, EventFrame

from sgnl.control import SnapShotControl
from sgnl.sinks import StrikeSink
from sgnl.strike_object import StrikeObject
from sgnl.transforms import StrikeTransform

# files are here /ligo/home/ligo.org/yun-jing.huang/phd/sgn/run/ram/test/copy
nbank = 1
dirname = "likelihood_ratio/"
input_likelihood_file = sorted(dirname + f for f in os.listdir(dirname))[:nbank]
output_likelihood_file = input_likelihood_file
rank_stat_pdf_file = "rank_stat_pdfs/H1L1V1-SGNL_RANK_STAT_PDFS-0-0.xml.gz"
dirname = "zerolag_rank_stat_pdfs/"
zerolag_rank_stat_pdf_file = sorted(dirname + f for f in os.listdir(dirname))[:nbank]
nsubbank = 2 * len(input_likelihood_file)
ifos = ["H1", "L1", "V1"]

SnapShotControl.snapshot_interval = 10
# SnapShotControl.delay = 0

bankids_map = {}
i0 = 0
for fn in input_likelihood_file:
    bankid = fn.split("-")[1].split("_")[0]
    bankids_map[bankid] = [i0, i0 + 1]
    i0 += 2

strike_object = StrikeObject(
    all_template_ids=[],  # type: ignore[arg-type]
    bankids_map=bankids_map,
    coincidence_threshold=0.005,
    cap_singles=False,
    chi2_over_snr2_min=0.0,
    chi2_over_snr2_max=1.0,
    chi_bin_min=0.0,
    chi_bin_max=1.0,
    compress_likelihood_ratio=True,
    compress_likelihood_ratio_threshold=0.03,
    ifos=ifos,
    injections=False,
    input_likelihood_file=input_likelihood_file,
    is_online=True,
    output_likelihood_file=output_likelihood_file,
    rank_stat_pdf_file=rank_stat_pdf_file,
    zerolag_rank_stat_pdf_file=zerolag_rank_stat_pdf_file,
)
nsubbank, ntemplate = 1, 100  # type: ignore[assignment]


@dataclass
class FakeItacacacSrc(SourceElement, SignalEOS):
    def new(self, pad):
        time.sleep(0.01)
        snrs_above_thresh = {}
        chisqs_above_thresh = {}
        masks = {}
        for ifo in ifos:
            snrs = torch.rand(nsubbank, ntemplate) * 10
            chisqs = torch.rand(nsubbank, ntemplate) * 2
            mask = snrs >= 4
            masks[ifo] = mask
            snrs_above_thresh[ifo] = snrs[mask]
            chisqs_above_thresh[ifo] = chisqs[mask]

        background = {
            "snrs": snrs_above_thresh,
            "chisqs": chisqs_above_thresh,
            "single_masks": masks,
        }
        ts = float(now())
        te = ts + 1
        tsns = int(ts * 1e9)
        tens = int(ts * 1e9)
        seg = segments.segment(ts, te)
        trigger_rates = {}
        # print(ts)
        for ifo in ifos:
            trigger_rates[ifo] = {}
            for bankid in bankids_map:
                trigger_rates[ifo][bankid] = (seg, 500)
        return EventFrame(
            events={
                "trigger_rates": EventBuffer(tsns, tens, data=[trigger_rates]),
                "background": EventBuffer(tsns, tens, data=[background]),
            },
            EOS=self.signaled_eos(),
        )


@dataclass
class FakeHorizon(SourceElement):
    def __post_init__(self):
        super().__post_init__()
        self.horizons = {"H1": 180, "L1": 200, "V1": 90}

    def new(self, pad):
        ts = float(now())
        te = ts + 1
        tsns = int(ts * 1e9)
        tens = int(ts * 1e9)
        ifo = self.rsrcs[pad]
        data = {"ifo": ifo, "horizon": self.horizons[ifo]}
        return EventFrame(events={"data": EventBuffer(tsns, tens, [data])})


pipeline = Pipeline()

fake_it = FakeItacacacSrc(
    name="FakeIt",
    source_pad_names=("background",),
)
strike_sink = StrikeSink(
    name="StrikeSink",
    ifos=ifos,
    is_online=True,
    injections=False,
    strike_object=strike_object,
    bankids_map=bankids_map,
    background_pad="background",
    horizon_pads=ifos,
)
fake_h = FakeHorizon(
    name="FakeH",
    source_pad_names=ifos,
)

pipeline.insert(
    fake_it,
    strike_sink,
    fake_h,
    link_map={
        "StrikeSink:snk:background": "FakeIt:src:background",
        "StrikeSink:snk:H1": "FakeH:src:H1",
        "StrikeSink:snk:L1": "FakeH:src:L1",
        "StrikeSink:snk:V1": "FakeH:src:V1",
    },
)

# True enables multiprocessing, false disables it
if True:
    with SnapShotControl() as control, Parallelize(pipeline) as parallelize:
        parallelize.run()
else:
    with SnapShotControl() as control:
        pipeline.run()
