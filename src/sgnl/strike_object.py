"""A class that contains all the strike related input/output files needed for
strike elements
"""

# Copyright (C) 2025 Yun-Jing Huang

import os
import shutil
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ligo.lw import utils as ligolw_utils
from strike.stats import far
from strike.stats.far import RankingStatPDF
from strike.stats.likelihood_ratio import LnLikelihoodRatio


@dataclass
class StrikeObject:
    """A class that contains all the strike related input/output files needed for
    strike elements

    Examples:
        offline:
            noninj:
                output files:
                    --output-likelihood-file
                        {IFOS}_{BANKID}_SGNL_LIKELIHOOD_RATIO-{START}-{DURATION}.xml.gz
        online:
            noninj:
                input files:
                    --input-likelihood-file:
                        e.g., {IFOS}_{BANKID}_SGNL_LIKELIHOOD_RATIO-0-0.xml.gz,
                        (will be updated, first one comes from
                        `sgnl-create-prior-diststats` in setup stage)
                    --rank-stat-pdf-file:
                        e.g., {IFOS}_SGNL_RANK_STAT_PDFS-0-0.xml.gz (read only, comes
                        from `sgnl-ll-marginalize-likelihoods_online`)
                    --zerolag-rank-stat-pdf:
                        e.g., {IFOS}_{BANKID}_SGNL_ZEROLAG_RANK_STAT_PDFS-0-0.xml.gz
                        (will be updated, first one created in setup stage)
                output files:
                    --output-likelihood-file:
                        e.g., {IFOS}_{BANKID}_SGNL_LIKELIHOOD_RATIO-0-0.xml.gz,
                        (same as --input-likelihood-file, will be copied from snapshots)
                    --zerolag-rank-stat-pdf-file:
                        e.g., {IFOS}_{BANKID}_SGNL_ZEROLAG_RANK_STAT_PDFS-0-0.xml.gz
                        (will be copied from the snapshot)
                    snapshots:
                        - trigger files
                        - segments
                        - likelihood files
                        - zerolagpdf files
                bottle outputs:
                    - likelihood files
                    - zerolagpdf files
            inj:
                input files:
                    --input-likelihood-file:
                        e.g., {IFOS}_{BANKID}_SGNL_LIKELIHOOD_RATIO-0-0.xml.gz,
                        (read only, same as the noninj one)
                    --rank-stat-pdf-file:
                        e.g., {IFOS}_SGNL_RANK_STAT_PDFS-0-0.xml.gz (read only)
                output files:
                    snapshots:
                        - trigger files
                        - segments
    """

    all_template_ids: Sequence[Any] = None
    bankids_map: dict[str, list] = None
    cap_singles: bool = False
    coincidence_threshold: float = None
    FAR_trialsfactor: float = 1
    ifos: list = None
    injections: bool = False
    input_likelihood_file: list[str] = None
    is_online: bool = False
    output_likelihood_file: list[str] = None
    rank_stat_pdf_file: str = None
    verbose: bool = False
    zerolag_rank_stat_pdf_file: list[str] = None

    def __post_init__(self):
        self._validate()

        # No time slides at the moment
        self.offset_vectors = {ifo: 0 for ifo in self.ifos}

        # Init LnLikelihoodRatio and RankingStatPDF instances
        self.likelihood_ratios = {}
        if self.is_online is False:
            # offline mode
            if self.output_likelihood_file is not None:
                for bankid, ids in self.bankids_map.items():
                    # offline mode, so initialize a LnLikelihoodRatio instance
                    bank_template_ids = self.all_template_ids[ids]
                    bank_template_ids = tuple(
                        bank_template_ids[bank_template_ids != -1]
                    )

                    # Ranking stat output
                    self.likelihood_ratios[bankid] = LnLikelihoodRatio(
                        template_ids=bank_template_ids,
                        instruments=self.ifos,
                        delta_t=self.coincidence_threshold,
                    )
        else:
            # load input files
            for bankid, lr_file in self.input_likelihood_file.items():
                self.likelihood_ratios[bankid] = LnLikelihoodRatio.load(lr_file)
            self.ln_lr_from_triggers = {}
            self.likelihood_ratio_uploads = {}
            self.update_assign_lr()

            self.rank_stat_pdf = None
            self.fapfar = None
            self.load_rank_stat_pdf()

            if self.injections is False:
                # noninj jobs
                # load zerolagpdfs
                self.zerolag_rank_stat_pdfs = {
                    bid: RankingStatPDF.load(zerolag_pdf_file)
                    for bid, zerolag_pdf_file in self.zerolag_rank_stat_pdf_file.items()
                }

    def _validate(self):
        if self.is_online is False:
            # offline mode
            if self.injections is True:
                # inj
                if self.output_likelihood_file is not None:
                    raise ValueError(
                        "Must not set --output-likelihood-file when --injections is set"
                    )
            else:
                # noninj
                # if --output-likelihood-file is not set, likelihood file won't be
                # created
                pass
        else:
            if self.injections is True:
                if self.input_likelihood_file is None:
                    raise ValueError(
                        "Must specify --input-likelihood-file when running"
                        " online injection job"
                    )
                if self.output_likelihood_file is not None:
                    raise ValueError(
                        "Must not specify --output-likelihood-file when "
                        " running online injection job"
                    )
                if self.rank_stat_pdf_file is None:
                    raise ValueError(
                        "Must specify --rank-stat-pdf-file when running "
                        " online injection job"
                    )
                if self.zerolag_rank_stat_pdf_file is not None:
                    raise ValueError(
                        "Must not specify --zerolag-rank-stat-pdf-file when "
                        " running online injection job"
                    )
            else:
                if self.input_likelihood_file is None:
                    raise ValueError(
                        "Must specify --input-likelihood-file when running"
                        " online noninj job"
                    )
                else:
                    self.input_likelihood_file = {
                        k: self.input_likelihood_file[i]
                        for i, k in enumerate(self.bankids_map)
                    }

                if self.output_likelihood_file is None:
                    raise ValueError(
                        "Must specify --output-likelihood-file when "
                        " running online noninj job"
                    )
                else:
                    self.output_likelihood_file = {
                        k: self.output_likelihood_file[i]
                        for i, k in enumerate(self.bankids_map)
                    }

                for infile, outfile in zip(
                    self.input_likelihood_file.values(),
                    self.output_likelihood_file.values(),
                ):
                    if infile != outfile:
                        raise ValueError(
                            "--input-likelihood-file must be the same as "
                            "--output-likelihood-file"
                        )
                if self.rank_stat_pdf_file is None:
                    raise ValueError(
                        "Must specify --rank-stat-pdf-file when running "
                        " online noninj job"
                    )
                if self.zerolag_rank_stat_pdf_file is None:
                    raise ValueError(
                        "Must specify --zerolag-rank-stat-pdf-file when "
                        " running online noninj job"
                    )
                else:
                    self.zerolag_rank_stat_pdf_file = {
                        k: self.zerolag_rank_stat_pdf_file[i]
                        for i, k in enumerate(self.bankids_map)
                    }

    def update_assign_lr(self):

        for bankid, in_lr_file in self.input_likelihood_file.items():
            lr = self.likelihood_ratios[bankid]

            # re-load likelihood ratio file for injection jobs
            if (
                in_lr_file is not None
                and in_lr_file != self.output_likelihood_file[bankid]
            ):
                params_before = (
                    lr.template_ids,
                    lr.instruments,
                    lr.min_instruments,
                    lr.delta_t,
                )
                for tries in range(10):
                    try:
                        lr = LnLikelihoodRatio.load(in_lr_file)
                    except OSError as e:
                        print(
                            f"Error in reading rank stat on try {tries}: {e}",
                            file=sys.stderr,
                        )
                        time.sleep(1)
                    else:
                        break
                else:
                    raise RuntimeError("Exceeded retries, exiting.")

                if params_before != (
                    lr.template_ids,
                    lr.instruments,
                    lr.min_instruments,
                    lr.delta_t,
                ):
                    raise ValueError(
                        "'%s' contains incompatible ranking statistic configuration"
                        % self.input_likelihood_file
                    )
                else:
                    self.likelihood_ratios[bankid] = lr

            if lr.is_healthy(self.verbose):
                # FIXME FIXME is this correct? this is following gstlal
                # self.ln_lr_from_triggers[bankid] = lr.copy(frankenstein=True).finish()

                _frank = lr.copy(frankenstein=True)
                # FIXME: finish does not return self
                _frank.finish()
                self.ln_lr_from_triggers[bankid] = _frank.ln_lr_from_triggers

                # FIXME gstlal calls another frankenstein, do we need another copy?
                self.likelihood_ratio_uploads[bankid] = lr.copy(frankenstein=True)
                if self.verbose:
                    print("likelihood ratio assignment ENABLED", file=sys.stderr)
            else:
                self.ln_lr_from_triggers[bankid] = None
                self.likelihood_ratio_uploads[bankid] = None
                if self.verbose:
                    print("likelihood ratio assignment DISABLED", file=sys.stderr)

    def load_rank_stat_pdf(self):
        if os.access(
            ligolw_utils.local_path_from_url(self.rank_stat_pdf_file), os.R_OK
        ):
            rspdf = RankingStatPDF.load(self.rank_stat_pdf_file)
            self.rank_stat_pdf = rspdf

            for bankid in self.bankids_map:
                if (
                    not self.likelihood_ratios[bankid].template_ids
                    <= rspdf.template_ids
                ):
                    raise ValueError("wrong templates")

            if rspdf.is_healthy(self.verbose):
                self.fapfar = far.FAPFAR(rspdf.new_with_extinction())
                if self.verbose:
                    print(
                        "false-alarm probability and rate assignment ENABLED",
                        file=sys.stderr,
                    )
            else:
                self.fapfar = None
                if self.verbose:
                    print(
                        "false-alarm probability and rate assignment DISABLED",
                        file=sys.stderr,
                    )
        else:
            # If we are unable to load the file, disable FAR assignment
            # FIXME: should we make it fail?
            self.rank_stat_pdf = None
            self.fapfar = None
            if self.verbose:
                print(
                    "cannot load file, false-alarm probability and rate assignment "
                    "DISABLED",
                    file=sys.stderr,
                )

    def save_snapshot(self, snapshot_filenames):
        for bankid, fn in zip(self.bankids_map, snapshot_filenames()):
            # write snapshot files to disk
            self.likelihood_ratios[bankid].save(fn)
            zfn = fn.replace("LIKELIHOOD_RATIO", "ZEROLAG_RANK_STAT_PDFS")
            self.zerolag_rank_stat_pdfs[bankid].save(zfn)

            # copy snapshot to output path
            shutil.copy(
                fn,
                ligolw_utils.local_path_from_url(self.output_likelihood_file[bankid]),
            )
            shutil.copy(
                zfn,
                ligolw_utils.local_path_from_url(
                    self.zerolag_rank_stat_pdf_file[bankid]
                ),
            )
