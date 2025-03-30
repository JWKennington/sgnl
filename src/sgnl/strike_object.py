"""A class that contains all the strike related input/output files needed for
strike elements
"""

# Copyright (C) 2025 Yun-Jing Huang

import os
import shutil
import sys
import time
import zlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy
import torch
from ligo.lw import utils as ligolw_utils
from strike.stats import far, inspiral_extrinsics
from strike.stats.far import RankingStatPDF
from strike.stats.likelihood_ratio import LnLikelihoodRatio
from strike.utilities import data


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
    compress_likelihood_ratio: bool = False
    compress_likelihood_ratio_threshold: float = 0.03
    FAR_trialsfactor: float = 1
    ifos: list = None
    injections: bool = False
    input_likelihood_file: list[str] = None
    is_online: bool = False
    output_likelihood_file: list[str] = None
    rank_stat_pdf_file: str = None
    verbose: bool = False
    zerolag_rank_stat_pdf_file: list[str] = None
    nsubbank_pretend: bool = False
    dtype: torch.dtype = torch.float32
    device: str = "cpu"

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
            # load dtdphi file only once, and share among the banks
            # assumes the dtdphi file is the same across banks
            dtdphi_file = set(
                [
                    f.terms["P_of_dt_dphi"].dtdphi_file
                    for f in self.input_likelihood_file.values()
                ]
            )
            assert len(dtdphi_file) == 1
            self.load_dtdphi(next(iter(dtdphi_file)))

            # load input files
            for i, (bankid, lr_file) in enumerate(self.input_likelihood_file.items()):
                if not self.nsubbank_pretend or (self.nsubbank_pretend and i == 0):
                    # if we are in nsubbbank_pretend mode, only load the first lr
                    # file and copy the rest
                    lr_class = LnLikelihoodRatio.load(lr_file)
                self.likelihood_ratios[bankid] = lr_class
                if self.compress_likelihood_ratio:
                    self.likelihood_ratios[bankid].terms[
                        "P_of_tref_Dh"
                    ].horizon_history.compress(
                        threshold=self.compress_likelihood_ratio_threshold,
                        verbose=self.verbose,
                    )

            self.ln_lr_from_triggers = {}
            self.likelihood_ratio_uploads = {}
            self.update_assign_lr(reload_file=False)

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
            else:
                self.zerolag_rank_stat_pdfs = None

        #
        # SNR chisq histogram
        #
        if len(self.likelihood_ratios) > 0:
            self.template_ids_tensor = torch.tensor(
                self.all_template_ids, device=self.device
            )

            # assume binning is the same for all banks
            self.binning = (
                list(self.likelihood_ratios.values())[0]
                .terms["P_of_SNR_chisq"]
                .snr_chi_binning
            )
            self.bins = tuple(
                torch.tensor(_bin.boundaries, dtype=self.dtype, device=self.device)
                for _bin in self.binning
            )
            self.bankids_index_expand = torch.zeros_like(
                self.template_ids_tensor, device=self.device
            )
            for i, ids in enumerate(self.bankids_map.values()):
                self.bankids_index_expand[ids] = i

            self.snr_chisq_lnpdf_noise = {}
            for ifo in self.ifos:
                lnpdf = []
                for bankid in self.bankids_map:
                    lnpdf.append(
                        self.likelihood_ratios[bankid]
                        .terms["P_of_SNR_chisq"]
                        .snr_chisq_lnpdf_noise[f"{ifo}_snr_chi"]
                        .array
                    )
                lnpdf = numpy.stack(lnpdf)
                self.snr_chisq_lnpdf_noise[ifo] = (
                    torch.from_numpy(lnpdf).to(self.device).to(torch.float32)
                )

            #
            # counts_by_template_id
            #
            # Create an empty tensor as the counter. It has the same shape as the
            # template ids tensor. Each location in the counter corresponds to the
            # count for the template id at the same location in the template_ids_tensor
            self.counts_by_template_id_counter = torch.zeros_like(
                self.template_ids_tensor, device=self.device
            )

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
                if self.output_likelihood_file is not None:
                    self.output_likelihood_file = {
                        k: self.output_likelihood_file[i]
                        for i, k in enumerate(self.bankids_map)
                    }
        else:
            if self.injections is True:
                if self.input_likelihood_file is None:
                    raise ValueError(
                        "Must specify --input-likelihood-file when running"
                        " online injection job"
                    )
                else:
                    self.input_likelihood_file = {
                        k: self.input_likelihood_file[i]
                        for i, k in enumerate(self.bankids_map)
                    }
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

    def update_assign_lr(self, reload_file=True):

        for bankid, in_lr_file in self.input_likelihood_file.items():
            lr = self.likelihood_ratios[bankid]

            # re-load likelihood ratio file for injection jobs
            if (
                reload_file
                and in_lr_file is not None
                and (
                    self.output_likelihood_file is None
                    or in_lr_file != self.output_likelihood_file[bankid]
                )
            ):
                params_before = (
                    lr.template_ids,
                    lr.instruments,
                    lr.min_instruments,
                    lr.delta_t,
                )

                # FIXME combine this hack with frankenstein class
                trigger_rates_before = lr.terms["P_of_tref_Dh"].triggerrates
                horizon_history_before = lr.terms["P_of_tref_Dh"].horizon_history

                for tries in range(10):
                    try:
                        lr = LnLikelihoodRatio.load(in_lr_file)
                    except (OSError, EOFError, zlib.error) as e:
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
                    lr.terms["P_of_tref_Dh"].triggerrates = trigger_rates_before
                    lr.terms["P_of_tref_Dh"].horizon_history = horizon_history_before
                    self.likelihood_ratios[bankid] = lr

            if lr.is_healthy(self.verbose):
                # FIXME FIXME is this correct? this is following gstlal
                # self.ln_lr_from_triggers[bankid] = lr.copy(frankenstein=True).finish()

                _frank = lr.copy(frankenstein=True)
                # FIXME: finish does not return self
                _frank.terms["P_of_dt_dphi"].time_phase_snr = self.time_phase_snr
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

    def load_dtdphi(self, dtdphi_file=None):
        if dtdphi_file is not None:
            self.time_phase_snr = inspiral_extrinsics.TimePhaseSNR.from_hdf5(
                dtdphi_file, instruments=self.ifos
            )
        else:
            filename = os.path.join(data.get_data_root_path(), "inspiral_dtdphi_pdf.h5")
            self.time_phase_snr = inspiral_extrinsics.TimePhaseSNR.from_hdf5(
                filename, instruments=self.ifos
            )
            print(
                "dtdphi_file is not set, so loading the file in the build : %s"
                % filename
            )

    def save_snr_chi_lnpdf(self):
        for ifo in self.ifos:
            for i, bankid in enumerate(self.bankids_map.keys()):
                self.likelihood_ratios[bankid].terms[
                    "P_of_SNR_chisq"
                ].snr_chisq_lnpdf_noise[f"{ifo}_snr_chi"].array = (
                    self.snr_chisq_lnpdf_noise[ifo][i].to("cpu").numpy()
                )

    def save_counts_by_template_id(self):
        counts = self.counts_by_template_id_counter.to("cpu").numpy()
        for bankid, ids in self.bankids_map.items():
            counts_by_template_id = (
                self.likelihood_ratios[bankid]
                .terms["P_of_Template"]
                .counts_by_template_id
            )
            counts_this_bank = counts[ids].ravel()
            count_mask = counts_this_bank > 0
            counts_this_bank = counts_this_bank[count_mask]
            template_ids_this_bank = self.all_template_ids[ids].ravel()[count_mask]
            assert len(counts_this_bank) == len(template_ids_this_bank)
            for tid, count in zip(template_ids_this_bank, counts_this_bank):
                assert tid in counts_by_template_id
                counts_by_template_id[tid] += count

        # reset the counter
        self.counts_by_template_id_counter[:] = 0

    def time_key(self, time):
        size = 10  # granularity for tracking counts
        return int(time) - int(time) % size

    def train_noise(self, time, snrs, chisqs, single_masks):
        # FIXME should this be in strike? But this depends on torch

        time = self.time_key(time)
        for ifo, single_mask in single_masks.items():
            if True in single_mask:
                # There are singles above threshold

                #
                # counts_by_template_id
                #
                # single mask is of the shape (nsubbank, ntemp_in_subbank)
                # True values in single mask identify the template that has a count
                self.counts_by_template_id_counter += single_masks

                #
                # SNR-chisq histogram
                #
                snr = snrs[ifo]
                chisq = chisqs[ifo]
                chisq_over_snr2 = chisq / snr**2

                # determine which bankid the triggers come from
                trig_bankids = self.bankids_index_expand[single_mask]

                # torch bucketize returns the index of the right boundary
                # but numpy histogramdd returns the left
                snr_inds = torch.bucketize(snr, boundaries=self.bins[0]) - 1
                chisq_over_snr_inds = (
                    torch.bucketize(chisq_over_snr2, boundaries=self.bins[1]) - 1
                )
                self.snr_chisq_lnpdf_noise[ifo].index_put_(
                    (trig_bankids, snr_inds, chisq_over_snr_inds),
                    torch.tensor(1.0),
                    accumulate=True,
                )

                #
                # Count tracker
                #
                # max number of time keys after which old temp counts are deleted
                num_keys = 500
                ct_mask = (snr >= 6) & (chisq_over_snr2 <= 4e-2)
                if True in ct_mask:
                    ct_snr = snr[ct_mask]
                    ct_chisq_over_snr2 = chisq_over_snr2[ct_mask]
                    ct_trig_bankids = trig_bankids[ct_mask]
                    ct_snr_inds = torch.bucketize(ct_snr, boundaries=self.bins[0]) - 1
                    ct_chisq_over_snr2_inds = (
                        torch.bucketize(ct_chisq_over_snr2, boundaries=self.bins[1]) - 1
                    )
                    for j, bankid in enumerate(self.bankids_map.keys()):
                        # FIXME: consider avoiding the for loop
                        # Get counts from this bankid
                        this_bankid_mask = ct_trig_bankids == j
                        if True in this_bankid_mask:
                            snr_inds_this_bankid = (
                                ct_snr_inds[this_bankid_mask].to("cpu").numpy()
                            )
                            chisq_over_snr2_inds_this_bankid = (
                                ct_chisq_over_snr2_inds[this_bankid_mask]
                                .to("cpu")
                                .numpy()
                            )
                            counts = numpy.ravel_multi_index(
                                (
                                    snr_inds_this_bankid,
                                    chisq_over_snr2_inds_this_bankid,
                                ),
                                self.binning.shape,
                            )
                            ct_temp = (
                                self.likelihood_ratios[bankid]
                                .terms["P_of_SNR_chisq"]
                                .count_tracker_chi_temp
                            )
                            if time in ct_temp[ifo]:
                                ct_temp[ifo][time] = numpy.concatenate(
                                    (ct_temp[ifo][time], counts)
                                )
                            else:
                                ct_temp[ifo][time] = counts
                            while len(ct_temp[ifo]) > num_keys:
                                ct_temp[ifo].popitem(last=False)

    def store_counts(self, gracedb_times):
        # Store counts for all bins whenever there is a gracedb upload
        for lr in self.likelihood_ratios.values():
            lr.terms["P_of_SNR_chisq"].store_counts(gracedb_times)
