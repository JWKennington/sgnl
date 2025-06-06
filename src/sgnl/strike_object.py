"""A class that contains all the strike related input/output files needed for
strike elements
"""

# Copyright (C) 2025 Yun-Jing Huang

import gc
import io
import os
import shutil
import sys
import time
import zlib
from collections.abc import Sequence
from dataclasses import dataclass
from time import asctime
from typing import Any

import numpy
import torch
from igwn_ligolw import utils as ligolw_utils
from strike.config import get_analysis_config
from strike.stats import far
from strike.stats.far import RankingStatPDF
from strike.stats.likelihood_ratio import LnLikelihoodRatio, P_of_Template

default_config = get_analysis_config()["default"]

GC = False


def xml_string(rstat):
    f = io.BytesIO()
    rstat.save_fileobj(f)
    if GC:
        print("gc save fileobj", gc.collect())
    f.seek(0)
    return f.read().decode("utf-8")


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
    chi2_over_snr2_min: float = default_config["chi2_over_snr2_min"]
    chi2_over_snr2_max: float = default_config["chi2_over_snr2_max"]
    chi_bin_min: float = default_config["chi_bin_min"]
    chi_bin_max: float = default_config["chi_bin_max"]
    chi_bin_num: int = default_config["chi_bin_num"]
    compress_likelihood_ratio: bool = False
    compress_likelihood_ratio_threshold: float = 0.03
    FAR_trialsfactor: float = 1
    ifos: list = None
    injections: bool = False
    input_likelihood_file: list[str] = None
    is_online: bool = False
    min_instruments: int = 1
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
        if not self.is_online:
            # offline mode
            if self.output_likelihood_file is not None:
                for bankid, ids in self.bankids_map.items():
                    # offline mode, so initialize a LnLikelihoodRatio instance
                    bank_template_ids = self.all_template_ids[ids]
                    bank_template_ids = numpy.array(
                        bank_template_ids[bank_template_ids != -1]
                    )

                    # Ranking stat output
                    self.likelihood_ratios[bankid] = LnLikelihoodRatio(
                        template_ids=bank_template_ids,
                        instruments=self.ifos,
                        min_instruments=self.min_instruments,
                        delta_t=self.coincidence_threshold,
                        chi2_over_snr2_min=self.chi2_over_snr2_min,
                        chi2_over_snr2_max=self.chi2_over_snr2_max,
                        chi_bin_min=self.chi_bin_min,
                        chi_bin_max=self.chi_bin_max,
                        chi_bin_num=self.chi_bin_num,
                    )
        else:
            # load input files
            for i, (bankid, lr_file) in enumerate(self.input_likelihood_file.items()):
                if not self.nsubbank_pretend or (self.nsubbank_pretend and i == 0):
                    # if we are in nsubbbank_pretend mode, only load the first lr
                    # file and copy the rest
                    lr_class = LnLikelihoodRatio.load(lr_file)
                    if GC:
                        print("gc load lr", gc.collect())
                    assert lr_class.min_instruments == self.min_instruments
                self.likelihood_ratios[bankid] = lr_class
                if self.compress_likelihood_ratio:
                    self.likelihood_ratios[bankid].terms[
                        "P_of_tref_Dh"
                    ].horizon_history.compress(
                        threshold=self.compress_likelihood_ratio_threshold,
                        verbose=self.verbose,
                    )

            template_ids = numpy.concatenate(
                [lr.template_ids for lr in self.likelihood_ratios.values()]
            )
            P_of_Template.load_population_model(
                template_ids,
                next(iter(self.likelihood_ratios.values())).population_model_file,
            )

            # load dtdphi file only once, and share among the banks
            # assumes the dtdphi file is the same across banks
            dtdphi_file = set(
                [
                    f.terms["P_of_dt_dphi"].dtdphi_file
                    for f in self.likelihood_ratios.values()
                ]
            )
            assert len(dtdphi_file) == 1, "Can only support banks with same dtdphi file"

            self.frankensteins = {}
            self.likelihood_ratio_uploads = {}

            for bankid in self.bankids_map:
                frankensteins, likelihood_ratio_uploads = (
                    StrikeObject._update_assign_lr(self.likelihood_ratios[bankid])
                )
                self.frankensteins[bankid] = frankensteins
                self.likelihood_ratio_uploads[bankid] = likelihood_ratio_uploads

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
                if GC:
                    print("gc load zerolag", gc.collect())
            else:
                self.zerolag_rank_stat_pdfs = None

        #
        # SNR chisq histogram
        #
        if self.all_template_ids is None:
            # Note this is only for quick pipeline testing purposes
            # Should not use this in a real pipeline
            ntempmax = 0
            temps = []
            for lr in self.likelihood_ratios.values():
                if len(lr.template_ids) > 600:
                    ntemp = int(round(len(lr.template_ids) / 2))
                    lrtemp = list(lr.template_ids)
                    temps.append(lrtemp[:ntemp])
                    temps.append(lrtemp[-ntemp:])
                    if ntemp > ntempmax:
                        ntempmax = ntemp
                else:
                    ntemp = len(lr.template_ids)
                    temps.append(lr.template_ids)
                    if ntemp > ntempmax:
                        ntempmax = ntemp

            nsubbanks = len(temps)
            self.all_template_ids = numpy.zeros(
                (nsubbanks, ntempmax), dtype=numpy.int32
            )
            for i, t in enumerate(temps):
                n = len(t)
                self.all_template_ids[i, :n] = t

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

    @staticmethod
    def _load_lr(in_lr_file):
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
        return lr

    @staticmethod
    def _update_assign_lr(lr):
        # Make frankensteins and call the finish method

        frankenstein = None
        likelihood_ratio_upload = None
        if lr.is_healthy():
            frankenstein = lr.copy(frankenstein=True)
            frankenstein.finish()

            likelihood_ratio_upload = lr.copy(frankenstein=True)
            print("likelihood ratio assignment ENABLED", file=sys.stderr, flush=True)
        else:
            print("likelihood ratio assignment DISABLED", file=sys.stderr, flush=True)

        return frankenstein, likelihood_ratio_upload

    def update_dynamic(
        self, bankid, frankenstein, likelihood_ratio_upload, new_lr=None
    ):
        # Update triggerrates and horizon_history since they are not
        # updated in the subprocess
        if new_lr is not None:
            old_lr = self.likelihood_ratios[bankid]
            # sanity check lrs
            if (
                new_lr.instruments,
                new_lr.min_instruments,
                new_lr.delta_t,
            ) != (
                old_lr.instruments,
                old_lr.min_instruments,
                old_lr.delta_t,
            ) or (new_lr.template_ids != old_lr.template_ids).any():
                raise ValueError("Found incompatible ranking statistic configuration")
            else:
                triggerrates = old_lr.terms["P_of_tref_Dh"].triggerrates
                horizon_history = old_lr.terms["P_of_tref_Dh"].horizon_history
                old_lr.terms["P_of_tref_Dh"].triggerrates = None
                old_lr.terms["P_of_tref_Dh"].horizon_history = None
                old_lr.triggerrates = None
                old_lr.horizon_history = None

                # replace with new dictionary because this lowers the ram for
                # some reason, I don't know if the root cause is in the LR class
                new_lrs = {
                    bid: f for bid, f in self.likelihood_ratios.items() if bid != bankid
                }
                del old_lr
                del self.likelihood_ratios
                if GC:
                    print("gc del likelihood ratios", gc.collect())

                # update triggerrates and horizon history
                new_lr.terms["P_of_tref_Dh"].triggerrates = triggerrates
                new_lr.terms["P_of_tref_Dh"].horizon_history = horizon_history
                new_lr.triggerrates = triggerrates
                new_lr.horizon_history = horizon_history

                new_lrs[bankid] = new_lr
                self.likelihood_ratios = new_lrs

        if frankenstein is not None:
            # replace with new dictionary because this lowers the ram for
            # some reason, I don't know if the root cause is in the LR class
            # FIXME: This reduces the ram, but is a mess
            new_frankensteins = {
                bid: f for bid, f in self.frankensteins.items() if bid != bankid
            }
            new_likelihood_ratio_uploads = {
                bid: f
                for bid, f in (self.likelihood_ratio_uploads.items())
                if bid != bankid
            }
            del self.frankensteins
            del self.likelihood_ratio_uploads
            if GC:
                print("gc frank", gc.collect(), flush=True)

            lr = self.likelihood_ratios[bankid]
            triggerrates = lr.terms["P_of_tref_Dh"].triggerrates
            horizon_history = lr.terms["P_of_tref_Dh"].horizon_history

            likelihood_ratio_upload.terms["P_of_tref_Dh"].triggerrates = triggerrates
            likelihood_ratio_upload.terms["P_of_tref_Dh"].horizon_history = (
                horizon_history
            )
            likelihood_ratio_upload.triggerrates = triggerrates
            likelihood_ratio_upload.horizon_history = horizon_history
            new_likelihood_ratio_uploads[bankid] = likelihood_ratio_upload

            frankenstein.terms["P_of_tref_Dh"].triggerrate = triggerrates
            frankenstein.terms["P_of_tref_Dh"].horizon_history = horizon_history
            frankenstein.triggerrates = triggerrates
            frankenstein.horizon_history = horizon_history
            new_frankensteins[bankid] = frankenstein

            self.frankensteins = new_frankensteins
            self.likelihood_ratio_uploads = new_likelihood_ratio_uploads

        # Now clear references manually
        if GC:
            if gc.garbage:
                print("Uncollectable objects found:", flush=True)
                for item in gc.garbage:
                    del item  # Attempt to break references
                gc.collect()  # Collect again after attempting to break references
                print(f"Garbage list is now: {gc.garbage}", flush=True)
            print("gc after cleaning gc.garbage", gc.collect(), flush=True)
            # print("ram after switching riggerrates", ram(), flush=True)
            print("gc garbage", gc.garbage, flush=True)

    def load_rank_stat_pdf(self):
        if os.access(
            ligolw_utils.local_path_from_url(self.rank_stat_pdf_file), os.R_OK
        ):
            if self.fapfar is not None:
                self.fapfar.ccdf_interpolator = None
                self.fapfar = None
                self.rank_stat_pdf.noise_lr_lnpdf = None
                self.rank_stat_pdf.signal_lr_lnpdf = None
                self.rank_stat_pdf.zero_lag_lr_lnpdf = None
                self.rank_stat_pdf.segments = None
                self.rank_stat_pdf.template_ids = None
                self.rank_stat_pdf.instruments = None
                self.rank_stat_pdf.noise_counts_before_extinction = None
                self.rank_stat_pdf = None
                del self.fapfar
                del self.rank_stat_pdf
                if GC:
                    print("gc", gc.collect(), flush=True)

            rspdf = RankingStatPDF.load(self.rank_stat_pdf_file)
            if GC:
                print("gc load pdf", gc.collect())
            self.rank_stat_pdf = rspdf

            for bankid in self.bankids_map:
                if not numpy.isin(
                    self.likelihood_ratios[bankid].template_ids, rspdf.template_ids
                ).all():
                    raise ValueError("wrong templates")

            if rspdf.is_healthy(self.verbose):
                self.fapfar = far.FAPFAR(rspdf.new_with_extinction())
                print(
                    "false-alarm probability and rate assignment ENABLED",
                    file=sys.stderr,
                )
            else:
                self.fapfar = None
                print(
                    "false-alarm probability and rate assignment DISABLED",
                    file=sys.stderr,
                )
        else:
            # If we are unable to load the file, disable FAR assignment
            # FIXME: should we make it fail?
            self.rank_stat_pdf = None
            self.fapfar = None
            print(
                f"{asctime()} cannot load file, false-alarm probability and ",
                "rate assignment DISABLED",
                file=sys.stderr,
            )

    def _save_snapshot(self, bankid, fn):
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
            ligolw_utils.local_path_from_url(self.zerolag_rank_stat_pdf_file[bankid]),
        )

    @staticmethod
    def save_snapshot(lr, fn, ofn):
        # write snapshot files to disk
        lr.save(fn)
        if GC:
            print("gc save lr", gc.collect())

        # copy snapshot to output path
        shutil.copy(
            fn,
            ligolw_utils.local_path_from_url(ofn),
        )

    def _save_snr_chi_lnpdf(self, bankid):
        for ifo in self.ifos:
            i = list(self.bankids_map.keys()).index(bankid)
            # FIXME the casting to float is to resolve strike errors
            self.likelihood_ratios[bankid].terms[
                "P_of_SNR_chisq"
            ].snr_chisq_lnpdf_noise[f"{ifo}_snr_chi"].array = (
                self.snr_chisq_lnpdf_noise[ifo][i].to("cpu").numpy().astype(float)
            )

    @staticmethod
    def save_snr_chi_lnpdf(ifos, bankidix, lr, snr_chisq_lnpdf_noise):
        for ifo in ifos:
            # FIXME the casting to float is to resolve strike errors
            lr.terms["P_of_SNR_chisq"].snr_chisq_lnpdf_noise[f"{ifo}_snr_chi"].array = (
                snr_chisq_lnpdf_noise[ifo][bankidix].to("cpu").numpy().astype(float)
            )

    def _save_counts_by_template_id(self, bankid):
        ids = self.bankids_map[bankid]
        counts = self.counts_by_template_id_counter
        counts_by_template_id = (
            self.likelihood_ratios[bankid].terms["P_of_Template"].counts_by_template_id
        )
        counts_this_bank = counts[ids].to("cpu").numpy().ravel()
        count_mask = counts_this_bank > 0
        counts_this_bank = counts_this_bank[count_mask]
        template_ids_this_bank = self.all_template_ids[ids].ravel()[count_mask]
        assert len(counts_this_bank) == len(template_ids_this_bank)
        for tid, count in zip(template_ids_this_bank, counts_this_bank):
            assert tid in counts_by_template_id
            counts_by_template_id[tid] += count

        # reset the counter
        self.counts_by_template_id_counter[ids] = 0

    @staticmethod
    def save_counts_by_template_id(lr, counts_this_bank, all_template_ids):
        # NOTE: modifies the contents of lr in place
        counts_by_template_id = lr.terms["P_of_Template"].counts_by_template_id
        count_mask = counts_this_bank > 0
        counts_this_bank = counts_this_bank[count_mask]
        template_ids_this_bank = all_template_ids.ravel()[count_mask]
        assert len(counts_this_bank) == len(template_ids_this_bank)
        for tid, count in zip(template_ids_this_bank, counts_this_bank):
            assert tid in counts_by_template_id
            counts_by_template_id[tid] += count

    def update_array_data(self, bankid):
        # useful variables
        lr = self.likelihood_ratios[bankid]

        # Save the snr chi counts
        StrikeObject.save_snr_chi_lnpdf(
            self.ifos,
            list(self.bankids_map.keys()).index(bankid),
            lr,
            self.snr_chisq_lnpdf_noise,
        )

        # Save counts by template id
        ids = self.bankids_map[bankid]
        counts = self.counts_by_template_id_counter
        counts_this_bank = counts[ids].to("cpu").numpy().ravel()
        StrikeObject.save_counts_by_template_id(
            lr, counts_this_bank, self.all_template_ids[ids]
        )
        # reset the counter
        self.counts_by_template_id_counter[ids] = 0

    @staticmethod
    def reset_dynamic(frankenstein, likelihood_ratio_upload, new_lr=None):
        # Set dynamic attributes to None in the subprocess
        # because we will be replacing them later in the main process
        # with the up-to-date objects.
        # Will probably lower the ram increase when put into the queue.
        if new_lr is not None:
            new_lr.terms["P_of_tref_Dh"].triggerrates = None
            new_lr.terms["P_of_tref_Dh"].horizon_history = None
            new_lr.triggerrates = None
            new_lr.horizon_history = None

        if frankenstein is not None:
            frankenstein.terms["P_of_tref_Dh"].triggerrates = None
            frankenstein.terms["P_of_tref_Dh"].horizon_history = None
            frankenstein.triggerrates = None
            frankenstein.horizon_history = None
            likelihood_ratio_upload.terms["P_of_tref_Dh"].triggerrates = None
            likelihood_ratio_upload.terms["P_of_tref_Dh"].horizon_history = None
            likelihood_ratio_upload.triggerrates = None
            likelihood_ratio_upload.horizon_history = None
        if GC:
            print("gc set triggerrates None", gc.collect(), flush=True)

    @staticmethod
    def on_snapshot_reload(in_lr_file):
        # This method is for injection jobs
        # FIXME: find a way to merge this with on_snapshot method??
        new_lr = StrikeObject._load_lr(in_lr_file)
        frankenstein, likelihood_ratio_upload = StrikeObject._update_assign_lr(new_lr)

        StrikeObject.reset_dynamic(frankenstein, likelihood_ratio_upload, new_lr)

        lr_dict = {
            "new_lr": new_lr,
            "frankenstein": frankenstein,
            "likelihood_ratio_upload": likelihood_ratio_upload,
        }
        return lr_dict

    def prepare_inq_data(self, fn, bankid):
        lr = self.likelihood_ratios[bankid]
        zero_lr = self.zerolag_rank_stat_pdfs[bankid]
        in_lr_file = self.input_likelihood_file[bankid]
        output_likelihood_file = self.output_likelihood_file[bankid]
        zerolag_output_file = self.zerolag_rank_stat_pdf_file[bankid]
        verbose = self.verbose
        data = {
            "lr": lr,
            "zero_lr": zero_lr,
            "bankid": bankid,
            "output_likelihood_file": output_likelihood_file,
            "output_zerolag_likelihood_file": zerolag_output_file,
            "fn": fn,
            "in_lr_file": in_lr_file,
            "verbose": verbose,
        }
        return data

    @staticmethod
    def snapshot_io(lr, zero_lr, fn, outfile, zero_outfile):
        print(f"{asctime()} Writing out likelihood ratio and zerolag file {fn}...")
        StrikeObject.save_snapshot(lr, fn, outfile)
        StrikeObject.save_snapshot(
            zero_lr,
            fn.replace("LIKELIHOOD_RATIO", "ZEROLAG_RANK_STAT_PDFS"),
            zero_outfile,
        )

    @staticmethod
    def snapshot_fileobj(lr, zero_lr, bankid):
        return {
            "xml": {bankid: xml_string(lr)},
            "zerolagxml": {bankid: xml_string(zero_lr)},
        }

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
                self.counts_by_template_id_counter += single_mask

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
