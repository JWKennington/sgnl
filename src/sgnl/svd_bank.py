"""This module contains methods for reading in SVD bank files."""

# Copyright (C) 2009      Kipp Cannon, Chad Hanna
# Copyright (C) 2010      Kipp Cannon, Chad Hanna, Leo Singer
# Copyright (C) 2024-2025 Yun-Jing Huang

import copy
import sys
import tempfile
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import lal
import numpy
import scipy
from igwn_ligolw import ligolw, lsctables
from igwn_ligolw import utils as ligolw_utils
from igwn_ligolw.array import use_in as array_use_in
from igwn_ligolw.ligolw import Array as ligolw_array
from igwn_ligolw.ligolw import Param as ligolw_param
from igwn_ligolw.param import use_in as param_use_in
from igwn_ligolw.utils import process as ligolw_process

from sgnl import cbc_template_fir, chirptime, spawaveform, templates
from sgnl.psd import HorizonDistance, condition_psd

Attributes = ligolw.sax.xmlreader.AttributesImpl


@array_use_in
@param_use_in
@lsctables.use_in
class DefaultContentHandler(ligolw.LIGOLWContentHandler):
    pass


# FIXME do we want to hardcode the program?
# FIXME Remove gstlal_bank_splitter once we're not longer using gstlal
def read_approximant(
    xmldoc, programs=("gstlal_bank_splitter", "sgnl-inspiral-bank-splitter")
):
    process_ids = set()
    for program in programs:
        process_ids |= lsctables.ProcessTable.get_table(xmldoc).get_ids_by_program(
            program
        )
    if not process_ids:
        raise ValueError(
            "document must contain process entries from %s" % ", ".join(programs)
        )
    approximant = set(
        row.pyvalue
        for row in lsctables.ProcessParamsTable.get_table(xmldoc)
        if (row.process_id in process_ids) and (row.param == "--approximant")
    )
    if not approximant:
        raise ValueError(
            "document must contain an 'approximant' process_params entry from %s"
            % ", ".join("'%s'" for program in programs)
        )
    if len(approximant) > 1:
        raise ValueError("document must contain only one approximant")
    approximant = approximant.pop()
    templates.sgnl_valid_approximant(approximant)
    return approximant


def check_ffinal_and_find_max_ffinal(xmldoc):
    f_final = lsctables.SnglInspiralTable.get_table(xmldoc).getColumnByName("f_final")
    if not all(f_final):
        raise ValueError("f_final column not populated")
    return max(f_final)


def max_stat_thresh(coeffs, fap, samp_tol=100.0):
    num = int(samp_tol / fap)
    out = numpy.zeros(num)
    for c in coeffs:
        out += c * scipy.randn(num) ** 2
    out.sort()
    return float(out[-int(samp_tol)])


def sum_of_squares_threshold_from_fap(fap, coefficients):
    return max_stat_thresh(coefficients, fap)


def group(inlist, parts):
    """!
    group a list roughly according to the distribution in parts, e.g.

    >>> A = list(range(12))
    >>> B = [2,3]
    >>> for g in group(A,B):
    ...     print(g)
    ...
    [0, 1]
    [2, 3]
    [4, 5]
    [6, 7, 8]
    [9, 10, 11]
    """
    mult_factor = len(inlist) // sum(parts) + 1
    inlist_copy = copy.deepcopy(inlist)
    for p in parts:
        for _j in range(mult_factor):
            if not inlist_copy:
                break
            yield inlist_copy[:p]
            del inlist_copy[:p]


@dataclass
class BankFragment:
    rate: int
    start: float
    end: float

    def __post_init__(self):
        self.orthogonal_template_bank = None
        self.singular_values = None
        self.mix_matrix = None
        self.chifacs = None

    # sum_of_squares_weights: Sequence[Any]
    def set_template_bank(
        self,
        template_bank,
        tolerance,
        snr_thresh,
        identity_transform=False,
        verbose=False,
    ):
        if verbose:
            print("\t%d templates of %d samples" % template_bank.shape, file=sys.stderr)

        (
            self.orthogonal_template_bank,
            self.singular_values,
            self.mix_matrix,
            self.chifacs,
        ) = cbc_template_fir.decompose_templates(
            template_bank, tolerance, identity=identity_transform
        )

        if verbose:
            print(
                "\tidentified %d components" % self.orthogonal_template_bank.shape[0],
                file=sys.stderr,
            )


@dataclass
class Bank:
    bank_xmldoc: object
    psd: Sequence[Any]
    time_slices: Sequence[Any]
    snr_threshold: float
    tolerance: float
    clipleft: int | None = None
    clipright: int | None = None
    flow: float = 40.0
    autocorrelation_length: int | None = None
    logname: str | None = None
    identity_transform: bool = False
    verbose: bool = False
    bank_id: int | None = None
    fhigh: float | None = None

    def __post_init__(self):
        self.template_bank_filename = None
        self.filter_length = self.time_slices["end"].max()
        self.snr_threshold = self.snr_threshold
        if self.logname is not None and not self.logname:
            raise ValueError("logname cannot be empty if it is set")
        (
            self.template_bank,
            self.autocorrelation_bank,
            self.autocorrelation_mask,
            self.sigmasq,
            self.bank_workspace,
        ) = cbc_template_fir.generate_templates(
            lsctables.SnglInspiralTable.get_table(self.bank_xmldoc),
            read_approximant(self.bank_xmldoc),
            self.psd,
            self.flow,
            self.time_slices,
            autocorrelation_length=self.autocorrelation_length,
            fhigh=self.fhigh,
            verbose=self.verbose,
        )

        sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(self.bank_xmldoc)
        self.sngl_inspiral_table = sngl_inspiral_table.copy()
        self.sngl_inspiral_table.extend(sngl_inspiral_table)
        self.processed_psd = self.bank_workspace.psd
        self.newdeltaF = 1.0 / self.bank_workspace.working_duration
        self.working_f_low = self.bank_workspace.working_f_low
        self.f_low = self.bank_workspace.f_low
        self.sample_rate_max = self.bank_workspace.sample_rate_max
        self.bank_fragments = [
            BankFragment(rate, begin, end)
            for rate, begin, end in self.bank_workspace.time_slices
        ]
        self.bank_correlation_matrix = None

        for i, bank_fragment in enumerate(self.bank_fragments):
            if self.verbose:
                print(
                    "constructing template decomposition %d of %d:  %g s ... %g s"
                    % (
                        i + 1,
                        len(self.bank_fragments),
                        -bank_fragment.end,
                        -bank_fragment.start,
                    ),
                    file=sys.stderr,
                )
            bank_fragment.set_template_bank(
                self.template_bank[i],
                self.tolerance,
                self.snr_threshold,
                identity_transform=self.identity_transform,
                verbose=self.verbose,
            )
            cmix = (
                bank_fragment.mix_matrix[:, ::2]
                + 1.0j * bank_fragment.mix_matrix[:, 1::2]
            )

            if self.bank_correlation_matrix is None:
                self.bank_correlation_matrix = numpy.dot(numpy.conj(cmix.T), cmix)
            else:
                self.bank_correlation_matrix += numpy.dot(numpy.conj(cmix.T), cmix)

        clipright = (
            len(self.sngl_inspiral_table) - self.clipright
            if self.clipright is not None
            else None
        )
        doubled_clipright = clipright * 2 if clipright is not None else None
        doubled_clipleft = self.clipleft * 2 if self.clipleft is not None else None

        new_sngl_table = self.sngl_inspiral_table.copy()
        for row in self.sngl_inspiral_table[self.clipleft : clipright]:
            row.Gamma1 = int(self.bank_id.split("_")[0])
            new_sngl_table.append(row)
        self.sngl_inspiral_table = new_sngl_table
        self.autocorrelation_bank = self.autocorrelation_bank[
            self.clipleft : clipright, :
        ]
        self.autocorrelation_mask = self.autocorrelation_mask[
            self.clipleft : clipright, :
        ]
        self.sigmasq = self.sigmasq[self.clipleft : clipright]
        self.bank_correlation_matrix = self.bank_correlation_matrix[
            self.clipleft : clipright, self.clipleft : clipright
        ]
        for frag in self.bank_fragments:
            if frag.mix_matrix is not None:
                frag.mix_matrix = frag.mix_matrix[:, doubled_clipleft:doubled_clipright]
            frag.chifacs = frag.chifacs[doubled_clipleft:doubled_clipright]

    def get_rates(self):
        return set(bank_fragment.rate for bank_fragment in self.bank_fragments)

    def set_template_bank_filename(self, name):
        self.template_bank_filename = name


def cal_higher_f_low(bank_sngl_table, f_high, flow, approximant, max_duration):
    def time_freq_bound(flow, max_duration, m1, m2, j1, j2, f_max):
        """
        To find the root of the function (flow)
        """
        return (
            chirptime.imr_time(f=flow, m1=m1, m2=m2, j1=j1, j2=j2, f_max=f_max)
            - max_duration
        )

    time_constrained_f_low = []
    for row in bank_sngl_table:
        m1_SI = lal.MSUN_SI * row.mass1
        m2_SI = lal.MSUN_SI * row.mass2
        spin1 = numpy.dot(row.spin1, row.spin1) ** 0.5
        spin2 = numpy.dot(row.spin2, row.spin2) ** 0.5
        f_max = min(
            row.f_final,
            (
                2
                * chirptime.ringf(
                    lal.MSUN_SI * row.mass1 + lal.MSUN_SI * row.mass2,
                    chirptime.overestimate_j_from_chi(max(spin1, spin2)),
                )
                if approximant in templates.sgnl_IMR_approximants
                else spawaveform.ffinal(row.mass1, row.mass2, "bkl_isco")
            ),
        )

        time_constrained_f_low.append(
            scipy.optimize.fsolve(
                time_freq_bound,
                x0=flow,
                args=(max_duration, m1_SI, m2_SI, spin1, spin2, f_max),
            )
        )
    f_low = float(max(flow, max(time_constrained_f_low)))
    if f_high is not None and f_high < f_low:
        raise ValueError(
            "Lower frequency must be lower than higher frequency cut off! Input "
            "max_duration is too short."
        )

    return f_low


def build_bank(
    template_bank_url,
    psd,
    flow,
    max_duration,
    svd_tolerance,
    clipleft=None,
    clipright=None,
    padding=1.5,
    identity_transform=False,
    verbose=False,
    snr_threshold=0,
    autocorrelation_length=201,
    samples_min=1024,
    samples_max_256=1024,
    samples_max_64=2048,
    samples_max=4096,
    bank_id=None,
    contenthandler=None,
    sample_rate=None,
    instrument_override=None,
):
    bank_xmldoc = ligolw_utils.load_url(
        template_bank_url, contenthandler=contenthandler, verbose=verbose
    )
    bank_sngl_table = lsctables.SnglInspiralTable.get_table(bank_xmldoc)
    # FIXME Do we want to hardcode the program here?
    try:
        (approximant,) = ligolw_process.get_process_params(
            bank_xmldoc, "sgnl-inspiral-bank-splitter", "--approximant"
        )
    except ValueError:
        (approximant,) = ligolw_process.get_process_params(
            bank_xmldoc, "gstlal_bank_splitter", "--approximant"
        )

    fhigh = check_ffinal_and_find_max_ffinal(bank_xmldoc)
    flow = cal_higher_f_low(bank_sngl_table, fhigh, flow, approximant, max_duration)
    if instrument_override is not None:
        for row in bank_sngl_table:
            row.ifo = instrument_override

    time_freq_bounds = templates.time_slices(
        bank_sngl_table,
        fhigh=check_ffinal_and_find_max_ffinal(bank_xmldoc),
        flow=flow,
        padding=padding,
        samples_min=samples_min,
        samples_max_256=samples_max_256,
        samples_max_64=samples_max_64,
        samples_max=samples_max,
        sample_rate=sample_rate,
        verbose=verbose,
    )

    if sample_rate is None:
        fhigh = None

    # Generate templates, perform SVD, get orthogonal basis
    # and store as Bank object
    bank = Bank(
        bank_xmldoc,
        psd[bank_sngl_table[0].ifo],
        time_freq_bounds,
        tolerance=svd_tolerance,
        clipleft=clipleft,
        clipright=clipright,
        flow=flow,
        autocorrelation_length=autocorrelation_length,  # samples
        identity_transform=identity_transform,
        verbose=verbose,
        snr_threshold=snr_threshold,
        bank_id=bank_id,
        fhigh=fhigh,
    )

    bank.set_template_bank_filename(ligolw_utils.local_path_from_url(template_bank_url))
    return bank


def write_bank(filename, banks, psd_input, process_param_dict=None, verbose=False):
    xmldoc = ligolw.Document()
    lw = xmldoc.appendChild(ligolw.LIGO_LW())
    if process_param_dict:
        ligolw_process.register_to_xmldoc(
            xmldoc,
            program="sgnl_inspiral_svd_bank",
            paramdict=process_param_dict,
            comment="Process parameter tables for further calculation",
        )
    for bank in banks:
        root = lw.appendChild(
            ligolw.LIGO_LW(Attributes({"Name": "sgnl_svd_bank_Bank"}))
        )

        for row in bank.sngl_inspiral_table:
            row.template_duration = bank.bank_fragments[-1].end

        if verbose:
            print("computing lambda/eta parameters for templates...")
        for row, auto_correlation in zip(
            bank.sngl_inspiral_table, bank.autocorrelation_bank
        ):
            row.Gamma2, row.Gamma3, row.Gamma4, row.Gamma5 = calc_lambda_eta_sum(
                auto_correlation
            )
        root.appendChild(bank.sngl_inspiral_table)
        root.appendChild(ligolw_param.from_pyvalue("filter_length", bank.filter_length))
        root.appendChild(ligolw_param.from_pyvalue("logname", bank.logname or ""))
        root.appendChild(ligolw_param.from_pyvalue("snr_threshold", bank.snr_threshold))
        root.appendChild(
            ligolw_param.from_pyvalue(
                "template_bank_filename", bank.template_bank_filename
            )
        )
        root.appendChild(ligolw_param.from_pyvalue("bank_id", bank.bank_id))
        root.appendChild(ligolw_param.from_pyvalue("new_deltaf", bank.newdeltaF))
        root.appendChild(ligolw_param.from_pyvalue("working_f_low", bank.working_f_low))
        root.appendChild(ligolw_param.from_pyvalue("f_low", bank.f_low))
        root.appendChild(
            ligolw_param.from_pyvalue("sample_rate_max", int(bank.sample_rate_max))
        )
        root.appendChild(
            ligolw_array.build(
                "autocorrelation_bank_real", bank.autocorrelation_bank.real
            )
        )
        root.appendChild(
            ligolw_array.build(
                "autocorrelation_bank_imag", bank.autocorrelation_bank.imag
            )
        )
        root.appendChild(
            ligolw_array.build("autocorrelation_mask", bank.autocorrelation_mask)
        )
        root.appendChild(ligolw_array.build("sigmasq", numpy.array(bank.sigmasq)))
        root.appendChild(
            ligolw_array.build(
                "bank_correlation_matrix_real", bank.bank_correlation_matrix.real
            )
        )
        root.appendChild(
            ligolw_array.build(
                "bank_correlation_matrix_imag", bank.bank_correlation_matrix.imag
            )
        )

        for frag in bank.bank_fragments:
            el = root.appendChild(ligolw.LIGO_LW())

            el.appendChild(ligolw_param.from_pyvalue("rate", int(frag.rate)))
            el.appendChild(ligolw_param.from_pyvalue("start", frag.start))
            el.appendChild(ligolw_param.from_pyvalue("end", frag.end))
            el.appendChild(ligolw_array.build("chifacs", frag.chifacs))
            if frag.mix_matrix is not None:
                el.appendChild(ligolw_array.build("mix_matrix", frag.mix_matrix))
            el.appendChild(
                ligolw_array.build(
                    "orthogonal_template_bank", frag.orthogonal_template_bank
                )
            )
            if frag.singular_values is not None:
                el.appendChild(
                    ligolw_array.build("singular_values", frag.singular_values)
                )

    psd = psd_input[bank.sngl_inspiral_table[0].ifo]
    lal.series.make_psd_xmldoc({bank.sngl_inspiral_table[0].ifo: psd}, lw)

    ligolw_utils.write_filename(xmldoc, filename, verbose=verbose)


def read_banks(filename, contenthandler, verbose=False, fast=False):
    # Load document
    xmldoc = ligolw_utils.load_url(
        filename, contenthandler=contenthandler, verbose=verbose
    )

    banks = []

    # FIXME in principle this could be different for each bank included in
    # this file, but we only put one in the file for now
    # FIXME, right now there is only one instrument so we just pull out the
    # only psd there is
    try:
        raw_psd = list(lal.series.read_psd_xmldoc(xmldoc).values())[0]
    except ValueError:
        # the bank file does not contain psd ligolw element.
        raw_psd = None

    for root in (
        elem
        for elem in xmldoc.getElementsByTagName(ligolw.LIGO_LW.tagName)
        if elem.hasAttribute("Name")
        and elem.Name in ["sgnl_svd_bank_Bank", "gstlal_svd_bank_Bank"]
    ):
        # Create new SVD bank object
        bank = Bank.__new__(Bank)

        # Read sngl inspiral table
        bank.sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(root)
        bank.sngl_inspiral_table.parentNode.removeChild(bank.sngl_inspiral_table)

        # Read root-level scalar parameters
        bank.bank_id = ligolw_param.get_param(root, "bank_id").value
        bank.sample_rate_max = ligolw_param.get_param(root, "sample_rate_max").value
        if not fast:
            bank.filter_length = ligolw_param.get_param(root, "filter_length").value
            bank.logname = ligolw_param.get_param(root, "logname") or None
            bank.snr_threshold = ligolw_param.get_param(root, "snr_threshold").value
            bank.template_bank_filename = ligolw_param.get_param(
                root, "template_bank_filename"
            ).value
            try:
                bank.newdeltaF = ligolw_param.get_param(root, "new_deltaf").value
                bank.working_f_low = ligolw_param.get_param(root, "working_f_low").value
                bank.f_low = ligolw_param.get_param(root, "f_low").value
                bank.sample_rate_max = ligolw_param.get_param(
                    root, "sample_rate_max"
                ).value
            except ValueError:
                pass

        # Read root-level arrays
        bank.autocorrelation_bank = (
            ligolw_array.get_array(root, "autocorrelation_bank_real").array
            + 1j * ligolw_array.get_array(root, "autocorrelation_bank_imag").array
        )
        bank.sigmasq = ligolw_array.get_array(root, "sigmasq").array

        if not fast:
            bank.autocorrelation_mask = ligolw_array.get_array(
                root, "autocorrelation_mask"
            ).array
            bank_correlation_real = ligolw_array.get_array(
                root, "bank_correlation_matrix_real"
            ).array
            bank_correlation_imag = ligolw_array.get_array(
                root, "bank_correlation_matrix_imag"
            ).array
            bank.bank_correlation_matrix = (
                bank_correlation_real + 1j * bank_correlation_imag
            )

            if raw_psd is not None:
                # reproduce the whitening psd and attach a reference to the psd
                bank.processed_psd = condition_psd(
                    raw_psd,
                    bank.newdeltaF,
                    minfs=(bank.working_f_low, bank.f_low),
                    maxfs=(
                        bank.sample_rate_max / 2.0 * 0.90,
                        bank.sample_rate_max / 2.0,
                    ),
                )
            else:
                bank.processed_psd = None

        # prepare the horizon distance factors
        bank.horizon_factors = dict(
            (row.template_id, sigmasq**0.5)
            for row, sigmasq in zip(bank.sngl_inspiral_table, bank.sigmasq)
        )

        # Read bank fragments
        bank.bank_fragments = []
        for el in (
            node for node in root.childNodes if node.tagName == ligolw.LIGO_LW.tagName
        ):
            frag = BankFragment(
                rate=ligolw_param.get_param(el, "rate").value,
                start=ligolw_param.get_param(el, "start").value,
                end=ligolw_param.get_param(el, "end").value,
            )

            if not fast:
                # Read arrays
                frag.chifacs = ligolw_array.get_array(el, "chifacs").array
                try:
                    frag.singular_values = ligolw_array.get_array(
                        el, "singular_values"
                    ).array
                except ValueError:
                    frag.singular_values = None

            try:
                frag.mix_matrix = ligolw_array.get_array(el, "mix_matrix").array
            except ValueError:
                frag.mix_matrix = None
            frag.orthogonal_template_bank = ligolw_array.get_array(
                el, "orthogonal_template_bank"
            ).array

            bank.bank_fragments.append(frag)

        banks.append(bank)
    template_id, func = horizon_distance_func(banks)
    template_id = abs(
        template_id
    )  # make sure horizon_distance_func did not pick the noise model template
    horizon_norm = None
    for bank in banks:
        if template_id in bank.horizon_factors:
            assert horizon_norm is None
            horizon_norm = bank.horizon_factors[template_id]
    for bank in banks:
        bank.horizon_distance_func = func
        bank.horizon_factors = dict(
            (tid, f / horizon_norm) for (tid, f) in bank.horizon_factors.items()
        )
    xmldoc.unlink()
    return banks


def preferred_horizon_distance_template(banks):
    template_id, m1, m2, s1z, s2z = min(
        (row.template_id, row.mass1, row.mass2, row.spin1z, row.spin2z)
        for bank in banks
        for row in bank.sngl_inspiral_table
    )
    return template_id, m1, m2, s1z, s2z


def horizon_distance_func(banks):
    """
    Takes a dictionary of objects returned by read_banks keyed by instrument
    """
    # span is [15 Hz, 0.85 * Nyquist frequency]
    # find the Nyquist frequency for the PSD to be used for each
    # instrument.  require them to all match
    nyquists = set((max(bank.get_rates()) / 2.0 for bank in banks))
    if len(nyquists) != 1:
        warnings.warn(
            "all banks should have the same Nyquist frequency to define a consistent "
            "horizon distance function (got {})".format(
                ", ".join(f"{rate:g}" for rate in sorted(nyquists))
            ),
            stacklevel=2,
        )
    # assume default 4 s PSD.  this is not required to be correct, but
    # for best accuracy it should not be larger than the true value and
    # for best performance it should not be smaller than the true
    # value.
    deltaF = 1.0 / 4.0
    # use the minimum template id as the cannonical horizon function
    template_id, m1, m2, s1z, s2z = preferred_horizon_distance_template(banks)

    return template_id, HorizonDistance(
        15.0,
        0.85 * max(nyquists),
        deltaF,
        m1,
        m2,
        spin1=(0.0, 0.0, s1z),
        spin2=(0.0, 0.0, s2z),
    )


def parse_bank_files(svd_banks, verbose, snr_threshold=None, fast=False):
    """
    given a dictionary of lists of svd template bank file names parse them
    into a dictionary of bank classes
    """

    banks = {}

    for instrument, filename in svd_banks.items():
        for n, bank in enumerate(
            read_banks(
                filename,
                contenthandler=DefaultContentHandler,
                verbose=verbose,
                fast=fast,
            )
        ):
            # Write out sngl inspiral table to temp file for
            # trigger generator
            # FIXME teach the trigger generator to get this
            # information a better way
            bank.template_bank_filename = tempfile.NamedTemporaryFile(
                suffix=".xml.gz", delete=False
            ).name
            xmldoc = ligolw.Document()
            # FIXME if this table reference is from a DB this
            # is a problem (but it almost certainly isn't)
            xmldoc.appendChild(ligolw.LIGO_LW()).appendChild(
                bank.sngl_inspiral_table.copy()
            ).extend(bank.sngl_inspiral_table)
            ligolw_utils.write_filename(
                xmldoc, bank.template_bank_filename, verbose=verbose
            )
            xmldoc.unlink()  # help garbage collector
            bank.logname = "%sbank%d" % (instrument, n)
            banks.setdefault(instrument, []).append(bank)
            if snr_threshold is not None:
                bank.snr_threshold = snr_threshold

    # FIXME remove when this is no longer an issue
    if not banks:
        raise ValueError(
            "Could not parse bank files into valid bank dictionary.\n\t- Perhaps you"
            " are using out-of-date svd bank files?  Please ensure that they were"
            " generated with the same code version as the parsing code"
        )
    return banks


def parse_svdbank_string(bank_string):
    """
    parses strings of form

    H1:bank1.xml,H2:bank2.xml,L1:bank3.xml

    into a dictionary of lists of bank files.
    """
    out = {}
    if bank_string is None:
        return out
    for b in bank_string.split(","):
        ifo, bank = b.split(":")
        if ifo in out:
            raise ValueError("Only one svd bank per instrument should be given")
        out[ifo] = bank
    return out


def calc_lambda_eta_sum(auto_correlation):
    acl = len(auto_correlation)
    norm_chisq = sum(2 - 2 * abs(auto_correlation) ** 2)
    center_ind = int((acl - 1) / 2.0)
    covmat_size = acl

    # the following calculation of the covariance matrix is based on Eqs.(45,
    # 46) in the technical notes of DCC-G2200635.
    covmat_cplx = numpy.zeros((covmat_size, covmat_size), dtype=numpy.complex128)
    for j, row in enumerate(covmat_cplx):
        row_start = max(j - center_ind, 0)
        row_end = min(j - center_ind + acl, covmat_size)
        R_start = max(center_ind - j, 0)
        R_end = min(center_ind - j + acl, covmat_size)
        row[row_start:row_end] += auto_correlation[R_start:R_end]
    covmat_cplx -= numpy.outer(auto_correlation.conj(), auto_correlation)
    covmat_real = numpy.vstack(
        [
            numpy.hstack([covmat_cplx.real, covmat_cplx.imag]),
            numpy.hstack([-covmat_cplx.imag, covmat_cplx.real]),
        ]
    )
    auto_correlation_real = numpy.hstack([auto_correlation.real, auto_correlation.imag])
    # the following calculation is based on Eqs.(69)- (72) in the technical
    # notes of DCC-G2200635.
    lambda_sum = norm_chisq
    lambdasq_sum = numpy.trace(covmat_real.dot(covmat_real))
    lambda_etasq_sum = sum(auto_correlation_real**2)
    lambdasq_etasq_sum = auto_correlation_real.T.dot(
        covmat_real.dot(auto_correlation_real)
    )
    return lambda_sum, lambdasq_sum, lambda_etasq_sum, lambdasq_etasq_sum
