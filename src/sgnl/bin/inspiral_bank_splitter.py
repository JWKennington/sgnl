"""An executable to split template banks for singular value decomposition."""

# Copyright (C) 2010 Melissa Frei
# Copyright (C) 2012 Stephen Privitera
# Copyright (C) 2011-2014 Chad Hanna
# Copyright (C) 2025 Cody Messick

import itertools
import json
import math
import os
from argparse import ArgumentParser

import lal
import numpy
import scipy
from ligo.lw import ligolw, lsctables
from ligo.lw import utils as ligolw_utils
from ligo.lw.utils import process as ligolw_process

from sgnl import chirptime, spawaveform, svd_bank, templates
from sgnl.psd import HorizonDistance, harmonic_mean, read_psd


class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass


lsctables.use_in(LIGOLWContentHandler)


def T050017_filename(instruments, description, seg, extension, path=None):
    """
    A function to generate a T050017 filename.
    """
    if not isinstance(instruments, str):
        instruments = "".join(sorted(list(instruments)))
    start, end = seg
    start = int(math.floor(start))
    try:
        duration = int(math.ceil(end)) - start
    # FIXME this is not a good way of handling this...
    except OverflowError:
        duration = 2000000000
    extension = extension.strip(".")
    if path is not None:
        return "%s/%s-%s-%d-%d.%s" % (
            path,
            instruments,
            description,
            start,
            duration,
            extension,
        )
    else:
        return "%s-%s-%d-%d.%s" % (instruments, description, start, duration, extension)


def calc_mu(mass1, mass2, spin1z, spin2z, mu="mu1"):
    """
    Calculate the first orthogonal PN phase coefficient
    see https://arxiv.org/abs/2007.09108
    """

    M = mass1 + mass2
    mchirp = (mass1 * mass2) ** 0.6 / M**0.2
    eta = mass1 * mass2 / M**2
    beta = (
        (113.0 * (mass1 / M) ** 2 + 75.0 * eta) * spin1z
        + (113.0 * (mass2 / M) ** 2 + 75.0 * eta) * spin2z
    ) / 12.0

    # the reference frequency below is taken from the literature. Please note
    # that the coefficients in the resultant linear combination depend on the
    # fref.
    fref = 200
    norm = lal.G_SI * lal.MSUN_SI / lal.C_SI**3
    v = numpy.pi * mchirp * fref * norm
    psi0 = 3.0 / 4 / (8 * v) ** (5.0 / 3)
    psi2 = (
        20.0 / 9 * (743.0 / 336 + 11.0 / 4 * eta) / eta ** (0.4) * v ** (2.0 / 3) * psi0
    )
    psi3 = (4 * beta - 16 * numpy.pi) / eta**0.6 * v * psi0

    # FIXME : the following linear combinations are taken from the ones in the
    # paper above, but this will need to be re-computed with o4 representitive
    # psd.
    if mu == "mu1":
        return 0.974 * psi0 + 0.209 * psi2 + 0.0840 * psi3
    elif mu == "mu2":
        return -0.221 * psi0 + 0.823 * psi2 + 0.524 * psi3
    else:
        raise ValueError("%s is not implemented, so cannnot be computed." % mu)


def group_templates(templates, n, overlap=0):
    """
    break up the template table into sub tables of length n with overlap
    overlap.  n must be less than the number of templates and overlap must be less
    than n
    """
    if n >= len(templates):
        yield templates
    else:
        n = len(templates) / round(len(templates) / float(n))
        assert n >= 1
        for i in itertools.count():
            start = int(round(i * n)) - overlap // 2
            end = int(round((i + 1) * n)) + overlap // 2
            yield templates[max(start, 0) : end]
            if end >= len(templates):
                break


def parse_command_line():
    parser = ArgumentParser()
    parser.add_argument(
        "--output-path",
        metavar="path",
        default=".",
        help="Set the path to the directory where output files will be "
        "written.  Default is '.'.",
    )
    parser.add_argument(
        "--stats-file",
        metavar="file",
        help="Set the path where the SVD metadata (stats) file will be "
        "written.  If the file exists already, new SVD metadata will be "
        "appended to it and the new SVD bin numbers will take the old SVD "
        "bins into account. Required",
    )
    parser.add_argument(
        "--bank-name",
        metavar="name",
        help="If specified, set the name of the template bank being split. "
        "Used to track metadata when splitting multiple template banks, and "
        "when providing the same SVD metadata file across multiple template "
        "banks, will create unique SVD bin numbers across all template banks.",
    )
    parser.add_argument(
        "--output-full-bank-file",
        metavar="path",
        help="Set the path to output the bank.",
    )
    parser.add_argument(
        "--n",
        metavar="count",
        type=int,
        help="Set the number of templates per output file (required). It will "
        "be rounded to make all sub banks approximately the same size.",
    )
    parser.add_argument(
        "--overlap",
        default=0,
        metavar="count",
        type=int,
        help="overlap the templates in each file by this amount, must be even",
    )
    parser.add_argument(
        "--sort-by",
        metavar="column",
        default="mchirp",
        help="Select the template sort column, default mchirp",
    )
    parser.add_argument(
        "--f-final", metavar="float", type=float, help="f_final to populate table with"
    )
    parser.add_argument(
        "--instrument",
        metavar="ifo",
        type=str,
        help="override the instrument, required",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Be verbose.")
    parser.add_argument(
        "--psd-xml",
        type=str,
        help="Specify a PSD to use for computing template bandwidth. Required "
        "if --sort-by=bandwidth or needing bandwidth and/or horizon distance "
        "metadata.",
    )
    parser.add_argument(
        "--approximant",
        type=str,
        action="append",
        help="Must specify an approximant given as mchirp_min:mchirp_max:string",
    )
    parser.add_argument(
        "--f-low",
        type=float,
        metavar="frequency",
        help="Lower frequency cutoff. Required",
    )
    parser.add_argument(
        "--group-by-chi",
        type=int,
        metavar="N",
        default=1,
        help="group templates into N groups of chi - helps with SVD. Default 1",
    )
    parser.add_argument(
        "--group-by-mu",
        type=int,
        metavar="N",
        default=20,
        help="group templates into N groups of mu2, one of the orthogonalized"
        " PN-phase coefficients, to help with SVD. Default 20",
    )
    parser.add_argument(
        "--num-banks",
        metavar="str",
        help="The number of parallel subbanks per SVD bank. More than 2 "
        "sub-banks is not recommended and return an error. Setting "
        "--num-banks-force forces the program to run through. Can be given "
        "as a list like 1,2,3,4 then it will split up the bank into N groups "
        "with M banks each. (required)",
    )  # FIXME The second half of this help message is incomprehensible
    parser.add_argument(
        "--num-banks-force",
        action="store_true",
        help="Set if you intentially set --num-banks larger than 2.",
    )
    parser.add_argument(
        "bank",
        metavar="Template Bank",
        help="Path to template bank to split.",
    )
    arguments = parser.parse_args()

    required_arguments = (
        "n",
        "instrument",
        "sort_by",
        "approximant",
        "f_low",
        "num_banks",
        "stats_file",
    )
    missing_arguments = [
        argument
        for argument in required_arguments
        if getattr(arguments, argument) is None
    ]
    if missing_arguments:
        raise ValueError(
            "missing required argument(s) %s"
            % ", ".join(
                "--%s" % argument.replace("_", "-") for argument in missing_arguments
            )
        )

    if arguments.overlap % 2:
        raise ValueError("overlap must be even")

    if arguments.overlap > arguments.n:
        raise ValueError("--overlap must be small than --n")

    if arguments.sort_by == "bandwidth" and not arguments.psd_xml:
        raise ValueError("must specify psd-xml if sort-by is bandwidth")

    approximants = []
    for appx in arguments.approximant:
        mn, mx, appxstring = appx.split(":")
        approximants.append((float(mn), float(mx), appxstring))

    if arguments.num_banks:
        num_banks_str = arguments.num_banks
        arguments.num_banks = [int(v) for v in arguments.num_banks.split(",")]
    if max(arguments.num_banks) > 2 and not arguments.num_banks_force:
        raise ValueError(
            "--num-banks cannot be larger than 2, while you set "
            "--num-banks=%s. If you know what you are doing, set "
            "--num-banks-force to avoid this error message." % num_banks_str
        )

    if arguments.psd_xml:
        psd = harmonic_mean(read_psd(arguments.psd_xml, verbose=arguments.verbose))
        f = numpy.arange(len(psd.data.data)) * psd.deltaF
        psdinterp = scipy.interpolate.interp1d(f, psd.data.data)
    else:
        psd = None
        psdinterp = None
    return arguments, psd, psdinterp, approximants


def assign_approximant(mchirp, approximants):
    for lo, hi, appx in approximants:
        if lo <= mchirp < hi:
            return appx
    raise ValueError("Valid approximant not given for this chirp mass")


def split_approximant_strings(approximant_strs):
    approximants = []
    for appx in approximant_strs:
        mn, mx, appxstring = appx.split(":")
        approximants.append((float(mn), float(mx), appxstring))
    return approximants


def split_bank(
    bank_name,
    stats_file,
    filename=None,
    verbose=None,
    f_final=None,
    psd_xml=None,
    f_low=None,
    psdinterp=None,
    psd=None,
    output_full_bank_file=None,
    sort_by=None,
    group_by_mu=None,
    group_by_chi=None,
    n=None,
    overlap: int = 0,
    num_banks=None,
    instrument=None,
    output_path=None,
    approximants=None,
    argument_dict=None,
):

    # make sure output_path exists
    os.makedirs(output_path, exist_ok=True)

    # load or generate svd metadata
    if os.path.exists(stats_file):
        with open(stats_file, "r") as f:
            svd_metadata = json.load(f)
    else:
        svd_metadata = {"banks": {}}

    if bank_name:
        if bank_name in svd_metadata["banks"]:
            raise KeyError(f"bank name {bank_name} is not unique in SVD metadata")

    # determine bin start
    if "bins" in svd_metadata:
        svd_bin_start = int(sorted(list(svd_metadata["bins"].keys()))[-1])
        split_bin_start = 0
        for bank_name in svd_metadata["banks"].keys():
            split_bin_start += svd_metadata["banks"][bank_name]["num_banks"]
    else:
        svd_metadata["bins"] = {}
        svd_bin_start = split_bin_start = 0

    # add bank name metadata
    if bank_name:
        svd_metadata["banks"][bank_name] = {}

    # load template bank
    xmldoc = ligolw_utils.load_filename(
        filename, verbose=verbose, contenthandler=LIGOLWContentHandler
    )
    sngl_inspiral_table = lsctables.SnglInspiralTable.get_table(xmldoc)
    temp_id_input = set(row.template_id for row in sngl_inspiral_table)
    if not len(temp_id_input) == len(sngl_inspiral_table):
        raise ValueError(
            "There are duplicated template ids in the entire sngl_inspiral_table"
        )
    temp_id_output = set()
    # Add a fake bandwidth column
    for row in sngl_inspiral_table:
        # FIXME don't hard code
        row.f_final = f_final
        if psd_xml:
            row.bandwidth = templates.bandwidth(
                row.mass1,
                row.mass2,
                row.spin1z,
                row.spin2z,
                f_min=f_low,
                f_max=row.f_final,
                delta_f=0.25,
                psd=psdinterp,
            )
            row.horizon = HorizonDistance(
                f_low,
                row.f_final,
                0.25,
                row.mass1,
                row.mass2,
                (0.0, 0.0, row.spin1z),
                (0.0, 0.0, row.spin2z),
            )(psd)[0]

    process = ligolw_process.register_to_xmldoc(
        xmldoc,
        program="sgnl_bank_splitter",
        paramdict=argument_dict,
        comment="Assign template IDs",
    )
    if output_full_bank_file is not None:
        ligolw_utils.write_filename(xmldoc, output_full_bank_file, verbose=verbose)

    banks_subbins = []

    if sort_by == "mu":
        # First partitioning by mu2
        sngl_inspiral_table.sort(
            key=lambda row: calc_mu(
                row.mass1, row.mass2, row.spin1z, row.spin2z, mu="mu2"
            )
        )
        num_group = group_by_mu
    else:
        # First partitioning by chi
        sngl_inspiral_table.sort(
            key=lambda row: spawaveform.computechi(
                row.mass1, row.mass2, row.spin1z, row.spin2z
            )
        )
        num_group = group_by_chi

    for chirows in group_templates(
        sngl_inspiral_table, len(sngl_inspiral_table) / num_group, overlap=0
    ):
        outputrows = []

        def sort_func(row, column=sort_by):
            if column == "mu":
                return calc_mu(row.mass1, row.mass2, row.spin1z, row.spin2z, mu="mu1")
            else:
                return getattr(row, column)

        chirows.sort(key=sort_func, reverse=sort_by in ["template_duration", "mu"])

        for numrow, rows in enumerate(group_templates(chirows, n, overlap)):
            assert len(rows) >= n / 2, (
                "There are too few templates in this chi interval.  Requested"
                " %d: have %d" % (n, len(rows))
            )
            # Pad the first group with an extra overlap / 2 templates
            if numrow == 0:
                rows = rows[: overlap // 2] + rows
            outputrows.append((rows[0], rows))
        # Pad the last group with an extra overlap / 2 templates
        outputrows[-1] = (
            (rows[0], rows + rows[-overlap // 2 :]) if overlap else (rows[0], rows)
        )
        banks_subbins += [outputrows]

    svd_groups = []
    metadata = {}
    name_num_banks = 0
    for outputrows in banks_subbins:
        for ind, (first_row, rows) in enumerate(outputrows):
            # just choose the first row to get mchirp
            # FIXME this could certainly be better
            approximant = assign_approximant(
                first_row.mchirp, approximants=approximants
            )
            for row in rows:
                # Record the conservative template duration
                row.template_duration = chirptime.imr_time(
                    f_low,
                    lal.MSUN_SI * row.mass1,
                    lal.MSUN_SI * row.mass2,
                    numpy.dot(row.spin1, row.spin1) ** 0.5,
                    numpy.dot(row.spin2, row.spin2) ** 0.5,
                    f_max=min(
                        row.f_final,
                        (
                            2
                            * chirptime.ringf(
                                lal.MSUN_SI * row.mass1 + lal.MSUN_SI * row.mass2,
                                chirptime.overestimate_j_from_chi(
                                    max(
                                        numpy.dot(row.spin1, row.spin1) ** 0.5,
                                        numpy.dot(row.spin2, row.spin2) ** 0.5,
                                    )
                                ),
                            )
                            if approximant in templates.sgnl_IMR_approximants
                            else spawaveform.ffinal(row.mass1, row.mass2, "bkl_isco")
                        ),
                    ),
                )
                # template_duration may be overwritten by svd_bank if the flow
                # is increased in order to satisfy the target maximum duration
            outputrows[ind] = (approximant, rows)
        name_num_banks += len(outputrows)
        svd_groups += list(svd_bank.group(outputrows, num_banks))

    if bank_name:
        svd_metadata["banks"][bank_name]["num_banks"] = name_num_banks

    def sort_svd(svd_group, column=sort_by):
        if column == "mu":
            return calc_mu(
                svd_group[0][1][0].mass1,
                svd_group[0][1][0].mass2,
                svd_group[0][1][0].spin1z,
                svd_group[0][1][0].spin2z,
                mu="mu1",
            )
        else:
            return getattr(svd_group[0][1][0], column)

    svd_groups.sort(key=sort_svd, reverse=sort_by in ["template_duration", "mu"])

    for n, svd in enumerate(svd_groups, start=svd_bin_start):
        svd_mchirps = []
        svd_mtotals = []
        svd_etas = []
        svd_mratios = []
        svd_durs = []
        svd_bw = []
        svd_mu1 = []
        clipleft = overlap // 2
        clipright = overlap // 2
        for m, (approximant, rows) in enumerate(svd, start=split_bin_start):
            # Make an output document
            xmldoc = ligolw.Document()
            lw = xmldoc.appendChild(ligolw.LIGO_LW())
            sngl_inspiral_table = lsctables.New(lsctables.SnglInspiralTable)
            lw.appendChild(sngl_inspiral_table)
            paramdict = dict(
                argument_dict,
                **{
                    "clipleft": clipleft,
                    "clipright": clipright,
                    "bank-id": "%d_%d" % (n, m),
                },
            )
            # overwrite approximant to store in process table
            paramdict["approximant"] = approximant
            process = ligolw_process.register_to_xmldoc(
                xmldoc,
                program="sgnl-inspiral-bank-splitter",
                paramdict=paramdict,
                comment="Split bank into smaller banks for SVD",
            )
            for row in rows:
                row.process_id = process.process_id
                # Make sure ifo and total mass is stored
                row.ifo = instrument
                row.mtotal = row.mass1 + row.mass2
                row.eta = row.mass1 * row.mass2 / row.mtotal**2

            clipped_rows = rows[clipleft:-clipright] if int(clipleft) else rows
            temp_id_splitbank = set(row.template_id for row in clipped_rows)
            if not len(temp_id_splitbank) == len(clipped_rows):
                raise ValueError(
                    "there are duplicated rows in %d-th split bank of %d-th svd bin"
                    % (m, n)
                )
            if temp_id_output & temp_id_splitbank:
                raise ValueError(
                    "there is overlap templates after clipping in %d-th split "
                    "bank of %d-th svd bin" % (m, n)
                )
            else:
                temp_id_output |= temp_id_splitbank
            svd_mchirps.extend([r.mchirp for r in clipped_rows])
            svd_mtotals.extend([r.mtotal for r in clipped_rows])
            svd_mratios.extend([r.mass1 / r.mass2 for r in clipped_rows])
            svd_etas.extend([r.eta for r in clipped_rows])
            svd_durs.extend([r.template_duration for r in clipped_rows])
            if sort_by == "mu":
                svd_mu1.extend(
                    [
                        calc_mu(r.mass1, r.mass2, r.spin1z, r.spin2z, mu="mu1")
                        for r in clipped_rows
                    ]
                )
            if psd_xml:
                svd_bw.extend([r.bandwidth for r in clipped_rows])
            sngl_inspiral_table[:] = rows
            output = T050017_filename(
                instrument,
                f"{n:04d}_SGNL_SPLIT_BANK_{m:04d}",
                (0, 0),
                ".xml.gz",
                path=output_path,
            )
            ligolw_utils.write_filename(xmldoc, output, verbose=verbose)
        split_bin_start += len(svd)

        bin_metadata = {
            "mean_mchirp": numpy.mean(svd_mchirps),
            "median_mchirp": numpy.median(svd_mchirps),
            "min_mchirp": numpy.min(svd_mchirps),
            "max_mchirp": numpy.max(svd_mchirps),
            "mean_mtotal": numpy.mean(svd_mtotals),
            "median_mtotal": numpy.median(svd_mtotals),
            "min_mtotal": numpy.min(svd_mtotals),
            "max_mtotal": numpy.max(svd_mtotals),
            "mean_eta": numpy.mean(svd_etas),
            "median_eta": numpy.median(svd_etas),
            "min_eta": numpy.min(svd_etas),
            "max_eta": numpy.max(svd_etas),
            "mean_mratio": numpy.mean(svd_mratios),
            "median_mratio": numpy.median(svd_mratios),
            "min_mratio": numpy.min(svd_mratios),
            "max_mratio": numpy.max(svd_mratios),
            "mean_dur": numpy.mean(svd_durs),
            "median_dur": numpy.median(svd_durs),
            "min_dur": numpy.min(svd_durs),
            "max_dur": numpy.max(svd_durs),
            "f_final": f_final,
        }
        if psd_xml:
            # some gymnastics to get the fiducial horizon template in the LR calculation
            rows_fb = svd[0][1][clipleft:-clipright]
            for _, _rows in svd[1:]:
                rows_fb += clipped_rows

            class fakebank(object):
                def __init__(self, sngl_inspiral_table=rows_fb):
                    self.sngl_inspiral_table = sngl_inspiral_table

            fb = fakebank()
            tid, _, _, _, _ = svd_bank.preferred_horizon_distance_template([fb])
            ref_hd = [
                r.horizon
                for (_, rows) in svd
                for r in rows[clipleft:-clipright]
                if r.template_id == tid
            ][0]

            bin_metadata["min_bw"] = numpy.min(svd_bw)
            bin_metadata["max_bw"] = numpy.max(svd_bw)
            bin_metadata["horizon_factors"] = dict(
                (r.template_id, r.horizon / ref_hd)
                for m, split_file in enumerate(svd)
                for r in metadata[split_file][clipleft:-clipright]
            )
        if sort_by == "mu":
            bin_metadata["median_mu1"] = numpy.median(svd_mu1)
            bin_metadata["min_mu1"] = numpy.min(svd_mu1)
            bin_metadata["max_mu1"] = numpy.max(svd_mu1)
        if bank_name:
            bin_metadata["bank_name"] = bank_name

        svd_metadata["bins"]["%04d" % n] = bin_metadata

    if temp_id_input != temp_id_output:
        raise ValueError(
            "input template ids is not consistent with output template ids."
        )
    # with open(datafind.T050017_filename(options.instrument,
    # "%04d_%04d_SGNL_INSPIRAL_SVD_BANK_METADATA" % (0, n), (0, 0), ".json",
    # path = options.output_path), "w") as jsf:
    with open(stats_file, "w") as jsf:
        jsf.write(json.dumps(svd_metadata, sort_keys=True, indent=4))


def main():
    arguments, psd, psdinterp, approximants = parse_command_line()
    split_bank(
        bank_name=arguments.bank_name,
        stats_file=arguments.stats_file,
        filename=arguments.bank,
        verbose=arguments.verbose,
        f_final=arguments.f_final,
        psd_xml=arguments.psd_xml,
        f_low=arguments.f_low,
        psdinterp=psdinterp,
        psd=psd,
        output_full_bank_file=arguments.output_full_bank_file,
        sort_by=arguments.sort_by,
        group_by_mu=arguments.group_by_mu,
        group_by_chi=arguments.group_by_chi,
        n=arguments.n,
        overlap=arguments.overlap,
        num_banks=arguments.num_banks,
        instrument=arguments.instrument,
        output_path=arguments.output_path,
        approximants=approximants,
        argument_dict=arguments.__dict__,
    )


if __name__ == "__main__":
    main()
