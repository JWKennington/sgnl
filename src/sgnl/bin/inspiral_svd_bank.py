"""An executable to SVD templates."""

# Copyright (C) 2010  Kipp Cannon, Chad Hanna, Leo Singer
# Copyright (C) 2009  Kipp Cannon, Chad Hanna
# Copyright (C) 2015, 2025 Surabhi Sachdev

from argparse import ArgumentParser

import numpy
from lal.utils import CacheEntry
from ligo.lw import lsctables
from ligo.lw import utils as ligolw_utils

from sgnl import svd_bank
from sgnl.psd import read_psd


def parse_command_line():
    parser = ArgumentParser()
    parser.add_argument(
        "--flow",
        metavar="Hz",
        default=40.0,
        type=float,
        help="Set the template low-frequency cut-off (default = 40.0).",
    )
    parser.add_argument(
        "--sample-rate",
        metavar="Hz",
        type=int,
        help="Set the sample rate. If not set, the sample rate will be based on the "
        "template frequency. The sample rate must be at least twice the highest "
        "frequency in the templates. If provided it must be a power of two",
    )
    parser.add_argument(
        "--padding",
        metavar="pad",
        type=float,
        default=1.5,
        help="Fractional amount to pad time slices.",
    )
    parser.add_argument(
        "--svd-tolerance",
        metavar="match",
        type=float,
        default=0.9999,
        help="Set the SVD reconstruction tolerance (default = 0.9999).",
    )
    parser.add_argument(
        "--reference-psd",
        metavar="filename",
        help="Load the spectrum from this LIGO light-weight XML file (required).",
        required=True,
    )
    parser.add_argument(
        "--instrument-override",
        metavar="ifo",
        help="Override the ifo column of the single inspiral tables",
    )
    parser.add_argument(
        "--template-bank-cache",
        metavar="filename",
        help="Provide a cache file with the names of the LIGO light-weight XML file "
        "from which to load the template bank.",
    )
    parser.add_argument(
        "--write-svd-bank",
        metavar="filename",
        help="Set the filename in which to save the template bank (required).",
        required=True,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Be verbose (optional)."
    )
    parser.add_argument(
        "--autocorrelation-length",
        type=int,
        default=201,
        help="The minimum number of samples to use for auto-chisquared, default 201 "
        "should be odd",
    )
    parser.add_argument(
        "--samples-min",
        type=int,
        default=1024,
        help="The minimum number of samples to use for time slices default 1024",
    )
    parser.add_argument(
        "--samples-max-256",
        type=int,
        default=1024,
        help="The maximum number of samples to use for time slices with frequencies "
        "above 256Hz, default 1024",
    )
    parser.add_argument(
        "--samples-max-64",
        type=int,
        default=2048,
        help="The maximum number of samples to use for time slices with frequencies "
        "between 64Hz and 256 Hz, default 2048",
    )
    parser.add_argument(
        "--samples-max",
        type=int,
        default=4096,
        help="The maximum number of samples to use for time slices with frequencies "
        "below 64Hz, default 4096",
    )
    parser.add_argument(
        "--max-duration",
        metavar="s",
        type=float,
        default=numpy.inf,
        help="The maximum time duration for the waveform to be searched instead of the "
        "minimum frequency",
    )
    parser.add_argument(
        "--template-banks",  # This captures all remaining arguments
        metavar="Input split banks to perform svd upon",
        nargs="*",  # This allows capturing zero or more template bank files
        help="Provide the split bank files to svd.",
    )

    options = parser.parse_args()

    if not options.autocorrelation_length % 2:
        raise ValueError("--autocorrelation-length must be odd")

    if options.sample_rate is not None and (
        not numpy.log2(options.sample_rate) == int(numpy.log2(options.sample_rate))
    ):
        raise ValueError("--sample-rate must be a power of two")

    return options


def extract_subbank_info(banks, verbose=True):
    var = {"clipleft": [], "clipright": [], "bank-id": []}
    for bank in banks:
        bank_xmldoc = ligolw_utils.load_url(
            bank, contenthandler=svd_bank.DefaultContentHandler, verbose=verbose
        )
        process_params = lsctables.ProcessParamsTable.get_table(bank_xmldoc)
        for row in process_params:
            param = row.param.replace("--", "")
            if param in var:
                var[param].append(row.value)
    assert (
        len(var["clipleft"])
        == len(var["clipright"])
        == len(var["bank-id"])
        == len(banks)
    )
    return var["clipleft"], var["clipright"], var["bank-id"]


def main():
    # FIXME
    snr_min = 0

    # parse arguments
    options = parse_command_line()
    if options.template_bank_cache:
        options.template_banks.extend(
            [CacheEntry(line).url for line in open(options.template_bank_cache)]
        )

    cliplefts, cliprights, bank_ids = extract_subbank_info(options.template_banks)
    cliplefts = [int(cl) for cl in cliplefts]
    cliprights = [int(cr) for cr in cliprights]

    psd = read_psd(options.reference_psd, verbose=options.verbose)

    banks = []
    for template_bank, bank_id, clipleft, clipright in zip(
        options.template_banks, bank_ids, cliplefts, cliprights
    ):
        bank = svd_bank.build_bank(
            template_bank,
            psd,
            options.flow,
            options.max_duration,
            options.svd_tolerance,
            clipleft,
            clipright,
            padding=options.padding,
            verbose=options.verbose,
            snr_threshold=snr_min,
            autocorrelation_length=options.autocorrelation_length,
            samples_min=options.samples_min,
            samples_max_256=options.samples_max_256,
            samples_max_64=options.samples_max_64,
            samples_max=options.samples_max,
            bank_id=bank_id,
            contenthandler=svd_bank.DefaultContentHandler,
            sample_rate=options.sample_rate,
            instrument_override=options.instrument_override,
        )
        banks.append(bank)

    process_param_dict = options.__dict__.copy()
    svd_bank.write_bank(options.write_svd_bank, banks, psd, process_param_dict)


if __name__ == "__main__":
    main()
