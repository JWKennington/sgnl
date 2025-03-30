"""A program to compute the LLOID filtering costs and memory usage of an SVD bank as
produced by sgnl-inspiral-svd-bank.
"""

# Copyright (C) 2014  Chad Hanna, Miguel Fernandez
# Copyright (C) 2025  Yun-Jing Huang

from argparse import ArgumentParser

import h5py
import numpy

from sgnl import svd_bank


def parse_command_line():
    parser = ArgumentParser()

    parser.add_argument(
        "--svd-bank",
        metavar="filename",
        nargs="+",
        required=True,
        help="Set the name of the LIGO light-weight XML file from which to load the "
        "svd bank for a given instrument.  To analyze multiple instruments, --svd-bank "
        "can be called multiple times for svd banks corresponding to different "
        "instruments.  If --data-source is lvshm or framexmit, then only svd banks "
        "corresponding to a single bin must be given. If given multiple times, the "
        "banks will be processed one-by-one, in order.  At least one svd bank for at "
        "least 2 detectors is required, but see also --svd-bank-cache.",
    )
    parser.add_argument(
        "--output-path",
        metavar="filename",
        required=True,
        help="Output filename.",
    )
    parser.add_argument(
        "--memory",
        action="store_true",
        help="Calculate memory usage.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Be verbose.",
    )
    options = parser.parse_args()

    return options


def flopulator(r, rT, UT, MT, NT, verbose=False):
    # Convolution of a shape = (upfactor, 17) filter requires a multiply-add
    # per sample point of data for each sample of the filter for each physical
    # template. Don't upsample the highest sample rate
    kernel = 17
    upfactor = numpy.array([r[i + 1] / ri for i, ri in enumerate(r[:-1])])
    resample = (r[:-1] * upfactor * kernel * 2 * MT).sum()

    # Convolution of a NT sample filter requires a multiply-add per sample
    # point of data for each sample of the filter for each svd template
    filter = (NT * rT * UT * 2).sum()

    reconstruct = (MT * UT * rT * 2).sum()

    add = (rT[:-1] * MT).sum()

    # get FLOPs per *complex* template (note the divide by 2)
    if verbose:
        print("--->\tMFLOPS from resampling: ", resample / 1000.0**2)
        print("--->\tMFLOPS from filtering: ", filter / 1000.0**2)
        print("--->\tMFLOPS from reconstruction: ", reconstruct / 1000.0**2)
        print("--->\tMFLOPS from addition: ", add / 1000.0**2)

    MFLOPs = (
        resample / 1000.0**2
        + filter / 1000.0**2
        + reconstruct / 1000.0**2
        + add / 1000.0**2
    )

    if verbose:
        print("--->\tTotal MFLOPS: ", MFLOPs)
        print("--->\tMFLOPS per complex template: ", MFLOPs / (MT / 2.0))
        print(
            "--->\tRatio number SVD filters to number real input templates: ",
            UT.sum() / MT,
        )

    return MFLOPs


def memulator(r, rT, UT, MT, NT, verbose=False):
    # FIXME: I don't know if these calculations are correct

    # Convolution of a shape = (upfactor, 17) filter requires a multiply-add
    # per sample point of data for each sample of the filter for each physical
    # template. Don't upsample the highest sample rate
    # inputs: (upfactor, 17), (MT, 16 + rate)
    # output: (MT, upfactor * rate)
    kernel = 17
    upfactor = numpy.array([r[i + 1] / ri for i, ri in enumerate(r[:-1])])
    resample = (upfactor * kernel + MT * (16 + r[:-1]) + MT * (upfactor * r[:-1])).sum()

    # Convolution of a NT sample filter requires a multiply-add per sample
    # point of data for each sample of the filter for each svd template
    # inputs: (UT, NT), (rate + NT -1,)
    # outputs: (UT, rate)
    filter = (UT * NT + rT + NT - 1 + UT * rT).sum()

    # inputs: (MT, UT), (UT, rate)
    # outputs: (MT, rate)
    reconstruct = (MT * UT + UT * rT + MT * rT).sum()

    # inputs: (MT, rate), (MT, rate)
    # outputs: (MT, rate)
    add = (3 * MT * rT[:-1]).sum()

    # get FLOPs per *complex* template (note the divide by 2)
    if verbose:
        print("--->\tMB/s from resampling: ", resample * 4 / 1000.0**2)
        print("--->\tMB/s from filtering: ", filter * 4 / 1000.0**2)
        print("--->\tMB/s from reconstruction: ", reconstruct * 4 / 1000.0**2)
        print("--->\tMB/s from addition: ", add * 4 / 1000.0**2)

    MB = (
        resample * 4 / 1000.0**2
        + filter * 4 / 1000.0**2
        + reconstruct * 4 / 1000.0**2
        + add * 4 / 1000.0**2
    )
    if verbose:
        print("--->\tTotal MB/s: ", MB)
        print("--->\tMB/s per complex template: ", MB / (MT / 2.0))
        print(
            "--->\tRatio number SVD filters to number real input templates: ",
            UT.sum() / MT,
        )

    return MB


def main():
    options = parse_command_line()
    hf = h5py.File(options.output_path, "a")
    for f in options.svd_bank:
        banks = svd_bank.read_banks(f, svd_bank.DefaultContentHandler)
        svdbin = banks[0].bank_id.split("_")[0]
        hf.create_group(f"{svdbin}")
        totalMFLOPS = 0
        totalMB = 0
        for i, bank in enumerate(banks):
            g = hf.create_group(f"{svdbin}/subbank-{i}")
            rT = [f.rate for f in bank.bank_fragments]
            r = numpy.array(sorted(list(set(rT))))
            rT = numpy.array(rT)

            UT = numpy.array([f.mix_matrix.shape[0] for f in bank.bank_fragments])
            MT = [f.mix_matrix.shape[1] for f in bank.bank_fragments][0]
            NT = numpy.array([(f.end - f.start) * f.rate for f in bank.bank_fragments])

            if options.verbose:
                print("\nSUB BANK %d" % i)
                print("--->\tUnique sampling rates: ", r)
                print("--->\tSampling rate for a given time slice: ", rT)
                print("--->\tTotal SVD filters for a given time slice: ", UT)
                print("--->\tNumber of SVD filter samples: ", NT)
                print(
                    "--->\tTotal real templates (e.g. twice number of complex "
                    " templates): ",
                    MT,
                )

            g.create_dataset("sample-rates", data=rT)
            g.create_dataset("num-svd-filters", data=UT)
            g.create_dataset("num-real-templates", data=MT)

            totalMFLOPS += flopulator(r, rT, UT, MT, NT, options.verbose)
            if options.memory:
                totalMB += memulator(r, rT, UT, MT, NT, options.verbose)

        g.create_dataset("total-mflops", data=totalMFLOPS)
        print("SVD BIN: %s | Total MFLOPs %f" % (svdbin, totalMFLOPS))
        if options.memory:
            g.create_dataset("total-mbs", data=totalMB)
            print("SVD BIN: %s | Total MB/s %f" % (svdbin, totalMB))
    hf.close()
