from argparse import ArgumentParser
import numpy
from lal.utils import CacheEntry

from sgnl.psd import read_psd, write_psd

def parse_command_line():
    parser = ArgumentParser(description = __doc__)

    # add our options
    parser.add_argument(
        "--output-name", 
        metavar = "filename", 
        help = "The output xml file (required)",
    )
    parser.add_argument(
        "--verbose", 
        action = "store_true", 
        help = "Be verbose.",
    )
    parser.add_argument(
        "--input-cache",
        metavar = "filename", 
        help = "Provide a cache file with the names of the LIGO light-weight XML input files *{xml, xml.gz}",
    )
    parser.add_argument(
        "--input-files",
        action = "append",
        nargs='+',
        help = "Provide the file names of the LIGO light-weight XML PSD input files *{xml, xml.gz}",
    )

    options = parser.parse_args()
    filenames = options.input_files[0]
    if options.input_cache:
        filenames += [CacheEntry(line).url for line in open(options.input_cache)]

    return options, filenames 

def parse_and_dump_psd_files(files, verbose=False):
    psd_dict = {}
    for f in files:
        for ifo, psd in read_psd(f, verbose).items():
            if psd is not None:
                psd_dict.setdefault(ifo, []).append(psd)
    psd_out_dict = dict((ifo, psds[0]) for ifo, psds in psd_dict.items())

    return psd_dict, psd_out_dict

def compute_median_psd(psd_dict, psd_out_dict):
    for ifo in psd_dict:
        psd_out_dict[ifo].data.data = numpy.median(numpy.array([psd.data.data for psd in psd_dict[ifo]]), axis=0)

    return psd_out_dict

def main():
    # parse arguments
    options, args = parse_command_line()
    psd_dict, psd_out_dict = parse_and_dump_psd_files(args, options.verbose)
    write_psd(options.output_name, compute_median_psd(psd_dict, psd_out_dict), verbose=options.verbose)

if __name__ == "__main__":
    main()

