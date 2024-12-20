# Copyright (C) 2010  Kipp Cannon (kipp.cannon@ligo.org)
# Copyright (C) 2010  Chad Hanna (chad.hanna@ligo.org)
# Copyright (C) 2020  Patrick Godwin (patrick.godwin@ligo.org)
# Copyright (C) 2024  Cort Posnansky (cort.posnansky@ligo.org)
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import collections
import glob
import itertools
import json
import math
import os
from dataclasses import dataclass, field
from enum import Enum

from lal.utils import CacheEntry
from ligo.segments import segment, segmentlist, segmentlistdict

# import sys

# import gwdatafind
# from ligo.lw import utils as ligolw_utils
# from ligo.lw.utils import segments as ligolw_segments


class DotDict(dict):
    """
    A dictionary supporting dot notation.
    """

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = DotDict(v)


def replace_hyphens(dict_, reverse=False):
    """
    Replace hyphens in key names with underscores
    """
    out = dict(dict_)
    for k, v in out.items():
        if isinstance(v, dict):
            out[k] = replace_hyphens(v)
    return {
        k.replace("_", "-") if reverse else k.replace("-", "_"): v
        for k, v in out.items()
    }


def recursive_update(d, u):
    """
    Recursively update a dictionary d from a dictionary u
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = recursive_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def to_ifo_list(instruments):
    """
    Given a string of IFO pairs (e.g. H1L1), return a list of IFOs.
    """
    return [instruments[2 * n : 2 * n + 2] for n in range(len(instruments) // 2)]


def to_ifo_combos(ifos, min_ifos=1):
    """
    Given a list of IFOs, return a list of all possible combinations.
    """
    # all_ifos = frozenset(ifos)
    ifo_combos = []
    for n_ifos in range(min_ifos, len(ifos) + 1):
        for combo in itertools.combinations(ifos, n_ifos):
            ifo_combos.append(frozenset(combo))
    return ifo_combos


def format_ifo_args(ifos, args):
    """
    Given a set of instruments and arguments keyed by instruments, this
    creates a list of strings in the form {ifo}={arg}. This is suitable
    for command line options like --channel-name which expects this
    particular format.
    """
    if isinstance(ifos, str):
        ifos = [ifos]
    return [f"{ifo}={args[ifo]}" for ifo in ifos]


def load_svd_options(option_file, svd_config):
    """
    Loads metadata from an SVD option file.
    """
    with open(option_file, "r") as f:
        svd_stats = DotDict(replace_hyphens(json.load(f)))

    # load default config for sub banks if available
    if "sub_banks" in svd_config:
        reduced_config = svd_config.copy()
        reduced_config.pop("sub_banks")
        for sub_bank, props in svd_config.sub_banks.items():
            svd_config.sub_banks[sub_bank] = DotDict(
                replace_hyphens({**reduced_config, **props})
            )

    # define svd bins, metadata
    svd_bins = svd_stats.bins.keys()

    return svd_bins, svd_stats


def flatten(lst):
    """
    Flatten a list by one level of nesting.
    """
    return list(itertools.chain.from_iterable(lst))


def mchirp_range_to_bins(min_mchirp, max_mchirp, svd_metadata):
    """
    Given a range of chirp masses and the SVD metadata, determine
    and return the SVD bins that overlap.
    """
    svd_bins = []
    mchirp_range = segment(min_mchirp, max_mchirp)
    for svd_bin, bin_metadata in svd_metadata["bins"].items():
        bin_range = segment(bin_metadata["min_mchirp"], bin_metadata["max_mchirp"])

        if mchirp_range.intersects(bin_range):
            svd_bins.append(svd_bin)

    return svd_bins


@dataclass
class DataCache:
    name: "DataType"
    cache: list = field(default_factory=list)

    @property
    def files(self):
        return [entry.path for entry in self.cache]

    @property
    def urls(self):
        urls = []
        for entry in self.cache:
            url = entry.url
            # Work around lal.utils.CacheEntry.from_T050017 removing two slashes
            if (url.startswith("osdf:/") and url[6] != "/") or (
                url.startswith("igwn+osdf:/") and url[11] != "/"
            ):
                url = url.replace("osdf:/", "osdf:///", 1)
            urls.append(url)
        return urls

    def __len__(self):
        return len(self.cache)

    def __add__(self, other):
        assert (
            self.name == other.name
        ), "can't combine two DataCaches with different data types"
        return DataCache(self.name, self.cache + other.cache)

    def chunked(self, chunk_size):
        for i in range(0, len(self), chunk_size):
            yield DataCache(self.name, self.cache[i : i + chunk_size])

    def groupby(self, *group):
        # determine groupby operation
        keyfunc = self._groupby_keyfunc(group)

        # return groups of DataCaches keyed by group
        grouped = collections.defaultdict(list)
        for entry in self.cache:
            grouped[keyfunc(entry)].append(entry)
        return {
            key: DataCache(self.name, cache) for key, cache in sorted(grouped.items())
        }

    def groupby_bins(self, bin_type, bins):
        assert bin_type in set(
            ("time", "segment", "time_bin")
        ), f"bin_type: {bin_type} not supported"

        # return groups of DataCaches keyed by group
        grouped = collections.defaultdict(list)
        for bin_ in bins:
            for entry in self.cache:
                if entry.segment in bin_:
                    grouped[bin_].append(entry)

        return {
            key: DataCache(self.name, cache) for key, cache in sorted(grouped.items())
        }

    def _groupby_keyfunc(self, groups):
        if isinstance(groups, str):
            groups = [groups]

        def keyfunc(key):
            keys = []
            for group in groups:
                if group in set(("ifo", "instrument", "observatory")):
                    keys.append(key.observatory)
                elif group in set(("time", "segment", "time_bin")):
                    keys.append(key.segment)
                elif group in set(("bin", "svd_bin")):
                    keys.append(key.description.split("_")[0])
                elif group in set(("subtype", "tag")):
                    keys.append(
                        key.description.rpartition(f"SGNL_{self.name.name}")[2].lstrip(
                            "_"
                        )
                    )
                elif group in set(("directory", "dirname")):
                    keys.append(os.path.dirname(key.path))
                else:
                    raise ValueError(f"{group} not a valid groupby operation")
            if len(keys) > 1:
                return tuple(keys)
            else:
                return keys[0]

        return keyfunc

    def copy(self, root=None):
        cache_paths = []
        for entry in self.cache:
            filedir = self._data_path(self.name, start=entry.segment[0], root=root)
            filename = os.path.basename(entry.path)
            cache_paths.append(os.path.join(filedir, filename))

        return DataCache.from_files(self.name, cache_paths)

    @classmethod
    def generate(
        cls,
        name,
        ifos,
        time_bins=None,
        svd_bins=None,
        subtype=None,
        extension=None,
        root=None,
        create_dirs=True,
    ):
        # format args
        if isinstance(ifos, str) or isinstance(ifos, frozenset):
            ifos = [ifos]
        if svd_bins and isinstance(svd_bins, str):
            svd_bins = [svd_bins]
        if subtype is None or isinstance(subtype, str):
            subtype = [subtype]

        # format time bins
        if not time_bins:
            time_bins = segmentlistdict(
                {ifo: segmentlist([segment(0, 0)]) for ifo in ifos}
            )
        elif isinstance(time_bins, segment):
            time_bins = segmentlistdict({ifo: segmentlist([time_bins]) for ifo in ifos})
        elif isinstance(time_bins, segmentlist):
            time_bins = segmentlistdict({ifo: time_bins for ifo in ifos})
        else:
            time_bins = segmentlistdict(
                {ifo: time_bins[ifo] for ifo in ifos if ifo in time_bins}
            )

        # generate the cache
        cache = []
        for ifo, time_bins in time_bins.items():
            for span in time_bins:
                path = cls._data_path(
                    name, start=span[0], root=root, create=create_dirs
                )
                if svd_bins:
                    for svd_bin in svd_bins:
                        for stype in subtype:
                            filename = name.filename(
                                ifo,
                                span,
                                svd_bin=svd_bin,
                                subtype=stype,
                                extension=extension,
                            )
                            # FIXME: remove this once we don't depend on GSTLAL input
                            # files
                            filename = filename.replace("GSTLAL", "SGNL")
                            cache.append(os.path.join(path, filename))
                else:
                    for stype in subtype:
                        filename = name.filename(
                            ifo, span, subtype=stype, extension=extension
                        )
                        cache.append(os.path.join(path, filename))

        return cls(name, [CacheEntry.from_T050017(entry) for entry in cache])

    @classmethod
    def find(
        cls,
        name,
        start=None,
        end=None,
        root=None,
        segments=None,
        svd_bins=None,
        extension=None,
        subtype=None,
    ):
        cache = []
        if svd_bins:
            svd_bins = set([svd_bins]) if isinstance(svd_bins, str) else set(svd_bins)
        else:
            svd_bins = [None]
        if subtype is None or isinstance(subtype, str):
            subtype = [subtype]
        for svd_bin in svd_bins:
            for stype in subtype:
                cache.extend(
                    glob.glob(
                        cls._glob_path(name, root, svd_bin, stype, extension=extension)
                    )
                )
                cache.extend(
                    glob.glob(
                        cls._glob_path(
                            name,
                            root,
                            svd_bin,
                            stype,
                            extension=extension,
                            gps_dir=False,
                        )
                    )
                )

        cache = [CacheEntry.from_T050017(entry) for entry in cache]
        if segments:
            cache = [
                entry for entry in cache if segments.intersects_segment(entry.segment)
            ]
        return cls(name, cache)

    @classmethod
    def from_files(cls, name, files):
        if isinstance(files, str):
            files = [files]
        return cls(name, [CacheEntry.from_T050017(entry) for entry in files])

    @staticmethod
    def _data_path(datatype, start=None, root=None, create=True):
        path = datatype.directory(start=start, root=root)
        if create:
            os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _glob_path(
        name, root=None, svd_bin=None, subtype=None, extension=None, gps_dir=True
    ):
        if gps_dir:
            glob_path = os.path.join(
                str(name).lower(),
                "*",
                name.file_pattern(svd_bin, subtype, extension=extension),
            )
        else:
            glob_path = os.path.join(
                str(name).lower(),
                name.file_pattern(svd_bin, subtype, extension=extension),
            )
        if root:
            glob_path = os.path.join(root, glob_path)
        return glob_path


class DataFileMixin:
    def description(self, svd_bin=None, subtype=None):
        # FIXME: sanity check subtype input
        description = []
        if svd_bin:
            description.append(svd_bin)
        description.append(f"GSTLAL_{self.name}")
        if subtype:
            description.append(subtype.upper())
        return "_".join(description)

    def filename(self, ifos, span=None, svd_bin=None, subtype=None, extension=None):
        if not span:
            span = segment(0, 0)
        if not extension:
            extension = self.extension
        return T050017_filename(
            ifos, self.description(svd_bin, subtype), span, extension
        )

    def file_pattern(self, svd_bin=None, subtype=None, extension=None):
        if not extension:
            extension = self.extension
        return f"*-{self.description(svd_bin, subtype)}-*-*{extension}"

    def directory(self, root=None, start=None):
        path = self.name.lower()
        if root:
            path = os.path.join(root, path)
        if start:
            path = os.path.join(path, gps_directory(start))
        return path


class DataType(DataFileMixin, Enum):
    REFERENCE_PSD = (0, "xml.gz")
    MEDIAN_PSD = (1, "xml.gz")
    SMOOTH_PSD = (2, "xml.gz")
    TRIGGERS = (10, "sqlite.gz")
    CLUSTERED_TRIGGERS = (11, "sqlite.gz")
    DIST_STATS = (20, "xml.gz")
    PRIOR_DIST_STATS = (21, "xml.gz")
    MARG_DIST_STATS = (22, "xml.gz")
    DIST_STAT_PDFS = (30, "xml.gz")
    POST_DIST_STAT_PDFS = (31, "xml.gz")
    ZEROLAG_DIST_STAT_PDFS = (32, "xml.gz")
    TEMPLATE_BANK = (40, "xml.gz")
    SPLIT_BANK = (41, "xml.gz")
    SVD_BANK = (42, "xml.gz")
    SVD_MANIFEST = (50, "json")
    MASS_MODEL = (60, "h5")
    FRAMES = (70, "gwf")
    INJECTIONS = (80, "xml")
    SPLIT_INJECTIONS = (81, "xml")
    MATCHED_INJECTIONS = (82, "xml")
    LNLR_SIGNAL_CDF = (90, "pkl")

    def __init__(self, value, extension):
        self.extension = extension

    def __str__(self):
        return self.name.upper()


# def load_frame_cache(start, end, frame_types, host=None):
#    """
#    Given a span and a set of frame types, loads a frame cache.
#    """
#    if not host:
#        host = DEFAULT_DATAFIND_SERVER
#    cache = []
#    with gwdatafind.Session() as sess:
#        for ifo, frame_type in frame_types.items():
#            urls = gwdatafind.find_urls(ifo[0], frame_type, start, end, host=host,
#                                           session=sess)
#            cache.extend([CacheEntry.from_T050017(url) for url in urls])
#
#    return cache
#
#
def gps_directory(gpstime):
    """
    Given a gps time, returns the directory name where files corresponding
    to this time will be written to, e.g. 1234567890 -> '12345'.
    """
    return str(int(gpstime))[:5]


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
