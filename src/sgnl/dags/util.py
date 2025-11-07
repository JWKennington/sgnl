# Copyright (C) 2010  Kipp Cannon (kipp.cannon@ligo.org)
# Copyright (C) 2010  Chad Hanna (chad.hanna@ligo.org)
# Copyright (C) 2020  Patrick Godwin (patrick.godwin@ligo.org)
# Copyright (C) 2024  Cort Posnansky (cort.posnansky@ligo.org)

import collections
import glob
import itertools
import json
import math
import os
from dataclasses import dataclass, field
from enum import Enum

from ezdag import Option
from igwn_segments import segment, segmentlist, segmentlistdict
from lal.utils import CacheEntry

DEFAULT_BACKUP_DIR = "backup"


class DotDict(dict):
    """
    A dictionary supporting dot notation.
    """

    __getattr__ = dict.get  # type: ignore[assignment]
    __setattr__ = dict.__setitem__  # type: ignore[assignment]
    __delattr__ = dict.__delitem__  # type: ignore[assignment]

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


def condor_scratch_space():
    """!
    A way to standardize the condor scratch space even if it changes
    >>> condor_scratch_space()
    '_CONDOR_SCRATCH_DIR'
    """
    return "_CONDOR_SCRATCH_DIR"


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
        for ifo, time_bin in time_bins.items():
            for span in time_bin:
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
        cache.sort(key=lambda cache_entry: cache_entry.observatory)
        cache.sort(key=lambda cache_entry: cache_entry.description)
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
    def description(self, svd_bin=None, subtype=None, sgnl=True):
        # FIXME: sanity check subtype input
        description = []
        if svd_bin:
            description.append(svd_bin)
        # FIXME: add SGNL in name once we don't depend on GSTLAL anymore
        description.append(f"SGNL_{self.name}")
        # description.append(f"{self.name}")
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
    LR_TRIGGERS = (12, "sqlite.gz")
    FAR_TRIGGERS = (13, "sqlite.gz")
    SEGMENTS_FAR_TRIGGERS = (14, "sqlite.gz")
    LIKELIHOOD_RATIO = (20, "xml.gz")
    PRIOR_LIKELIHOOD_RATIO = (21, "xml.gz")
    MARG_LIKELIHOOD_RATIO = (22, "xml.gz")
    MARG_LIKELIHOOD_RATIO_PRIOR = (23, "xml.gz")
    RANK_STAT_PDFS = (30, "xml.gz")
    POST_RANK_STAT_PDFS = (31, "xml.gz")
    ZEROLAG_RANK_STAT_PDFS = (32, "xml.gz")
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


def estimate_osdf_frame_GB(uri_list):
    """
    Given a list of OSDF frame file URIs, return the estimated total file
    size as a float in units of gigabytes.
    """
    # Hardcode file size since we don't have access to pelican for OSDF
    # metadata. The following values were determined from O4b and O4c frames.
    HL_frame_file_size_GB = 1.6
    V_frame_file_size_GB = 0.11
    inj_frame_file_size_GB = 0.15
    # GWOSC size determined from O4a frames
    gwosc_frame_file_size_GB = 0.5

    # FIXME Assume gwosc frames don't include V1. Hardcoding to O4a directory
    # in the hopes that by the time we need O4b this code will be replaced with
    # something better
    num_gwosc_frames = sum(f.startswith("osdf:///gwdata/O4a") for f in uri_list)
    num_HL_frames = sum(f.startswith("osdf:///igwn/ligo") for f in uri_list)
    num_V_frames = sum(f.startswith("osdf:///igwn/virgo") for f in uri_list)
    num_inj_frames = sum(f.startswith("osdf:///igwn/shared") for f in uri_list)
    if len(uri_list) > num_gwosc_frames + num_HL_frames + num_V_frames + num_inj_frames:
        raise ValueError("Couldn't estimate OSDF frame file size")

    assert not ((num_gwosc_frames > 0) and (num_HL_frames > 0 or num_V_frames > 0))

    total_size_GB = (
        num_gwosc_frames * gwosc_frame_file_size_GB
        + HL_frame_file_size_GB * num_HL_frames
        + V_frame_file_size_GB * num_V_frames
        + inj_frame_file_size_GB * num_inj_frames
    )
    return total_size_GB


def osdf_flatten_frame_cache(
    input_cache_file, new_frame_dir="/srv", new_cache_name=None
):
    """
    Given a frame cache, write a new cache with any OSDF uris changed to
    {new_frame_dir}/{filename}. If the cache does not contain OSDF uris, don't
    write anything. Return whether the cache contained OSDF uris, and the path
    to the frame cache which should be used.
    """

    # Process inputs
    if not os.path.isabs(new_frame_dir):
        raise ValueError("new_frame_dir must be an absolute path")

    input_cache_basename = os.path.basename(input_cache_file)
    if new_cache_name is None:
        output_cache_file = "flat_" + input_cache_basename
    else:
        output_cache_file = new_cache_name

    with open(input_cache_file, "r") as file:
        input_cache = list(map(CacheEntry, file))

    # Loop through cache
    has_osdf_frames = False
    output_cache_lines = []
    for entry in input_cache:
        if entry.scheme == "osdf":
            if not has_osdf_frames:
                has_osdf_frames = True
            filename = os.path.basename(entry.path)
            new_path = os.path.join(new_frame_dir, filename)
            entry.url = "file://localhost" + new_path
        output_cache_lines.append(str(entry) + "\n")

    # Write file and return
    if has_osdf_frames:
        with open(output_cache_file, "w") as file:
            file.writelines(output_cache_lines)
        return True, output_cache_file
    else:
        return False, input_cache_file


def lookup_osdf_frames_for_segment(frame_cache_entries, start, end):
    """
    Filter pre-loaded cache entries for a time segment and return OSDF frame info.

    Args:
        frame_cache_entries: List of CacheEntry objects (already parsed from file)
        start: Job start time
        end: Job end time

    Returns:
        tuple: (osdf_frame_uris list, size_GB float)
    """
    seg = segment(start, end)

    # Filter entries that intersect with this segment and are OSDF
    osdf_entries = [
        entry
        for entry in frame_cache_entries
        if seg.intersects(entry.segment) and entry.scheme == "osdf"
    ]

    if not osdf_entries:
        return [], 0.0

    osdf_frame_uris = DataCache(DataType.FRAMES, osdf_entries).urls
    size_GB = estimate_osdf_frame_GB(osdf_frame_uris)
    return osdf_frame_uris, size_GB


def add_osdf_frames_to_node(
    node, osdf_frame_uris, transfer_cache_file, cache_option_name, osdf_option_name
):
    """
    Modify a node's inputs to use OSDF frames.

    Args:
        node: The ezdag Node to modify
        osdf_frame_uris: List of OSDF frame URIs for this node
        transfer_cache_file: Path to flattened cache file for HTCondor transfer
        cache_option_name: Name of the cache option to replace (e.g., "frame-cache")
        osdf_option_name: Name of the OSDF option to add (e.g., "osdf-frame-files")
    """
    # Find and delete original frame cache option
    cache_position = None
    for j, node_input in enumerate(node.inputs):
        if node_input.name == cache_option_name:
            cache_position = j
            del node.inputs[j]
            break

    if cache_position is not None:
        # Insert at the same position to maintain input order across nodes
        node.inputs.insert(
            cache_position, Option(cache_option_name, transfer_cache_file, track=False)
        )
        node.inputs.insert(
            cache_position + 1,
            Option(osdf_option_name, osdf_frame_uris, track=False, suppress=True),
        )


def adjust_layer_disk_for_osdf(layer, *osdf_sizes_GB):
    """
    Adjust layer disk request to account for OSDF frame files.

    Args:
        layer: The ezdag Layer to adjust
        *osdf_sizes_GB: Variable number of OSDF size estimates in GB
    """
    total_osdf_size_GB = sum(osdf_sizes_GB)
    if total_osdf_size_GB == 0:
        return

    request_disk = layer.submit_description["request_disk"]
    if not (isinstance(request_disk, str) and request_disk.endswith("GB")):
        raise ValueError(
            f"The default request_disk should be in GB. "
            f"Current value: {request_disk}"
        )

    request_disk_GB = float(request_disk.rstrip(" GB"))
    new_request_disk = str(round(request_disk_GB + total_osdf_size_GB, 1)) + "GB"
    layer.submit_description["request_disk"] = new_request_disk


def add_osdf_support_to_layer(layer, source_config, is_injection_workflow=False):
    """
    Add OSDF frame support to a layer by modifying its nodes and disk
    requirements.

    Call this immediately after creating a layer that uses frame data.
    This keeps all OSDF-specific code in dagger.py/util.py rather than
    spread across layers.

    Args:
        layer: The ezdag Layer to modify
        source_config: Source configuration with frame cache info
        is_injection_workflow: If True, also handle injection frame caches

    Example:
        layer = layers.reference_psd(...)
        util.add_osdf_support_to_layer(layer, config.source)
        dag.attach(layer)
    """
    if not (source_config and source_config.frame_cache):
        return

    # Read and parse cache files once (if using OSDF)
    frame_cache_entries = None
    inj_cache_entries = None

    if source_config.frames_in_osdf:
        with open(source_config.frame_cache, "r") as f:
            frame_cache_entries = [CacheEntry(line) for line in f]

    if is_injection_workflow and source_config.inj_frames_in_osdf:
        with open(source_config.inj_frame_cache, "r") as f:
            inj_cache_entries = [CacheEntry(line) for line in f]

    # Create lookup caches for (start, end) -> (uris, size)
    osdf_lookup_cache = {}
    inj_osdf_lookup_cache = {}

    osdf_frame_max_size_GB = 0
    osdf_inj_frame_max_size_GB = 0

    # Iterate through all nodes in the layer
    for node in layer.nodes:
        # Extract start/end times from node's arguments
        start = None
        end = None
        for arg in node.arguments:
            if arg.name == "gps-start-time":
                start = int(arg.argument[0])
            elif arg.name == "gps-end-time":
                end = int(arg.argument[0])

        if start is None or end is None:
            raise ValueError(
                f"Could not find gps-start-time and gps-end-time "
                f"in node arguments. "
                f"Found: {[arg.name for arg in node.arguments]}"
            )

        # Handle regular frames
        if frame_cache_entries is not None:
            # Lookup or compute OSDF frames for this segment
            if (start, end) not in osdf_lookup_cache:
                osdf_lookup_cache[(start, end)] = lookup_osdf_frames_for_segment(
                    frame_cache_entries, start, end
                )
            osdf_frame_uris, osdf_size = osdf_lookup_cache[(start, end)]

            # Modify node inputs
            add_osdf_frames_to_node(
                node,
                osdf_frame_uris,
                source_config.transfer_frame_cache,
                "frame-cache",
                "osdf-frame-files",
            )
            osdf_frame_max_size_GB = max(osdf_size, osdf_frame_max_size_GB)

        # Handle injection frames
        if inj_cache_entries is not None:
            # Lookup or compute injection OSDF frames for this segment
            if (start, end) not in inj_osdf_lookup_cache:
                inj_osdf_lookup_cache[(start, end)] = lookup_osdf_frames_for_segment(
                    inj_cache_entries, start, end
                )
            osdf_inj_frame_uris, osdf_inj_size = inj_osdf_lookup_cache[(start, end)]

            # Modify node inputs
            add_osdf_frames_to_node(
                node,
                osdf_inj_frame_uris,
                source_config.transfer_inj_frame_cache,
                "noiseless-inj-frame-cache",
                "osdf-inj-frame-files",
            )
            osdf_inj_frame_max_size_GB = max(osdf_inj_size, osdf_inj_frame_max_size_GB)

    # Adjust layer disk requirements
    adjust_layer_disk_for_osdf(
        layer, osdf_frame_max_size_GB, osdf_inj_frame_max_size_GB
    )


def groups(lt, n):
    """!
    Given a list, returns back sublists with a maximum size n.
    """
    for i in range(0, len(lt), n):
        yield lt[i : i + n]
