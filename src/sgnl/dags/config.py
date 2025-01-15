# Copyright (C) 2024  Cort Posnansky (cort.posnansky@ligo.org)
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


import getpass
import pathlib

import yaml
from ligo.lw import utils as ligolw_utils
from ligo.lw.utils import segments as ligolw_segments
from ligo.segments import segment, segmentlist, segmentlistdict

from sgnl.dags import segments
from sgnl.dags.util import (
    DotDict,
    recursive_update,
    replace_hyphens,
    to_ifo_combos,
    to_ifo_list,
)


def build_config(config_path, dag_dir):
    # Load default config
    path_to_dags = pathlib.Path(__file__).parent
    default_config_file = path_to_dags / "default_config.yml"
    with open(default_config_file.as_posix(), "r") as file:
        default_config_yaml = yaml.safe_load(file)

    # Handle empty default config
    if default_config_yaml is None:
        default_config_yaml = {}

    default_config = replace_hyphens(default_config_yaml)

    # Load input config
    with open(config_path, "r") as file:
        config_in = replace_hyphens(yaml.safe_load(file))

    # Ensure presence of options required by all dags
    config_in = DotDict(config_in)
    assert config_in.condor, "The config is missing the condor section"
    assert (
        config_in.condor.accounting_group
    ), "The condor section of the config must specify an accounting-group"

    # FIXME uncomment once we have a container
    # assert (
    #     config_in.condor.container
    # ), "The condor section of the config must specify a container"

    # Overwrite default config values with those from the input config
    config = DotDict(recursive_update(default_config, config_in))

    # Set a few more config options derived from inputs
    if config.start:
        config.span = segment(config.start, config.stop)
    else:
        config.span = segment(0, 0)

    config.ifo_list = to_ifo_list(config.instruments)
    config.ifos = config.ifo_list
    config.ifo_combos = to_ifo_combos(config.ifo_list)
    config.all_ifos = frozenset(config.ifos)
    if not config.min_ifos:
        config.min_ifos = 1

    config = create_segments(config)

    if not config.paths:
        config.paths = DotDict({})
    if not config.paths.storage:
        config.paths.storage = dag_dir
    if not config.condor.accounting_group_user:
        config.condor.accounting_group_user = getpass.getuser()

    return config


def create_segments(config):
    # Load segments and create time bins.
    if config.source and config.source.frame_segments_file:
        xmldoc = ligolw_utils.load_filename(
            config.source.frame_segments_file,
            contenthandler=ligolw_segments.LIGOLWContentHandler,
        )
        config.segments = ligolw_segments.segmenttable_get_by_name(
            xmldoc, "datasegments"
        ).coalesce()
    else:
        config.segments = segmentlistdict(
            (ifo, segmentlist([config.span])) for ifo in config.ifos
        )

    if config.span != segment(0, 0):
        config = create_time_bins(
            config, start_pad=512, min_instruments=config.min_ifos
        )

    return config


def create_time_bins(
    config,
    start_pad=512,
    overlap=512,
    min_instruments=1,
    one_ifo_only=False,
    one_ifo_length=(3600 * 8),
):
    config.time_boundaries = segments.split_segments_by_lock(
        config.ifos, config.segments, config.span
    )
    config.time_bins = segmentlistdict()
    if not one_ifo_only:
        for span in config.time_boundaries:
            analysis_segs = segments.analysis_segments(
                config.ifos,
                config.segments,
                span,
                start_pad=start_pad,
                overlap=overlap,
                min_instruments=min_instruments,
                one_ifo_length=one_ifo_length,
            )
            config.time_bins.extend(analysis_segs)
    else:
        for span in config.time_boundaries:
            time_bin = segmentlistdict()
            for ifo, segs in config.segments.items():
                ifo_key = frozenset([ifo])
                segs = segs & segmentlist([span])
                time_bin[ifo_key] = segments.split_segments(
                    segs, one_ifo_length, start_pad
                )
            config.time_bins.extend(time_bin)
    return config
