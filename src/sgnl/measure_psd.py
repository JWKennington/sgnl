"""A module for measuring PSDs."""

# Copyright (C) 2010-2013  Kipp Cannon, Chad Hanna, Leo Singer
# Copyright (C) 2024 Anushka Doke, Yun-Jing Huang, Ryan Magee, Shio Sakon

from __future__ import annotations

import sys

from sgn.apps import Pipeline
from sgnligo.sources import DevShmSource
from sgnligo.transforms import Whiten
from sgnts.sinks import NullSeriesSink
from sgnts.transforms import Resampler

from sgnl.sinks.psd_sink import PSDSink


def measure_psd(
    gw_data_source_info,
    channel_name,
    shared_memory_dir,
    wait_time,
    sample_rate,
    whitening_method,
    reference_psd,
    instrument,
    rate,
    psd_fft_length=8,
    verbose=False,
):
    """
    Measure a power spectral density (PSD) using a pipeline that includes
    data source, resampling, whitening, and PSD sink components.

    Args:
        gw_data_source_info: Source information
        channel_name (str): The detector channel name
        shared_memory_dir (str): Directory for shared memory
        wait_time (float): Time to wait
        sample_rate (float): Target sample rate
        whitening_method (str): Method used for whitening
        reference_psd: Reference PSD for whitening
        instrument (str): Instrument name (e.g., "H1", "L1")
        rate (float): Sample rate
        psd_fft_length (int, optional): FFT length in seconds. Defaults to 8.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Raises:
        ValueError: If the segment is too short for a good PSD measurement

    Returns:
        None
    """
    #
    # 8 FFT-lengths is just a ball-parky estimate of how much data is
    # needed for a good PSD, this isn't a requirement of the code (the
    # code requires a minimum of 1)
    #

    if (
        gw_data_source_info.seg is not None
        and float(abs(gw_data_source_info.seg)) < 8 * psd_fft_length
    ):
        raise ValueError("segment %s too short" % str(gw_data_source_info.seg))

    #
    # build pipeline
    #

    if verbose:
        print(
            "measuring PSD in segment %s" % str(gw_data_source_info.seg),
            file=sys.stderr,
        )
        print("building pipeline ...", file=sys.stderr)

    pipeline = Pipeline()

    #
    #          -----------
    #         | DevShmSource |
    #          -----------
    #         /
    #     H1 /
    #   ------------
    #  |  Resampler |
    #   ------------
    #       |
    #   ------------  hoft ----------
    #  |  Whiten    | --- | NullSink |
    #   ------------       ----------
    #          |psd
    #   ------------
    #  |  PSDSink   |
    #   ------------

    pipeline.insert(
        DevShmSource(
            name="src1",
            source_pad_names=("frsrc",),
            rate=16384,
            num_samples=16384,
            channel_name=channel_name,
            instrument=instrument,
            shared_memory_dir=shared_memory_dir,
            wait_time=wait_time,
        ),
        Resampler(
            name="Resampler",
            source_pad_names=("resamp",),
            sink_pad_names=("frsrc",),
            inrate=16384,
            outrate=sample_rate,
        ),
        Whiten(
            name="Whitener",
            source_pad_names=("hoft", "spectrum"),
            sink_pad_names=("resamp",),
            instrument=instrument,
            sample_rate=sample_rate,
            fft_length=psd_fft_length,
            whitening_method=whitening_method,
            reference_psd=reference_psd,
            psd_pad_name="Whitener:src:spectrum",
        ),
        NullSeriesSink(
            name="HoftSnk",
            sink_pad_names=("hoft",),
            verbose=True,
        ),
        PSDSink(
            fname="test.xml",
            name="PSDSnk",
            sink_pad_names=("spectrum",),
            verbose=True,
        ),
    )

    pipeline.insert(
        link_map={
            "Resampler:snk:frsrc": "src1:src:frsrc",
            "Whitener:snk:resamp": "Resampler:src:resamp",
            "PSDSink:snk:spectrum": "Whitener:src:spectrum",
            "HoftSnk:snk:hoft": "Whitener:src:hoft",
        }
    )

    if verbose:
        print("running pipeline ...", file=sys.stderr)

    pipeline.run()

    if verbose:
        print("PSD measurement complete", file=sys.stderr)
