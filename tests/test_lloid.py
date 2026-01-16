"""Tests for sgnl.transforms.lloid module."""

import pathlib
from unittest import mock

import torch

from sgnl.sort_bank import SortedBank, group_and_read_banks
from sgnl.transforms.lloid import RESAMPLE_VARIANCE_GAIN, lloid

PATH_DATA = pathlib.Path(__file__).parent / "data"

PATHS_SVD_BANK = [
    PATH_DATA / "H1-0008_SGNL_SVD_BANK-0-0.xml.gz",
    PATH_DATA / "L1-0008_SGNL_SVD_BANK-0-0.xml.gz",
]


class TestLloid:
    """Tests for lloid function."""

    def _create_sorted_bank(self, nslice=-1):
        """Helper to create a SortedBank from test data."""
        banks = group_and_read_banks(
            [p.as_posix() for p in PATHS_SVD_BANK],
            source_ifos=["H1", "L1"],
            nslice=nslice,
        )
        return SortedBank(banks, device="cpu", dtype=torch.float32)

    def test_lloid_basic(self):
        """Test basic lloid function call."""
        sorted_bank = self._create_sorted_bank()

        # Create a mock pipeline with insert method
        pipeline = mock.MagicMock()

        input_source_links = {
            "H1": "source:src:H1",
            "L1": "source:src:L1",
        }

        result = lloid(
            pipeline=pipeline,
            sorted_bank=sorted_bank,
            input_source_links=input_source_links,
            nslice=-1,
            device="cpu",
            dtype=torch.float32,
        )

        # Verify output_source_links is returned with correct keys
        assert isinstance(result, dict)
        assert "H1" in result
        assert "L1" in result

        # Verify pipeline.insert was called multiple times
        assert pipeline.insert.call_count > 0

    def test_lloid_with_nslice_1(self):
        """Test lloid with nslice=1 (lines 213-214)."""
        sorted_bank = self._create_sorted_bank(nslice=1)

        pipeline = mock.MagicMock()
        input_source_links = {
            "H1": "source:src:H1",
            "L1": "source:src:L1",
        }

        result = lloid(
            pipeline=pipeline,
            sorted_bank=sorted_bank,
            input_source_links=input_source_links,
            nslice=1,
            device="cpu",
            dtype=torch.float32,
        )

        assert isinstance(result, dict)
        assert "H1" in result
        assert "L1" in result
        # With nslice=1, the output links should be mmname links
        for ifo in ["H1", "L1"]:
            assert "_mm_" in result[ifo]

    def test_lloid_with_reconstruction_segment_list(self):
        """Test lloid with reconstruction_segment_list."""
        import igwn_segments as segments
        from lal import LIGOTimeGPS

        sorted_bank = self._create_sorted_bank()

        pipeline = mock.MagicMock()
        input_source_links = {
            "H1": "source:src:H1",
            "L1": "source:src:L1",
        }

        # Create a reconstruction segment list
        recon_list = segments.segmentlist(
            [segments.segment(LIGOTimeGPS(0), LIGOTimeGPS(1000000))]
        )

        result = lloid(
            pipeline=pipeline,
            sorted_bank=sorted_bank,
            input_source_links=input_source_links,
            nslice=-1,
            device="cpu",
            dtype=torch.float32,
            reconstruction_segment_list=recon_list,
        )

        assert isinstance(result, dict)
        assert "H1" in result
        assert "L1" in result


class TestResampleVarianceGain:
    """Tests for RESAMPLE_VARIANCE_GAIN constant."""

    def test_resample_variance_gain_value(self):
        """Test RESAMPLE_VARIANCE_GAIN has expected value."""
        # This is a known constant from gstlal
        assert RESAMPLE_VARIANCE_GAIN == 0.9684700588501590213
        assert isinstance(RESAMPLE_VARIANCE_GAIN, float)
