"""Tests for sgnl.dags.config"""

import sys
from unittest import mock

import pytest
from igwn_segments import segment, segmentlist, segmentlistdict

# Mock ezdag before importing config (ezdag may not be installed)
sys.modules["ezdag"] = mock.MagicMock()

from sgnl.dags import config  # noqa: E402


class TestBuildConfig:
    """Tests for build_config function."""

    def test_build_config_basic(self, tmp_path):
        """Test basic config building."""
        # Create a minimal config file
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
condor:
  accounting-group: ligo.dev.o4.test
instruments: H1L1
start: 1000000000
stop: 1000010000
source:
  frame-segments-file: segments.xml.gz
"""
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist(
            [segment(1000000000, 1000005000), segment(1000005000, 1000010000)]
        )
        mock_segments["L1"] = segmentlist([segment(1000000000, 1000010000)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
            mock.patch.object(
                config.segments,
                "split_segments_by_lock",
                return_value=[segment(1000000000, 1000010000)],
            ),
            mock.patch.object(
                config.segments,
                "analysis_segments",
                return_value=segmentlistdict(),
            ),
        ):
            result = config.build_config(
                str(config_file), str(tmp_path), force_segments=True
            )

        assert result.condor.accounting_group == "ligo.dev.o4.test"
        assert result.ifo_list == ["H1", "L1"]
        assert result.span == segment(1000000000, 1000010000)

    def test_build_config_missing_condor_raises(self, tmp_path):
        """Test that missing condor section raises AssertionError."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
instruments: H1
"""
        )

        with pytest.raises(AssertionError) as exc_info:
            config.build_config(str(config_file), str(tmp_path))

        assert "condor section" in str(exc_info.value)

    def test_build_config_missing_accounting_group_raises(self, tmp_path):
        """Test that missing accounting-group raises AssertionError."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
condor:
  container: /path/to/container
instruments: H1
"""
        )

        with pytest.raises(AssertionError) as exc_info:
            config.build_config(str(config_file), str(tmp_path))

        assert "accounting-group" in str(exc_info.value)

    def test_build_config_no_start_sets_zero_span(self, tmp_path):
        """Test that missing start/stop sets span to (0, 0)."""
        # Create a segments file that will be loaded
        seg_file = tmp_path / "segments.xml.gz"
        seg_file.touch()

        config_file = tmp_path / "config.yml"
        config_file.write_text(
            f"""
condor:
  accounting-group: ligo.dev.o4.test
instruments: H1
source:
  frame-segments-file: {seg_file}
"""
        )

        mock_xmldoc = mock.MagicMock()
        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(0, 100)])

        with (
            mock.patch.object(
                config.ligolw_utils, "load_filename", return_value=mock_xmldoc
            ),
            mock.patch.object(
                config.ligolw_segments,
                "segmenttable_get_by_name",
                return_value=mock_segments,
            ),
        ):
            result = config.build_config(str(config_file), str(tmp_path))

        assert result.span == segment(0, 0)

    def test_build_config_sets_default_paths(self, tmp_path):
        """Test that default paths are set correctly."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
condor:
  accounting-group: ligo.dev.o4.test
instruments: H1
source:
  frame-segments-file: segments.xml.gz
"""
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(0, 100)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.build_config(
                str(config_file), str(tmp_path), force_segments=True
            )

        assert result.paths.storage == str(tmp_path)

    def test_build_config_sets_accounting_user(self, tmp_path):
        """Test that accounting_group_user defaults to current user."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
condor:
  accounting-group: ligo.dev.o4.test
instruments: H1
source:
  frame-segments-file: segments.xml.gz
"""
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(0, 100)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
            mock.patch.object(config.getpass, "getuser", return_value="testuser"),
        ):
            result = config.build_config(
                str(config_file), str(tmp_path), force_segments=True
            )

        assert result.condor.accounting_group_user == "testuser"

    def test_build_config_min_instruments_default(self, tmp_path):
        """Test that min_instruments_segments defaults to 1."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
condor:
  accounting-group: ligo.dev.o4.test
instruments: H1
source:
  frame-segments-file: segments.xml.gz
"""
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(0, 100)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.build_config(
                str(config_file), str(tmp_path), force_segments=True
            )

        assert result.min_instruments_segments == 1


class TestCreateSegments:
    """Tests for create_segments function."""

    def test_create_segments_loads_from_file(self, tmp_path):
        """Test loading segments from existing file."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"
        seg_file.touch()

        cfg = DotDict(
            {
                "source": DotDict({"frame_segments_file": str(seg_file)}),
                "span": segment(0, 0),
            }
        )

        mock_xmldoc = mock.MagicMock()
        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(0, 100)])

        with (
            mock.patch.object(
                config.ligolw_utils, "load_filename", return_value=mock_xmldoc
            ),
            mock.patch.object(
                config.ligolw_segments,
                "segmenttable_get_by_name",
                return_value=mock_segments,
            ),
        ):
            result = config.create_segments(cfg)

        assert "H1" in result.segments

    def test_create_segments_force_recreates(self, tmp_path):
        """Test that force_segments recreates even if file exists."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"
        seg_file.touch()

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": None,
                        "use_gwosc_segments": False,
                    }
                ),
                "instruments": ["H1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(0, 0),
            }
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.create_segments(cfg, force_segments=True)

        assert "H1" in result.segments

    def test_create_segments_query_gwosc(self, tmp_path):
        """Test querying GWOSC segments."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": None,
                        "use_gwosc_segments": True,
                    }
                ),
                "instruments": ["H1", "L1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(0, 0),
            }
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])
        mock_segments["L1"] = segmentlist([segment(1000000000, 1000010000)])

        with (
            mock.patch.object(
                config.segments,
                "query_gwosc_segments",
                return_value=mock_segments,
            ) as mock_query,
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.create_segments(cfg)

        mock_query.assert_called_once()
        assert "H1" in result.segments

    def test_create_segments_query_dqsegdb_with_flags(self, tmp_path):
        """Test querying DQSegDB with flags."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": {"H1": "DMT-ANALYSIS_READY:1", "L1": None},
                        "use_gwosc_segments": False,
                    }
                ),
                "instruments": ["H1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(0, 0),
            }
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ) as mock_query,
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.create_segments(cfg)

        mock_query.assert_called_once()
        # Only H1 should have a flag (L1 is None)
        call_args = mock_query.call_args
        assert call_args[0][3] == {"H1": "H1:DMT-ANALYSIS_READY:1"}
        assert "H1" in result.segments

    def test_create_segments_gwosc_with_flags_raises(self, tmp_path):
        """Test that using GWOSC with flags raises ValueError."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": {"H1": "DMT-ANALYSIS_READY:1"},
                        "use_gwosc_segments": True,
                    }
                ),
                "instruments": ["H1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(0, 0),
            }
        )

        with pytest.raises(ValueError) as exc_info:
            config.create_segments(cfg)

        assert "use-gwosc-segments" in str(exc_info.value)

    def test_create_segments_creates_time_bins(self, tmp_path):
        """Test that time bins are created when span is non-zero."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": None,
                        "use_gwosc_segments": False,
                    }
                ),
                "instruments": ["H1"],
                "ifos": ["H1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(1000000000, 1000010000),
                "min_instruments_segments": 1,
            }
        )

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
            mock.patch.object(
                config.segments,
                "split_segments_by_lock",
                return_value=[segment(1000000000, 1000010000)],
            ),
            mock.patch.object(
                config.segments,
                "analysis_segments",
                return_value=segmentlistdict(),
            ),
        ):
            result = config.create_segments(cfg)

        assert hasattr(result, "time_bins")

    def test_create_segments_handles_raw_segments(self, tmp_path):
        """Test that non-segmentlist segments are converted."""
        from sgnl.dags.util import DotDict

        seg_file = tmp_path / "segments.xml.gz"

        cfg = DotDict(
            {
                "source": DotDict(
                    {
                        "frame_segments_file": str(seg_file),
                        "flags": None,
                        "use_gwosc_segments": False,
                    }
                ),
                "instruments": ["H1"],
                "start": 1000000000,
                "stop": 1000010000,
                "span": segment(0, 0),
            }
        )

        # Return raw list of segments instead of segmentlist
        mock_segments = {"H1": [segment(1000000000, 1000010000)]}

        with (
            mock.patch.object(
                config.segments,
                "query_dqsegdb_segments",
                return_value=mock_segments,
            ),
            mock.patch.object(config.segments, "write_segments"),
        ):
            result = config.create_segments(cfg)

        assert isinstance(result.segments["H1"], segmentlist)


class TestApplyCat1Vetoes:
    """Tests for apply_cat1_vetoes function."""

    def test_apply_cat1_vetoes_no_file_specified(self, tmp_path):
        """Test that no vetoes file returns segments unchanged."""
        from sgnl.dags.util import DotDict

        segdict = segmentlistdict()
        segdict["H1"] = segmentlist([segment(0, 1000)])

        cfg = DotDict({"source": DotDict({})})

        result = config.apply_cat1_vetoes(segdict, cfg)

        assert result is segdict

    def test_apply_cat1_vetoes_file_not_found_raises(self, tmp_path):
        """Test that missing vetoes file raises FileNotFoundError."""
        from sgnl.dags.util import DotDict

        segdict = segmentlistdict()
        segdict["H1"] = segmentlist([segment(0, 1000)])

        cfg = DotDict(
            {"source": DotDict({"cat1_vetoes_file": str(tmp_path / "nonexistent.xml")})}
        )

        with pytest.raises(FileNotFoundError) as exc_info:
            config.apply_cat1_vetoes(segdict, cfg)

        assert "not found" in str(exc_info.value)

    def test_apply_cat1_vetoes_subtracts_vetoes(self, tmp_path):
        """Test that vetoes are properly subtracted from segments."""
        from sgnl.dags.util import DotDict

        # Create science segments
        segdict = segmentlistdict()
        segdict["H1"] = segmentlist([segment(0, 1000)])
        segdict["L1"] = segmentlist([segment(0, 1000)])

        # Create veto segments
        veto_dict = segmentlistdict()
        veto_dict["H1"] = segmentlist([segment(200, 400)])
        veto_dict["L1"] = segmentlist([segment(500, 700)])

        # Write vetoes to file
        vetoes_file = tmp_path / "vetoes.xml.gz"
        config.segments.write_segments(veto_dict, output=str(vetoes_file))

        cfg = DotDict({"source": DotDict({"cat1_vetoes_file": str(vetoes_file)})})

        result = config.apply_cat1_vetoes(segdict, cfg)

        # H1: [0, 1000] - [200, 400] = [0, 200] + [400, 1000]
        assert len(result["H1"]) == 2
        assert result["H1"][0] == segment(0, 200)
        assert result["H1"][1] == segment(400, 1000)

        # L1: [0, 1000] - [500, 700] = [0, 500] + [700, 1000]
        assert len(result["L1"]) == 2
        assert result["L1"][0] == segment(0, 500)
        assert result["L1"][1] == segment(700, 1000)

    def test_apply_cat1_vetoes_missing_ifo_in_vetoes(self, tmp_path):
        """Test vetoes with missing IFO are handled correctly."""
        from sgnl.dags.util import DotDict

        # Create science segments for H1 and L1
        segdict = segmentlistdict()
        segdict["H1"] = segmentlist([segment(0, 1000)])
        segdict["L1"] = segmentlist([segment(0, 1000)])

        # Create veto segments only for H1
        veto_dict = segmentlistdict()
        veto_dict["H1"] = segmentlist([segment(200, 400)])

        # Write vetoes to file
        vetoes_file = tmp_path / "vetoes.xml.gz"
        config.segments.write_segments(veto_dict, output=str(vetoes_file))

        cfg = DotDict({"source": DotDict({"cat1_vetoes_file": str(vetoes_file)})})

        result = config.apply_cat1_vetoes(segdict, cfg)

        # H1 should have vetoes applied
        assert len(result["H1"]) == 2

        # L1 should be unchanged (no vetoes for L1)
        assert len(result["L1"]) == 1
        assert result["L1"][0] == segment(0, 1000)


class TestCreateTimeBins:
    """Tests for create_time_bins function."""

    def test_create_time_bins_basic(self):
        """Test basic time bin creation."""
        from sgnl.dags.util import DotDict

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])
        mock_segments["L1"] = segmentlist([segment(1000000000, 1000010000)])

        cfg = DotDict(
            {
                "ifos": ["H1", "L1"],
                "segments": mock_segments,
                "span": segment(1000000000, 1000010000),
            }
        )

        mock_analysis_segs = segmentlistdict()
        mock_analysis_segs[frozenset(["H1", "L1"])] = segmentlist(
            [segment(1000000512, 1000010000)]
        )

        with (
            mock.patch.object(
                config.segments,
                "split_segments_by_lock",
                return_value=[segment(1000000000, 1000010000)],
            ),
            mock.patch.object(
                config.segments,
                "analysis_segments",
                return_value=mock_analysis_segs,
            ),
        ):
            result = config.create_time_bins(cfg)

        assert hasattr(result, "time_bins")
        assert hasattr(result, "time_boundaries")

    def test_create_time_bins_one_ifo_only(self):
        """Test time bin creation with one_ifo_only=True."""
        from sgnl.dags.util import DotDict

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000100000)])

        cfg = DotDict(
            {
                "ifos": ["H1"],
                "segments": mock_segments,
                "span": segment(1000000000, 1000100000),
            }
        )

        mock_split_segs = segmentlist([segment(1000000512, 1000029000)])

        with (
            mock.patch.object(
                config.segments,
                "split_segments_by_lock",
                return_value=[segment(1000000000, 1000100000)],
            ),
            mock.patch.object(
                config.segments, "split_segments", return_value=mock_split_segs
            ),
        ):
            result = config.create_time_bins(cfg, one_ifo_only=True)

        assert hasattr(result, "time_bins")

    def test_create_time_bins_custom_parameters(self):
        """Test time bin creation with custom parameters."""
        from sgnl.dags.util import DotDict

        mock_segments = segmentlistdict()
        mock_segments["H1"] = segmentlist([segment(1000000000, 1000010000)])

        cfg = DotDict(
            {
                "ifos": ["H1"],
                "segments": mock_segments,
                "span": segment(1000000000, 1000010000),
            }
        )

        with (
            mock.patch.object(
                config.segments,
                "split_segments_by_lock",
                return_value=[segment(1000000000, 1000010000)],
            ),
            mock.patch.object(
                config.segments,
                "analysis_segments",
                return_value=segmentlistdict(),
            ) as mock_analysis,
        ):
            config.create_time_bins(
                cfg,
                start_pad=256,
                overlap=256,
                min_instruments=2,
                one_ifo_length=7200,
            )

        # Check that custom params were passed
        call_kwargs = mock_analysis.call_args[1]
        assert call_kwargs["start_pad"] == 256
        assert call_kwargs["overlap"] == 256
        assert call_kwargs["min_instruments"] == 2
        assert call_kwargs["one_ifo_length"] == 7200
